package rules

import (
	"fmt"
	"log"

	"github.com/Abc-Arbitrage/infix/logging"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/storage"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
)

type DropFieldRule struct {
	check bool
	shard storage.ShardInfo

	measurementFilter filter.Filter
	fieldFilter       filter.Filter
	typeFilter        filter.Filter

	deleted map[string][]string

	logger *log.Logger
}

type DropFieldRuleConfig struct {
	Measurement filter.Filter
	Field       filter.Filter
	Type        filter.Filter
}

func NewDropField(measurementFilter filter.Filter, fieldFilter filter.Filter, typeFilter filter.Filter) *DropFieldRule {
	return &DropFieldRule{
		measurementFilter: filter.NewMeasurementFilter(measurementFilter),
		fieldFilter:       fieldFilter,
		typeFilter:        typeFilter,
		deleted:           make(map[string][]string),
		logger:            logging.GetLogger("DropFieldRule"),
	}
}

func (r *DropFieldRule) CheckMode(check bool) {
	r.check = check
}

func (r *DropFieldRule) Flags() int {
	return Standard
}

func (r *DropFieldRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

func (r *DropFieldRule) FilterKey(key []byte) bool {
	return r.measurementFilter.Filter(key)
}

func (r *DropFieldRule) Start() {
}

func (r *DropFieldRule) End() {
}

func (r *DropFieldRule) StartShard(info storage.ShardInfo) bool {
	r.shard = info
	r.deleted = make(map[string][]string)
	return true
}

func (r *DropFieldRule) EndShard() error {
	if r.check || len(r.deleted) == 0 {
		return nil
	}

	shard := r.shard

	if shard.FieldsIndex == nil {
		return nil
	}

	for measurement, fields := range r.deleted {
		oldFields := shard.FieldsIndex.FieldsByString(measurement)

		if oldFields == nil {
			return fmt.Errorf("Failed to find fields in index for measurement '%s'", measurement)
		}

		tmpFields := make(map[string]influxql.DataType)

		oldFields.ForEachField(func(name string, fieldType influxql.DataType) bool {
			found := false

			for _, f := range fields {
				if name == f {
					found = true
					break
				}

				if !found {
					tmpFields[name] = fieldType
				}
			}
			return true
		})

		shard.FieldsIndex.Delete(measurement)
		newFields := shard.FieldsIndex.CreateFieldsIfNotExists([]byte(measurement))

		for f, t := range tmpFields {
			if err := newFields.CreateFieldIfNotExists([]byte(f), t); err != nil {
				return err
			}
		}
	}

	if err := shard.FieldsIndex.Save(); err != nil {
		return err
	}

	return nil
}

func (r *DropFieldRule) StartTSM(path string) bool {
	return true
}

func (r *DropFieldRule) EndTSM() {
}

func (r *DropFieldRule) StartWAL(path string) bool {
	return true
}

func (r *DropFieldRule) EndWAL() {
}

func (r *DropFieldRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	dataType, err := tsm1.Values(values).InfluxQLType()
	if err != nil {
		return nil, nil, err
	}

	typeString := dataType.String()
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)

	log.Printf("type is %s", typeString)

	if r.measurementFilter.Filter(key) && r.fieldFilter.Filter(field) && r.typeFilter.Filter([]byte(typeString)) {
		measurement, _ := models.ParseKey(seriesKey)
		r.logger.Printf("Dropping field '%s' from measurement '%s' (type '%s')", field, measurement, typeString)

		fs := string(field)

		if fields, ok := r.deleted[measurement]; ok {
			found := false
			for _, f := range fields {
				if f == fs {
					found = true
					break
				}
			}

			if !found {
				fields = append(fields, fs)
			}
		} else {
			r.deleted[measurement] = []string{fs}
		}

		return nil, nil, nil
	}

	return key, values, nil
}

func (c *DropFieldRuleConfig) Sample() string {
	return `
	[measurement.strings]
	    equal="mem"
	[field.strings]
	    equal="used"
`
}

func (c *DropFieldRuleConfig) Build() (Rule, error) {
	if c.Measurement == nil {
		return nil, ErrMissingMeasurementFilter
	}

	if c.Field == nil {
		return nil, ErrMissingFieldFilter
	}

	typeFilter := c.Type
	if typeFilter == nil {
		typeFilter = &filter.AlwaysTrueFilter{}
	}

	return NewDropField(c.Measurement, c.Field, typeFilter), nil
}
