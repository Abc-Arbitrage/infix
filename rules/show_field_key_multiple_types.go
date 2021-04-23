package rules

import (
	"fmt"
	"log"
	"strings"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/logging"
	"github.com/Abc-Arbitrage/infix/storage"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
)

type shardFieldInfo struct {
    shard storage.ShardInfo
    key string
    fieldType influxql.DataType
}

type measurementInfo struct {
    name string

    fields map[string][]shardFieldInfo
}

// ShowFieldKeyMultipleTypesRule is a rule to show fields on measurements that have different
// data type along shards
type ShowFieldKeyMultipleTypesRule struct {
    check bool
    shard storage.ShardInfo

    measurementFilter filter.Filter
    fieldFilter filter.Filter

    measurements map[string] measurementInfo

    logger *log.Logger
}

// ShowFieldKeyMultipleTypesConfig represents the toml configuration for ShowFieldKeyMultipleTypesRule
type ShowFieldKeyMultipleTypesConfig struct {
    Measurement filter.Filter
    Field filter.Filter
}

// NewShowFieldKeyMultipleTypes creates an ShowFieldKeyMultipleTypesRule
func NewShowFieldKeyMultipleTypes(measurementFilter filter.Filter, fieldFilter filter.Filter) *ShowFieldKeyMultipleTypesRule {
    return &ShowFieldKeyMultipleTypesRule{
        measurementFilter: filter.NewMeasurementFilter(measurementFilter),
        fieldFilter: fieldFilter,
        measurements: make(map[string] measurementInfo),
        logger: logging.GetLogger("ShowFieldKeyMultipleTypesRule"),
    }
}

// CheckMode sets the check mode on the rule
func (r* ShowFieldKeyMultipleTypesRule) CheckMode(check bool) {
    r.check = check
}

// Flags implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) Flags() int {
	return ReadOnly
}

// WithLogger sets the logger on the rule
func (r *ShowFieldKeyMultipleTypesRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// FilterKey implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) FilterKey(key []byte) bool {
    _, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(key)
    return r.measurementFilter.Filter(key) && r.fieldFilter.Filter(fieldKey)
}

// Start implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) Start() {

}

// End implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) End() {
    for measurement, info := range r.measurements {
        for fieldKey, fieldsInfo := range info.fields {
            if len(fieldsInfo) > 1 {
                var sb strings.Builder
                sb.WriteString("[")
                for i, f := range fieldsInfo {
                    if i >= 1 {
                        sb.WriteString(", ")
                    }
                    fmt.Fprintf(&sb, "%s (shard %d)", f.fieldType, f.shard.ID)
                }
                sb.WriteString("]")
                r.logger.Printf("Detected multiple types for field '%s' of measurement '%s' %s", fieldKey, measurement, sb.String())
            }
        }
    }

}

// StartShard implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) StartShard(info storage.ShardInfo) bool {
    r.shard = info
	return true
}

// EndShard implements Rule interface
func (r* ShowFieldKeyMultipleTypesRule) EndShard() error {
    shard := r.shard
    index := shard.FieldsIndex
    if index == nil {
        return fmt.Errorf("no fields index for shard id %d", shard.ID)
    }

    for m, info := range r.measurements {
        fields := index.FieldsByString(m)
        if fields == nil {
            continue
        }
        fieldsSet := fields.FieldSet()

        for fieldKey, fieldType := range fieldsSet {
            if fieldsInfo, ok := info.fields[fieldKey]; ok {
                found := false
                for _, f := range fieldsInfo {
                    if f.fieldType == fieldType {
                        found = true
                        break
                    }
                }

                if !found {
                    fieldsInfo = append(fieldsInfo, shardFieldInfo{
                        shard: shard,
                        key: fieldKey,
                        fieldType: fieldType,
                    })
                    info.fields[fieldKey] = fieldsInfo
                }
            } else {
                info.fields[fieldKey] = []shardFieldInfo {
                    {
                        shard: shard,
                        key: fieldKey,
                        fieldType: fieldType,
                    },
                }
            }
        }

    }

    return nil
}

// StartTSM implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *ShowFieldKeyMultipleTypesRule) EndWAL() {
}

// Apply implements Rule interface
func (r* ShowFieldKeyMultipleTypesRule) Apply(key []byte, values []tsm1.Value)  ([]byte, []tsm1.Value, error) {
    seriesKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(key)
    if r.measurementFilter.Filter(key) && r.fieldFilter.Filter(fieldKey) {
		measurement, _ := models.ParseKey(seriesKey)
        if _, ok := r.measurements[measurement]; !ok {
            r.measurements[measurement] = measurementInfo{
                name: measurement,
                fields: make(map[string][]shardFieldInfo),
            }
        }
    }
    return nil, nil, nil
}

// Sample implements Config interface
func (c *ShowFieldKeyMultipleTypesConfig) Sample() string {
    return `
    [measurement.strings]
       hassuffix=".gauge"
    [field.strings]
       equal="value"
    `
}

// Build implements Config interface
func (c *ShowFieldKeyMultipleTypesConfig) Build() (Rule, error) {
    measurementFilter := c.Measurement
    fieldFilter := c.Field

    if measurementFilter == nil {
        measurementFilter = &filter.AlwaysTrueFilter{}
    }

    if fieldFilter == nil {
        fieldFilter = &filter.AlwaysTrueFilter{}
    }

    return NewShowFieldKeyMultipleTypes(measurementFilter, fieldFilter), nil
}
