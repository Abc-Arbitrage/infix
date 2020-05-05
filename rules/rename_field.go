package rules

import (
	"fmt"
	"log"

	"github.com/oktal/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
	"github.com/oktal/infix/filter"
	"github.com/oktal/infix/storage"
)

type fieldRename struct {
	oldKey string
	newKey string
}

// RenameFieldRule is a rule to rename a field from a given measurement
type RenameFieldRule struct {
	check bool
	shard storage.ShardInfo

	measurementFilter filter.Filter
	fieldFilter       filter.Filter

	renamed  map[string][]fieldRename
	renameFn RenameFn

	logger *log.Logger
}

// RenameFieldRuleConfig represents toml configuration a RenameField rule
type RenameFieldRuleConfig struct {
	Measurement filter.Filter
	Field       filter.Filter
	To          string
}

// NewRenameField creates a new RenameFiled rule with given measurement and filter filters, will renamed fields according to renameFn
func NewRenameField(measurement filter.Filter, field filter.Filter, renameFn RenameFn) *RenameFieldRule {
	return &RenameFieldRule{
		measurementFilter: measurement,
		fieldFilter:       field,
		renamed:           make(map[string][]fieldRename),
		renameFn:          renameFn,
		logger:            logging.GetLogger("RenameFieldRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *RenameFieldRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *RenameFieldRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *RenameFieldRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *RenameFieldRule) Start() {
}

// End implements Rule interface
func (r *RenameFieldRule) End() {
}

// StartShard implements Rule interface
func (r *RenameFieldRule) StartShard(info storage.ShardInfo) bool {
	r.shard = info
	r.renamed = make(map[string][]fieldRename)
	return true
}

// EndShard implements Rule interface
func (r *RenameFieldRule) EndShard() error {
	if len(r.renamed) > 0 {
		shard := r.shard
		if shard.FieldsIndex == nil {
			return nil
		}

		for m, renames := range r.renamed {
			oldFields := shard.FieldsIndex.FieldsByString(m)
			if oldFields == nil {
				return fmt.Errorf("Failed to find fields in index for measurement '%s'", m)
			}

			getFieldKey := func(key string) string {
				for _, r := range renames {
					if r.oldKey == key {
						return r.newKey
					}
				}

				return key
			}

			fieldSet := make(map[string]influxql.DataType)

			oldFields.ForEachField(func(name string, fieldType influxql.DataType) bool {
				key := getFieldKey(name)
				if key != name {
					r.logger.Printf("Renaming field '%s' to '%s' in index for measurement '%s'", name, key, m)
				}
				fieldSet[getFieldKey(name)] = fieldType
				return true
			})

			shard.FieldsIndex.Delete(m)
			newFields := shard.FieldsIndex.CreateFieldsIfNotExists([]byte(m))

			for f, t := range fieldSet {
				if err := newFields.CreateFieldIfNotExists([]byte(f), t); err != nil {
					return err
				}
			}
		}

		if !r.check {
			return shard.FieldsIndex.Save()
		}
	}

	return nil
}

// StartTSM implements Rule interface
func (r *RenameFieldRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *RenameFieldRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *RenameFieldRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *RenameFieldRule) EndWAL() {
}

// Apply implements Rule interface
func (r *RenameFieldRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, _ := models.ParseKey(seriesKey)

	if r.measurementFilter.Filter([]byte(measurement)) && r.fieldFilter.Filter(field) {
		newField := r.renameFn(string(field))
		r.logger.Printf("Renaming field '%s' to '%s' for measurement %s", field, newField, measurement)
		rename := fieldRename{oldKey: string(field), newKey: newField}
		r.renamed[measurement] = append(r.renamed[measurement], rename)

		newKey := tsm1.SeriesFieldKeyBytes(string(seriesKey), newField)
		return newKey, values, nil
	}

	return key, values, nil
}

// Sample implements Config interface
func (c *RenameFieldRuleConfig) Sample() string {
	return `
	to="agg_5m_${1}_${2}"
    [measurement.strings]
        hasprefix="linux."
    [field.pattern]
		pattern="(.+)_(avg|sum)"
`
}

// Build implements Config interface
func (c *RenameFieldRuleConfig) Build() (Rule, error) {
	if c.Measurement == nil {
		return nil, fmt.Errorf("missing measurement filter")
	}
	if c.Field == nil {
		return nil, fmt.Errorf("missing field filter")
	}

	patternFilter, ok := c.Measurement.(*filter.PatternFilter)
	var renameFn RenameFn
	if ok {
		renameFn = func(name string) string {
			return string(patternFilter.Pattern.ReplaceAll([]byte(name), []byte(c.To)))
		}
	} else {
		renameFn = func(name string) string {
			return c.To
		}
	}

	return NewRenameField(c.Measurement, c.Field, renameFn), nil
}
