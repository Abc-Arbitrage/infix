package rules

import (
	"errors"
	"fmt"
	"log"

	"github.com/oktal/infix/filter"
	"github.com/oktal/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/oktal/infix/storage"
)

// ErrMissingFromFilter is raised when a config is missing from filter
var ErrMissingFromFilter = errors.New("missing from filter")

// ErrMissingRenameTo is raised when a config is missing to
var ErrMissingRenameTo = errors.New("missing rename 'to'")

// RenameMeasurementRule represents a rule to rename a measurement
type RenameMeasurementRule struct {
	filter filter.Filter

	renameFn RenameFn
	renamed  map[string]string

	check  bool
	shard  storage.ShardInfo
	logger *log.Logger
}

// RenameMeasurementRuleConfig represents the toml configuration for RenameMeasurementRule
type RenameMeasurementRuleConfig struct {
	From filter.Filter
	To   string
}

// NewRenameMeasurement creates a new RenameMeasurementRule to rename a single measurement
func NewRenameMeasurement(srcName string, dstName string) *RenameMeasurementRule {
	renameFn := func(measurement string) string {
		return dstName
	}
	filter := filter.NewIncludeFilter([]string{srcName})
	return NewRenameMeasurementWithFilter(filter, renameFn)
}

// NewRenameMeasurementWithPattern creates a new RenameMeasurementRule to rename measurements that match the given pattern
func NewRenameMeasurementWithPattern(pattern string, renameFn RenameFn) (*RenameMeasurementRule, error) {
	filter, err := filter.NewPatternFilter(pattern)
	if err != nil {
		return nil, err
	}
	return NewRenameMeasurementWithFilter(filter, renameFn), nil
}

// NewRenameMeasurementWithFilter creates a new RenameMeasurementRule to rename measurements that uses the given filter
func NewRenameMeasurementWithFilter(f filter.Filter, renameFn RenameFn) *RenameMeasurementRule {
	measurementFilter := filter.NewMeasurementFilter(f)

	return &RenameMeasurementRule{
		filter:   measurementFilter,
		renameFn: renameFn,
		renamed:  make(map[string]string),
		check:    false,
		logger:   logging.GetLogger("RenameMeasurementRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *RenameMeasurementRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *RenameMeasurementRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *RenameMeasurementRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *RenameMeasurementRule) Start() {

}

// End implements Rule interface
func (r *RenameMeasurementRule) End() {

}

// StartShard implements Rule interface
func (r *RenameMeasurementRule) StartShard(info storage.ShardInfo) bool {
	r.shard = info
	return true
}

// EndShard implements Rule interface
func (r *RenameMeasurementRule) EndShard() error {
	if len(r.renamed) > 0 {
		shard := r.shard
		if shard.FieldsIndex == nil {
			return nil
		}

		for oldName, newName := range r.renamed {
			oldFields := shard.FieldsIndex.FieldsByString(oldName)
			if oldFields == nil {
				return fmt.Errorf("Could not find fields. ShardId: %d Measurement: %s", shard.ID, oldName)
			}

			r.logger.Printf("Deleting fields in index for measurement '%s'", oldName)
			shard.FieldsIndex.Delete(oldName)
			shard.FieldsIndex.Delete(newName)

			r.logger.Printf("Updating index with %d fields for new measurement '%s'", oldFields.FieldN(), newName)

			newFields := shard.FieldsIndex.CreateFieldsIfNotExists([]byte(newName))
			for name, iflxType := range oldFields.FieldSet() {
				if err := newFields.CreateFieldIfNotExists([]byte(name), iflxType); err != nil {
					return err
				}
			}
		}

		if !r.check {
			shard.FieldsIndex.Save()
		}

		r.renamed = make(map[string]string)
	}

	return nil
}

// StartTSM implements Rule interface
func (r *RenameMeasurementRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *RenameMeasurementRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *RenameMeasurementRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *RenameMeasurementRule) EndWAL() {
}

// Apply implements Rule interface
func (r *RenameMeasurementRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	if r.filter.Filter(key) {
		seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(seriesKey)

		newName := r.renameFn(measurement)
		r.logger.Printf("Renaming '%s' to '%s'", measurement, newName)
		newSeriesKey := models.MakeKey([]byte(newName), tags)
		newKey := tsm1.SeriesFieldKeyBytes(string(newSeriesKey), string(field))
		r.renamed[measurement] = newName

		return newKey, values, nil
	}

	return key, values, nil
}

// Count returns the number of measurements renamed
func (r *RenameMeasurementRule) Count() int {
	return len(r.renamed)
}

// Sample implements Config interface
func (c *RenameMeasurementRuleConfig) Sample() string {
	return `
        to="linux.$1"
        [from.pattern]
            pattern="^(cpu|disk)$"
    `
}

// Build implements Config interface
func (c *RenameMeasurementRuleConfig) Build() (Rule, error) {
	if c.From == nil {
		return nil, ErrMissingFromFilter
	}
	if c.To == "" {
		return nil, ErrMissingRenameTo
	}

	var renameFn RenameFn

	patternFilter, ok := c.From.(*filter.PatternFilter)
	if ok {
		renameFn = func(name string) string {
			return string(patternFilter.Pattern.ReplaceAll([]byte(name), []byte(c.To)))
		}
	} else {
		renameFn = func(name string) string {
			return c.To
		}
	}

	return NewRenameMeasurementWithFilter(c.From, renameFn), nil
}
