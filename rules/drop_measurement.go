package rules

import (
	"log"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/filter"
	"github.com/oktal/infix/logging"
	"github.com/oktal/infix/storage"
)

// DropMeasurementRule is a rule to drop measurements
type DropMeasurementRule struct {
	filter filter.Filter

	check bool

	shard   storage.ShardInfo
	dropped map[string]bool

	logger *log.Logger
}

// DropMeasurementRuleConfig represents the toml configuration for DropMeasurementRule
type DropMeasurementRuleConfig struct {
	DropFilter filter.Filter
}

// NewDropMeasurement creates a new DropMeasurementRule to drop a single measurement
func NewDropMeasurement(srcName string) *DropMeasurementRule {
	filter := filter.NewIncludeFilter([]string{srcName})
	return NewDropMeasurementWithFilter(filter)
}

// NewDropMeasurementWithPattern creates a new DropMeasurementRule to rename measurements that match the given pattern
func NewDropMeasurementWithPattern(pattern string) (*DropMeasurementRule, error) {
	filter, err := filter.NewPatternFilter(pattern)
	if err != nil {
		return nil, err
	}
	return NewDropMeasurementWithFilter(filter), nil
}

// NewDropMeasurementWithFilter creates a new DropMeasurementRule to rename measurements that uses the given filter
func NewDropMeasurementWithFilter(f filter.Filter) *DropMeasurementRule {
	measurementFilter := filter.NewMeasurementFilter(f)

	return &DropMeasurementRule{
		filter:  measurementFilter,
		dropped: make(map[string]bool),
		check:   false,
		logger:  logging.GetLogger("DropMeasurementRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *DropMeasurementRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *DropMeasurementRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *DropMeasurementRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *DropMeasurementRule) Start() {

}

// End implements Rule interface
func (r *DropMeasurementRule) End() {

}

// StartShard implements Rule interface
func (r *DropMeasurementRule) StartShard(info storage.ShardInfo) bool {
	r.shard = info
	return true
}

// EndShard implements Rule interface
func (r *DropMeasurementRule) EndShard() error {
	if len(r.dropped) > 0 {
		shard := r.shard
		if shard.FieldsIndex == nil {
			return nil
		}

		for d := range r.dropped {
			r.logger.Printf("Deleting fields in index for measurement '%s'", d)
			shard.FieldsIndex.Delete(d)
		}

		if !r.check {
			shard.FieldsIndex.Save()
		}

		r.dropped = make(map[string]bool)
	}

	return nil
}

// StartTSM implements Rule interface
func (r *DropMeasurementRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *DropMeasurementRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *DropMeasurementRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *DropMeasurementRule) EndWAL() {
}

// Apply implements Rule interface
func (r *DropMeasurementRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	if r.filter.Filter(key) {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, _ := models.ParseKey(seriesKey)

		r.logger.Printf("Dropping '%s'", measurement)
		r.dropped[measurement] = true
		return nil, nil, nil
	}

	return key, values, nil
}

// Count returns the number of measurements dropped
func (r *DropMeasurementRule) Count() int {
	return len(r.dropped)
}

// Sample implements Config interface
func (c *DropMeasurementRuleConfig) Sample() string {
	return `
    [[rules.drop-measurement]]
        [rules.rename-measurement.dropFilter.pattern]
            pattern="^linux\..*"
    `
}

// Build implements Config interface
func (c *DropMeasurementRuleConfig) Build() (Rule, error) {
	return NewDropMeasurementWithFilter(c.DropFilter), nil
}
