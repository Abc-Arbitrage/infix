package rules

import (
	"log"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/Abc-Arbitrage/infix/storage"
)

// UpdateTagValueRule defines a rule to update the value of a tag for a given measurement
type UpdateTagValueRule struct {
	measurementFilter filter.Filter

	keyFilter   filter.Filter
	valueFilter filter.Filter

	renameFn RenameFn

	check  bool
	logger *log.Logger
}

// UpdateTagValueRuleConfig represents the toml configuration of UpdateTagValue rule
type UpdateTagValueRuleConfig struct {
	Measurement filter.Filter
	Key         filter.Filter
	Value       filter.Filter
	To          string
}

// NewUpdateTagValueRule creates a new UpdateTagValueRule
func NewUpdateTagValueRule(measurementFilter filter.Filter, keyFilter filter.Filter, valueFilter filter.Filter, renameFn RenameFn) *UpdateTagValueRule {
	return &UpdateTagValueRule{
		measurementFilter: measurementFilter,

		keyFilter:   keyFilter,
		valueFilter: valueFilter,

		renameFn: renameFn,

		check:  false,
		logger: logging.GetLogger("UpdateTagValueRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *UpdateTagValueRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *UpdateTagValueRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *UpdateTagValueRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// FilterKey implements Rule interface
func (r *UpdateTagValueRule) FilterKey(key []byte) bool {
	return false
}

// Start implements Rule interface
func (r *UpdateTagValueRule) Start() {

}

// End implements Rule interface
func (r *UpdateTagValueRule) End() {

}

// StartShard implements Rule interface
func (r *UpdateTagValueRule) StartShard(info storage.ShardInfo) bool {
	return true
}

// EndShard implements Rule interface
func (r *UpdateTagValueRule) EndShard() error {
	return nil
}

// StartTSM implements Rule interface
func (r *UpdateTagValueRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *UpdateTagValueRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *UpdateTagValueRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *UpdateTagValueRule) EndWAL() {
}

// Apply implements Rule interface
func (r *UpdateTagValueRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	if r.measurementFilter.Filter(key) {
		seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(seriesKey)

		var newTags models.Tags
		for _, tag := range tags {
			newTag := tag.Clone()
			if r.keyFilter.Filter(tag.Key) && r.valueFilter.Filter(tag.Value) {
				newTagValue := r.renameFn(string(tag.Value))
				r.logger.Printf("Updating tag for measurement '%s' %s=%s to %s=%s", measurement, tag.Key, tag.Value, tag.Key, newTagValue)
				newTag.Value = []byte(newTagValue)
			}

			newTags = append(newTags, newTag)
		}

		newKey := models.MakeKey([]byte(measurement), newTags)
		newSeriesKey := tsm1.SeriesFieldKeyBytes(string(newKey), string(field))
		return newSeriesKey, values, nil
	}

	return key, values, nil
}

// Sample implements Config interface
func (c *UpdateTagValueRuleConfig) Sample() string {
	return `
	to="aws-$1"
	[measurement.strings]
	   hasprefix="linux."
	[key.strings]
	   equal="region"
	[value.pattern]
	  pattern="amazon-(.*)"
	`
}

// Build implements Config interface
func (c *UpdateTagValueRuleConfig) Build() (Rule, error) {
	if c.To == "" {
		return nil, ErrMissingRenameTo
	}
	if c.Measurement == nil {
		return nil, ErrMissingMeasurementFilter
	}
	if c.Key == nil {
		return nil, ErrMissingTagKeyFilter
	}
	if c.Value == nil {
		return nil, ErrMissingTagValueFilter
	}

	renameFn := RenameFnFromFilter(c.Value, c.To)
	return NewUpdateTagValueRule(c.Measurement, c.Key, c.Value, renameFn), nil
}
