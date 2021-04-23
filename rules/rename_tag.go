package rules

import (
	"log"

	"github.com/Abc-Arbitrage/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/storage"
)

// RenameTagRule is a rule to rename a tag key
type RenameTagRule struct {
	measurementFilter filter.Filter
	tagFilter         filter.Filter

	check    bool
	renameFn RenameFn

	logger *log.Logger
}

// RenameTagRuleConfig represents the toml configuration of RenameTag rule
type RenameTagRuleConfig struct {
	Measurement filter.Filter
	Tag         filter.Filter
	To          string
}

// NewRenameTagRule creates a new RenameTagRule
func NewRenameTagRule(measurementFilter filter.Filter, tagFilter filter.Filter, renameFn RenameFn) *RenameTagRule {
	return &RenameTagRule{
		measurementFilter: filter.NewMeasurementFilter(measurementFilter),
		tagFilter:         tagFilter,
		check:             false,
		renameFn:          renameFn,
		logger:            logging.GetLogger("RenameTagRule"),
	}
}

// CheckMode implements Rule interface
func (r *RenameTagRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *RenameTagRule) Flags() int {
	return Standard
}

// WithLogger implements Rule interface
func (r *RenameTagRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// FilterKey implements Rule interface
func (r *RenameTagRule) FilterKey(key []byte) bool {
	return r.measurementFilter.Filter(key)
}

// Start implements Rule interface
func (r *RenameTagRule) Start() {
}

// End implements Rule interface
func (r *RenameTagRule) End() {
}

// StartShard implements Rule interface
func (r *RenameTagRule) StartShard(shard storage.ShardInfo) bool {
	return true
}

// EndShard implements Rule interface
func (r *RenameTagRule) EndShard() error {
	return nil
}

// StartTSM implements Rule interface
func (r *RenameTagRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *RenameTagRule) EndTSM() {

}

// StartWAL implements Rule interface
func (r *RenameTagRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *RenameTagRule) EndWAL() {

}

// Apply implements Rule interface
func (r *RenameTagRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	if r.measurementFilter.Filter(key) {
		seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(seriesKey)
		var newTags models.Tags

		for _, t := range tags {
			newTag := t.Clone()
			if r.tagFilter.Filter(t.Key) {
				newTagKey := r.renameFn(string(t.Key))
				r.logger.Printf("renaming tag '%s' from measurement '%s' to '%s'", t.Key, measurement, newTagKey)
				newTag.Key = []byte(newTagKey)
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
func (c *RenameTagRuleConfig) Sample() string {
	return `
    to="hostname"
    [measurement.strings]
        hasprefix="linux."
    [tag.strings]
        equal="host"
	`
}

// Build implements Config interface
func (c *RenameTagRuleConfig) Build() (Rule, error) {
	if c.To == "" {
		return nil, ErrMissingRenameTo
	}
	if c.Measurement == nil {
		return nil, ErrMissingMeasurementFilter
	}
	if c.Tag == nil {
		return nil, ErrMissingTagFilter
	}

	renameFn := RenameFnFromFilter(c.Tag, c.To)
	return NewRenameTagRule(c.Measurement, c.Tag, renameFn), nil
}
