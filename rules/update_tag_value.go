package rules

import (
	"log"

	"github.com/Abc-Arbitrage/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/Abc-Arbitrage/infix/storage"
)

// UpdateTagValueRule defines a rule to update the value of a tag for a given measurement
// TODO: use filters for measurement and tag
type UpdateTagValueRule struct {
	measurement string
	tagKey      string
	oldValue    string
	newValue    string

	check  bool
	logger *log.Logger
}

// NewUpdateTagValue creates a new UpdateTagValueRule
func NewUpdateTagValue(measurement string, tagKey string, oldValue string, newValue string) *UpdateTagValueRule {
	return &UpdateTagValueRule{
		measurement: measurement,
		tagKey:      tagKey,
		oldValue:    oldValue,
		newValue:    newValue,
		check:       false,
		logger:      logging.GetLogger("UpdateTagValueRule"),
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
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, tags := models.ParseKey(seriesKey)

	if measurement != r.measurement {
		return key, values, nil
	}

	count := 0

	var newTags []models.Tag
	for _, tag := range tags {
		if string(tag.Key) == r.tagKey && string(tag.Value) == r.oldValue {
			tag.Value = []byte(r.newValue)
			count++
		}

		newTags = append(newTags, tag)
	}

	if count > 0 {
		r.logger.Printf("Updating tag for measurement '%s' %s=%s to %s=%s", r.measurement, r.tagKey, r.oldValue, r.tagKey, r.newValue)
	}

	newSeriesKey := models.MakeKey([]byte(measurement), newTags)
	newKey := tsm1.SeriesFieldKeyBytes(string(newSeriesKey), string(field))
	return newKey, values, nil
}
