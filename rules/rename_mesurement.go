package rules

import (
	"fmt"
	"log"
	"regexp"

	"github.com/oktal/infix/logging"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/oktal/infix/storage"
)

// RenameFn defines a function to rename a measurement
type RenameFn func(string) string

// RenameMeasurementRule represents a rule to rename a measurement
type RenameMeasurementRule struct {
	pattern *regexp.Regexp

	renameFn RenameFn
	renamed  map[string]string

	check  bool
	shard  storage.ShardInfo
	logger *log.Logger
}

// NewRenameMeasurement creates a new RenameMeasurementRule to rename a single measurement
func NewRenameMeasurement(srcName string, dstName string) *RenameMeasurementRule {
	renameFn := func(measurement string) string {
		return dstName
	}
	return NewRenameMeasurementWithPattern(srcName, renameFn)
}

// NewRenameMeasurementWithPattern creates a new RenameMeasurementRule to rename measurements that match the given pattern
func NewRenameMeasurementWithPattern(pattern string, renameFn RenameFn) *RenameMeasurementRule {
	r := regexp.MustCompile(pattern)
	return &RenameMeasurementRule{
		pattern:  r,
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

// WithLogger sets the logger on the rule
func (r *RenameMeasurementRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// StartShard implements Rule interface
func (r *RenameMeasurementRule) StartShard(info storage.ShardInfo) {
	r.shard = info
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
func (r *RenameMeasurementRule) StartTSM(path string) {
}

// EndTSM implements Rule interface
func (r *RenameMeasurementRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *RenameMeasurementRule) StartWAL(path string) {
}

// EndWAL implements Rule interface
func (r *RenameMeasurementRule) EndWAL() {
}

// Apply implements Rule interface
func (r *RenameMeasurementRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, tags := models.ParseKey(seriesKey)

	if r.pattern.Match([]byte(measurement)) {
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
