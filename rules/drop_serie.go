package rules

import (
	"log"

	"github.com/oktal/infix/filter"
	"github.com/oktal/infix/logging"

	"github.com/influxdata/influxdb/models"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/storage"
)

// DropSerieRule defines a rule to drop series
type DropSerieRule struct {
	dropFilter filter.Filter
	logger     *log.Logger
	check      bool
}

// DropSerieRuleConfiguration represents the toml configuration for DropSerieRule
type DropSerieRuleConfiguration struct {
	DropFilter filter.Filter
}

// NewDropSerieRule creates a new DropSerieRule
func NewDropSerieRule(dropFilter filter.Filter) *DropSerieRule {
	return &DropSerieRule{
		dropFilter: dropFilter,
		logger:     logging.GetLogger("DropSerieRule"),
		check:      false,
	}
}

// CheckMode sets the check mode on the rule
func (r *DropSerieRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *DropSerieRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *DropSerieRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *DropSerieRule) Start() {

}

// End implements Rule interface
func (r *DropSerieRule) End() {

}

// StartShard implements Rule interface
func (r *DropSerieRule) StartShard(info storage.ShardInfo) bool {
	return true
}

// EndShard implements Rule interface
func (r *DropSerieRule) EndShard() error {
	return nil
}

// StartTSM implements Rule interface
func (r *DropSerieRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *DropSerieRule) EndTSM() {

}

// StartWAL implements Rule interface
func (r *DropSerieRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *DropSerieRule) EndWAL() {

}

// Apply implements Rule interface
func (r *DropSerieRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	if r.dropFilter.Filter(key) {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, _ := models.ParseKey(seriesKey)
		r.logger.Printf("Dropping serie for measurement %s", measurement)
		return nil, nil, nil
	}

	return key, values, nil
}

// Sample implements the Config interface
func (c *DropSerieRuleConfiguration) Sample() string {
	return `
		[[rules.drop-serie]]
			[rules.drop-serie.dropFilter.serie]
				measurement="cpu"
				[rules.drop-serie.dropFilter.serie.where]
					cpu="cpu0"
				[rules.drop-serie.dropFilter.serie.field]
					[rules.drop-serie.dropFilter.serie.field.pattern]
						pattern="^(idle|usage_idle)$"
			        
	`
}

// Build implements the Config interface
func (c *DropSerieRuleConfiguration) Build() (Rule, error) {
	return NewDropSerieRule(filter.NewRawSerieFilter(c.DropFilter)), nil
}
