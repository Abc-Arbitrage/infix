package rules

import (
	"log"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/logging"

	"github.com/influxdata/influxdb/models"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/Abc-Arbitrage/infix/storage"
)

// DropSerieRule defines a rule to drop series
type DropSerieRule struct {
	dropFilter filter.Filter
	logger     *log.Logger
	check      bool

	count uint64
	total uint64

	shardCount uint64
	shardTotal uint64
}

// DropSerieRuleConfig represents the toml configuration for DropSerieRule
type DropSerieRuleConfig struct {
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
	r.shardCount = 0
	r.shardTotal = 0
	return true
}

// EndShard implements Rule interface
func (r *DropSerieRule) EndShard() error {
	log.Printf("dropped %d (%d%%) total keys in current shard", r.shardCount, (r.shardCount*100)/r.shardTotal)
	return nil
}

// StartTSM implements Rule interface
func (r *DropSerieRule) StartTSM(path string) bool {
	r.count = 0
	r.total = 0
	return true
}

// EndTSM implements Rule interface
func (r *DropSerieRule) EndTSM() {
	r.shardCount += r.count
	r.shardTotal += r.total
	log.Printf("dropped %d (%d%%) total keys in current TSM", r.count, (r.count*100)/r.total)
}

// StartWAL implements Rule interface
func (r *DropSerieRule) StartWAL(path string) bool {
	r.count = 0
	r.total = 0
	return true
}

// EndWAL implements Rule interface
func (r *DropSerieRule) EndWAL() {
	r.shardCount += r.count
	r.shardTotal += r.total
	log.Printf("dropped %d total keys in current WAL", r.count)
}

// Apply implements Rule interface
func (r *DropSerieRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	r.total++

	if r.dropFilter.Filter(key) {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, _ := models.ParseKey(seriesKey)
		r.logger.Printf("Dropping serie for measurement %s", measurement)
		r.count++
		return nil, nil, nil
	}

	return key, values, nil
}

// Sample implements the Config interface
func (c *DropSerieRuleConfig) Sample() string {
	return `
		[dropFilter.serie]
			[dropFilter.serie.measurement.strings]
				equal="cpu"
			[dropFilter.serie.tag.where]
				cpu="cpu0"
			[dropFilter.serie.field.pattern]
				pattern="^(idle|usage_idle)$"
			        
	`
}

// Build implements the Config interface
func (c *DropSerieRuleConfig) Build() (Rule, error) {
	return NewDropSerieRule(filter.NewRawSerieFilter(c.DropFilter)), nil
}
