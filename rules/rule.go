package rules

import (
	"errors"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/storage"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// RenameFn defines a function to rename a measurement or field
type RenameFn func(string) string

// RenameFnFromFilter returns a RenameFn that expands captured variables from a pattern if the given filter is a PatternFilter
func RenameFnFromFilter(f filter.Filter, to string) RenameFn {
	patternFilter, ok := f.(*filter.PatternFilter)

	var renameFn RenameFn

	if ok {
		renameFn = func(name string) string {
			return string(patternFilter.Pattern.ReplaceAll([]byte(name), []byte(to)))
		}
	} else {
		renameFn = func(name string) string {
			return to
		}
	}

	return renameFn
}

// ErrMissingMeasurementFilter is raised when a config is missing a measurement filter
var ErrMissingMeasurementFilter = errors.New("missing measurement filter")

// ErrMissingTagFilter is raised when a config is missing a tag filter
var ErrMissingTagFilter = errors.New("missing tag filter")

// ErrMissingFieldFilter is raised when a config is missing a field filter
var ErrMissingFieldFilter = errors.New("missing field filter")

const (
	// TSMReadOnly is a flag for rules that should be read only for TSM files
	TSMReadOnly = 1
	// WALReadOnly is a flag for rules that should be read only for WAL files
	WALReadOnly = TSMReadOnly << 1

	// TSMWriteOnly is a flag for rules that should only write TSM files
	TSMWriteOnly = WALReadOnly << 1
	// WALWriteOnly is a flag for rules that should only write WAL files
	WALWriteOnly = TSMWriteOnly << 1

	// ReadOnly is a flag for rules that should be readonly only
	ReadOnly = TSMReadOnly | WALReadOnly

	// Standard is a flag for standard rules
	Standard = TSMWriteOnly | WALWriteOnly
)

// Rule represents a rule to apply to a given TSM or WAL entry
type Rule interface {
	CheckMode(check bool)
	Flags() int

	FilterKey(key []byte) bool

	Start()
	End()

	StartShard(info storage.ShardInfo) bool
	EndShard() error

	StartTSM(path string) bool
	EndTSM()

	StartWAL(path string) bool
	EndWAL()

	Apply(key []byte, values []tsm1.Value) (newKey []byte, newValues []tsm1.Value, err error)
}
