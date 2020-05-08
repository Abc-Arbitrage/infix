package rules

import (
	"errors"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/storage"
)

// RenameFn defines a function to rename a measurement or field
type RenameFn func(string) string

// ErrMissingMeasurementFilter is raised when a config is missing a measurement filter
var ErrMissingMeasurementFilter = errors.New("missing measurement filter")

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
