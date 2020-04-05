package rules

import (
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/storage"
)

// Rule represents a rule to apply to a given TSM or WAL entry
type Rule interface {
	CheckMode(check bool)

	StartShard(info storage.ShardInfo)
	EndShard() error

	StartTSM(path string)
	EndTSM()

	StartWAL(path string)
	EndWAL()

	Apply(key []byte, values []tsm1.Value) (newKey []byte, newValues []tsm1.Value, err error)
}
