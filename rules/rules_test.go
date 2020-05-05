package rules

import (
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/oktal/infix/storage"
)

type measurementFields struct {
	measurement string
	fields      map[string]influxql.DataType
}

func newTestShard(measurements []measurementFields) storage.ShardInfo {
	index, err := tsdb.NewMeasurementFieldSet("path")
	if err != nil {
		panic(err)
	}

	for _, m := range measurements {
		measurementFields := index.CreateFieldsIfNotExists([]byte(m.measurement))
		for key, iflxType := range m.fields {
			measurementFields.CreateFieldIfNotExists([]byte(key), iflxType)
		}
	}

	return storage.ShardInfo{
		ID:              12,
		Database:        "test_db",
		RetentionPolicy: "test_rp",
		Path:            "/var/lib/influxdb/data/test_db/test_rp/12",
		FieldsIndex:     index,
	}
}
