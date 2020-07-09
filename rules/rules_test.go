package rules

import (
	"testing"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/naoina/toml"
	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/storage"
	"github.com/stretchr/testify/assert"
)

type measurementFields struct {
	measurement string
	fields      map[string]influxql.DataType
}

func assertBuildFromSample(t *testing.T, config Config) {
	assertBuildFromStringCallback(t, config.Sample(), config, func(r Rule, err error) {
		assert.NoError(t, err)
		assert.NotNil(t, r)
	})
}

func assertBuildFromStringCallback(t *testing.T, tomlConfig string, config Config, callback func(r Rule, err error)) {
	table, err := toml.Parse([]byte(tomlConfig))
	assert.NoError(t, err)
	assert.NotNil(t, table)

	err = filter.UnmarshalConfig(table, config)
	assert.NoError(t, err)

	rule, err := config.Build()
	callback(rule, err)
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
