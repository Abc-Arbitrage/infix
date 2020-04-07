package rules

import (
	"testing"

	"github.com/influxdata/influxql"

	"github.com/oktal/infix/storage"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
)

func TestRenameMeasurement_ShouldApplyAndRename(t *testing.T) {
	rule := NewRenameMeasurement("cpu", "linux.cpu")

	var tags = map[string]string{
		"host": "my-host",
	}

	var data = []struct {
		key    []byte
		newKey []byte
		values []tsm1.Value
	}{
		{makeKey("cpu", tags, "idle"), makeKey("linux.cpu", tags, "idle"), []tsm1.Value{tsm1.NewValue(0, 10.0)}},
		{makeKey("disk", tags, "usage"), makeKey("disk", tags, "usage"), []tsm1.Value{tsm1.NewValue(0, 20.0)}},
	}

	for _, d := range data {
		newKey, values, err := rule.Apply([]byte(d.key), d.values)
		assert.NoError(t, err)
		assert.Equal(t, len(d.values), len(values))
		assert.Equal(t, newKey, d.newKey)
	}
}

func TestRenameMeasurement_ShouldApplyAndRenameWithPattern(t *testing.T) {
	rule, err := NewRenameMeasurementWithPattern("^(cpu|disk)$", func(measurement string) string {
		return "linux." + measurement
	})

	assert.NoError(t, err)

	var tags = map[string]string{
		"host": "my-host",
	}

	var data = []struct {
		key    []byte
		newKey []byte
		values []tsm1.Value
	}{
		{makeKey("cpu", tags, "idle"), makeKey("linux.cpu", tags, "idle"), []tsm1.Value{tsm1.NewValue(0, 10.0)}},
		{makeKey("disk", tags, "usage"), makeKey("linux.disk", tags, "usage"), []tsm1.Value{tsm1.NewValue(0, 20.0)}},
	}

	for _, d := range data {
		newKey, values, err := rule.Apply([]byte(d.key), d.values)
		assert.NoError(t, err)
		assert.Equal(t, len(d.values), len(values))
		assert.Equal(t, newKey, d.newKey)
	}
}

func TestRenameMeasurement_ShouldUpdateFieldsIndex(t *testing.T) {
	rule, err := NewRenameMeasurementWithPattern("^(cpu|disk)$", func(measurement string) string {
		return "linux." + measurement
	})
	assert.NoError(t, err)
	rule.CheckMode(true)

	measurements := []measurementFields{
		{
			"cpu", map[string]influxql.DataType{
				"idle": influxql.Integer,
			},
		},
		{
			"disk", map[string]influxql.DataType{
				"usage": influxql.Float,
			},
		},
	}

	var tags = map[string]string{
		"host": "my-host",
	}

	var data = []struct {
		key    []byte
		newKey []byte
		values []tsm1.Value
	}{
		{makeKey("cpu", tags, "idle"), makeKey("linux.cpu", tags, "idle"), []tsm1.Value{tsm1.NewValue(0, 10.0)}},
		{makeKey("disk", tags, "usage"), makeKey("linux.disk", tags, "usage"), []tsm1.Value{tsm1.NewValue(0, 20.0)}},
	}

	shard := newTestShard(measurements)

	rule.StartShard(shard)

	for _, d := range data {
		_, _, err := rule.Apply([]byte(d.key), d.values)
		assert.NoError(t, err)
	}

	assert.Equal(t, rule.Count(), 2)

	err = rule.EndShard()
	assert.NoError(t, err)

	for _, d := range data {
		oldMeasurement, _ := models.ParseKey(d.key)
		newMeasurement, _ := models.ParseKey(d.newKey)

		oldFields := shard.FieldsIndex.FieldsByString(oldMeasurement)
		newFields := shard.FieldsIndex.FieldsByString(newMeasurement)

		assert.Nil(t, oldFields)
		assert.NotNil(t, newFields)

		for _, m := range measurements {
			if m.measurement == newMeasurement {
				for key, iflxType := range m.fields {
					field := newFields.Field(key)
					assert.NotNil(t, field)
					assert.Equal(t, field.Type, iflxType)
				}
				break
			}
		}
	}

	assert.Equal(t, rule.Count(), 0)
}

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

func makeKey(measurement string, tags map[string]string, field string) []byte {
	t := models.NewTags(tags)
	seriesKey := models.MakeKey([]byte(measurement), t)
	return tsm1.SeriesFieldKeyBytes(string(seriesKey), field)
}
