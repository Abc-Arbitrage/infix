package rules

import (
	"testing"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/assert"
)

func TestDropField_ShouldBuildFromSapmle(t *testing.T) {
	assertBuildFromSample(t, &DropFieldRuleConfig{})
}

func TestDropField_ShouldBuildFail(t *testing.T) {
	data := []struct {
		name string

		config        string
		expectedError error
	}{
		{
			"missing measurement filter",

			`
			[field.strings]
			    hasprefix="bucket_"
			`,
			ErrMissingMeasurementFilter,
		},
		{
			"missing field filter",

			`
			[measurement.strings]
			    hasprefix="linux."
			`,
			ErrMissingFieldFilter,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assertBuildFromStringCallback(t, d.config, &DropFieldRuleConfig{}, func(r Rule, err error) {
				assert.Nil(t, r)
				assert.Equal(t, err, d.expectedError)
			})
		})
	}
}

func TestDropField_ShouldApplyAndDelete(t *testing.T) {
	measurementFilter := filter.NewIncludeFilter([]string{"mem"})
	fieldFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{
		Contains:    "",
		ContainsAny: "",
		Equal:       "used",
		EqualFold:   "",
		HasPrefix:   "",
		HasSuffix:   "",
	})

	assert.NoError(t, err)

	rule := NewDropField(measurementFilter, fieldFilter, &filter.AlwaysTrueFilter{})

	key := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	var data = []struct {
		key    []byte
		values []tsm1.Value

		expectedKey []byte
	}{
		{key("mem,host=my-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, nil},
		{key("mem,host=other-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, nil},
		{key("mem,host=my-host", "available"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("mem,host=my-host", "available")},
		{key("cpu,host=my-host", "idle"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("cpu,host=my-host", "idle")},
		{key("cpu,host=my-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("cpu,host=my-host", "used")},
	}

	for _, d := range data {
		key, _, err := rule.Apply(d.key, d.values)

		assert.NoError(t, err)
		assert.Equalf(t, key, d.expectedKey, "expected key '%s' but got '%s'", d.expectedKey, key)
	}
}

func TestDropField_ShouldUpdateFieldsIndex(t *testing.T) {
	measurementFilter := filter.NewIncludeFilter([]string{"mem"})
	fieldFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{
		Contains:    "",
		ContainsAny: "",
		Equal:       "used",
		EqualFold:   "",
		HasPrefix:   "",
		HasSuffix:   "",
	})

	assert.NoError(t, err)

	rule := NewDropField(measurementFilter, fieldFilter, &filter.AlwaysTrueFilter{})

	key := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	measurements := []measurementFields{
		{
			measurement: "mem",
			fields: map[string]influxql.DataType{
				"used":      influxql.Float,
				"available": influxql.Float,
			},
		},
		{
			measurement: "cpu",
			fields: map[string]influxql.DataType{
				"idle": influxql.Float,
				"used": influxql.Float,
			},
		},
	}

	shard := newTestShard(measurements)

	var data = []struct {
		key    []byte
		values []tsm1.Value

		expectedKey []byte
	}{
		{key("mem,host=my-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, nil},
		{key("mem,host=other-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, nil},
		{key("mem,host=my-host", "available"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("mem,host=my-host", "available")},
		{key("cpu,host=my-host", "idle"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("cpu,host=my-host", "idle")},
		{key("cpu,host=my-host", "used"), []tsm1.Value{tsm1.NewFloatValue(0, 3.5)}, key("cpu,host=my-host", "used")},
	}

	assert.True(t, rule.StartShard(shard))

	remainingFields := make(map[string][]string)
	deletedFields := make(map[string][]string)

	addFieldTo := func(fieldsMap map[string][]string, measurement string, name string) {
		if fields, ok := fieldsMap[measurement]; ok {
			fieldsMap[measurement] = append(fields, name)
		} else {
			fieldsMap[measurement] = []string{name}
		}
	}

	for _, d := range data {
		key, _, err := rule.Apply(d.key, d.values)

		assert.NoError(t, err)
		assert.Equalf(t, key, d.expectedKey, "expected key '%s' but got '%s'", d.expectedKey, key)

		_, field := tsm1.SeriesAndFieldFromCompositeKey(d.key)
		measurement, _ := models.ParseKey(d.key)

		if key == nil {
			addFieldTo(deletedFields, measurement, string(field))
		} else {
			addFieldTo(remainingFields, measurement, string(field))
		}
	}

	assert.NoError(t, rule.EndShard())

	for m, fs := range remainingFields {
		fields := shard.FieldsIndex.FieldsByString(m)
		assert.NotNil(t, fields)

		for _, f := range fs {
			field := fields.Field(f)
			assert.NotNil(t, field)
		}
	}

	for m, fs := range deletedFields {
		fields := shard.FieldsIndex.FieldsByString(m)
		assert.NotNil(t, fields)

		for _, f := range fs {
			field := fields.Field(f)
			assert.Nil(t, field)
		}
	}
}
