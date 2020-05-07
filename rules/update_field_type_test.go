package rules

import (
	"strconv"
	"testing"

	"github.com/influxdata/influxql"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/filter"

	"github.com/stretchr/testify/assert"
)

func TestUpdateFieldType_ShouldBuildFromSample(t *testing.T) {
	assertBuildFromSample(t, &UpdateFieldTypeRuleConfig{})
}

func TestUpdateFieldType_ShouldBuildFailInvalidFromType(t *testing.T) {
	config := `
		 fromType="char"
		 toType="integer"
		 [measurement.strings]
			equal="cpu"
		 [field.pattern]
		 	pattern="^(idle|active)"
	`

	assertBuildFromStringCallback(t, config, &UpdateFieldTypeRuleConfig{}, func(r Rule, err error) {
		assert.Nil(t, r)
		assert.EqualError(t, err, "Unknown FromType 'char'")
	})
}

func TestUpdateFieldType_ShouldBuildFailInvalidToType(t *testing.T) {
	config := `
		 fromType="integer"
		 toType="char"
		 [measurement.strings]
			equal="cpu"
		 [field.pattern]
		 	pattern="^(idle|active)"
	`

	assertBuildFromStringCallback(t, config, &UpdateFieldTypeRuleConfig{}, func(r Rule, err error) {
		assert.Nil(t, r)
		assert.EqualError(t, err, "Unknown ToType 'char'")
	})
}

func TestUpdateFieldType_ShouldApply(t *testing.T) {
	measurementFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{HasSuffix: ".gauge"})
	assert.NoError(t, err)
	fieldFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{Equal: "value"})
	assert.NoError(t, err)

	key := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	intVal := func(ts int64, v int64) tsm1.Value {
		return tsm1.NewIntegerValue(ts, v)
	}

	floatVal := func(ts int64, v float64) tsm1.Value {
		return tsm1.NewFloatValue(ts, v)
	}

	boolVal := func(ts int64, v bool) tsm1.Value {
		return tsm1.NewBooleanValue(ts, v)
	}

	strVal := func(ts int64, v string) tsm1.Value {
		return tsm1.NewStringValue(ts, v)
	}

	toInt := func(v float64) int64 {
		return int64(v)
	}

	type testData struct {
		name string

		key    []byte
		values []tsm1.Value

		expectedKey    []byte
		expectedValues []tsm1.Value
		expectedError  error
	}

	var tests = []struct {
		name string

		fromType influxql.DataType
		toType   influxql.DataType

		data []testData
	}{
		{
			"should convert integer to float",
			influxql.Integer,
			influxql.Float,
			[]testData{
				{
					"convert integer to float",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, 12), intVal(1, 15), intVal(2, 8)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12), floatVal(1, 15), floatVal(2, 8)},
					nil,
				},
				{
					"keep float value",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12), floatVal(1, 15), floatVal(2, 8)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12), floatVal(1, 15), floatVal(2, 8)},
					nil,
				},
			},
		},
		{
			"should convert integer to boolean",
			influxql.Integer,
			influxql.Boolean,
			[]testData{
				{
					"convert integer to boolean",

					key("node_up.gauge", "value"),
					[]tsm1.Value{intVal(0, 0), intVal(1, 1)},

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},
					nil,
				},

				{
					"keep float value",

					key("node_up.gauge", "value"),
					[]tsm1.Value{floatVal(0, 0), floatVal(1, 1)},

					key("node_up.gauge", "value"),
					[]tsm1.Value{floatVal(0, 0), floatVal(1, 1)},
					nil,
				},

				{
					"keep boolean value",

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},

					nil,
				},
			},
		},
		{
			"should convert integer to string",
			influxql.Integer,
			influxql.String,
			[]testData{
				{
					"convert integer to stringj",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, 12), intVal(1, 15), intVal(2, 8)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{strVal(0, "12"), strVal(1, "15"), strVal(2, "8")},
					nil,
				},
				{
					"keep float value",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12), floatVal(1, 15), floatVal(2, 8)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12), floatVal(1, 15), floatVal(2, 8)},
					nil,
				},
			},
		},
		{
			"should convert float to integer",
			influxql.Float,
			influxql.Integer,
			[]testData{
				{
					"convert float to integer",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12.8), floatVal(1, 15.2), floatVal(2, 20.3)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, toInt(12.8)), intVal(1, toInt(15.2)), intVal(2, toInt(20.3))},
					nil,
				},

				{
					"keep integer value",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, 12), intVal(1, 15), intVal(2, 20)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, 12), intVal(1, 15), intVal(2, 20)},
					nil,
				},
			},
		},
		{
			"should convert boolean to string",
			influxql.Boolean,
			influxql.String,
			[]testData{
				{
					"convert boolean to string",

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, true), boolVal(1, false)},

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, strconv.FormatBool(true)), strVal(1, strconv.FormatBool(false))},
					nil,
				},
				{
					"keep string",

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, "up"), strVal(1, "down")},

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, "up"), strVal(1, "down")},
					nil,
				},
			},
		},
		{
			"should convert string to integer",
			influxql.String,
			influxql.Integer,
			[]testData{
				{
					"convert string to integer",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{strVal(0, "12"), strVal(1, "15"), strVal(2, "20")},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{intVal(0, 12), intVal(1, 15), intVal(2, 20)},
					nil,
				},

				{
					"conversion error",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{strVal(0, "12.8"), strVal(1, "15.2"), strVal(2, "20.3")},

					nil,
					nil,
					strconv.ErrSyntax,
				},
			},
		},
		{
			"should convert string to float",
			influxql.String,
			influxql.Float,
			[]testData{
				{
					"convert string to float",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{strVal(0, "12.8"), strVal(1, "15.2"), strVal(2, "20.3")},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12.8), floatVal(1, 15.2), floatVal(2, 20.3)},
					nil,
				},

				{
					"keep float value",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12.8), floatVal(1, 15.2), floatVal(2, 20.3)},

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{floatVal(0, 12.8), floatVal(1, 15.2), floatVal(2, 20.3)},
					nil,
				},

				{
					"conversion error",

					key("memory_bytes.gauge", "value"),
					[]tsm1.Value{strVal(0, "abc"), strVal(1, "def"), strVal(2, "ghj")},

					nil,
					nil,
					strconv.ErrSyntax,
				},
			},
		},
		{
			"should convert string to boolean",
			influxql.String,
			influxql.Boolean,
			[]testData{
				{
					"convert string as int to boolean",

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, "0"), strVal(1, "1")},

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},
					nil,
				},

				{
					"convert string as boolean literal to boolean",

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, "false"), strVal(1, "true")},

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},
					nil,
				},

				{
					"keep bool value",

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},

					key("node_up.gauge", "value"),
					[]tsm1.Value{boolVal(0, false), boolVal(1, true)},
					nil,
				},

				{
					"conversion error",

					key("node_up.gauge", "value"),
					[]tsm1.Value{strVal(0, "up"), strVal(1, "down")},

					nil,
					nil,
					strconv.ErrSyntax,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rule := NewUpdateFieldType(measurementFilter, fieldFilter, test.fromType, test.toType)
			for _, d := range test.data {
				t.Run(d.name, func(t *testing.T) {
					key, values, err := rule.Apply(d.key, d.values)
					assert.Equal(t, d.expectedKey, key)
					assert.Equal(t, d.expectedValues, values)
					if d.expectedError != nil {
						numErr := err.(*strconv.NumError)
						assert.Equal(t, numErr.Err, d.expectedError)
					} else {
						assert.NoError(t, err)
					}
				})
			}
		})
	}
}
