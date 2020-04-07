package rules

import (
	"fmt"
	"log"
	"strconv"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"

	"github.com/oktal/infix/logging"
	"github.com/oktal/infix/storage"
)

// UpdateMeasurementFieldTypeRule will update a field type for a given measurement
type UpdateMeasurementFieldTypeRule struct {
	check       bool
	measurement string
	fieldKey    string
	fromType    influxql.DataType
	toType      influxql.DataType

	shard storage.ShardInfo
	count uint64

	logger *log.Logger
}

// NewUpdateMeasurementFieldType creates an UpdateMeasurementFieldTypeRule
func NewUpdateMeasurementFieldType(measurement string, fieldKey string, fromType influxql.DataType, toType influxql.DataType) *UpdateMeasurementFieldTypeRule {
	return &UpdateMeasurementFieldTypeRule{
		measurement: measurement,
		fieldKey:    fieldKey,
		fromType:    fromType,
		toType:      toType,
		count:       0,
		logger:      logging.GetLogger("UpdateMeasurementFieldTypeRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *UpdateMeasurementFieldTypeRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *UpdateMeasurementFieldTypeRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) Start() {

}

// End implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) End() {

}

// StartShard implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) StartShard(info storage.ShardInfo) {
	r.shard = info
	r.count = 0
}

// EndShard implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) EndShard() error {
	if r.count > 0 {
		shard := r.shard
		if shard.FieldsIndex == nil {
			return fmt.Errorf("No index for shard id %d", r.shard.ID)
		}

		fields := shard.FieldsIndex.FieldsByString(r.measurement)
		if fields == nil {
			return fmt.Errorf("Could not find fields. SharId: %d Measurement %s", shard.ID, r.measurement)
		}

		field := fields.Field(r.fieldKey)
		if field == nil {
			return fmt.Errorf("Could not find field. ShardId: %d Measurement %s Field %s", shard.ID, r.measurement, r.fieldKey)
		}

		if field.Type != r.fromType {
			r.logger.Printf("Converting type of field '%s' measurement '%s' from '%s' to '%s'", r.fieldKey, r.measurement, r.fromType, r.toType)
			field.Type = r.toType
		}

		if !r.check {
			return shard.FieldsIndex.Save()
		}
	}

	return nil
}

// StartTSM implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) StartTSM(path string) {
}

// EndTSM implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) StartWAL(path string) {
}

// EndWAL implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) EndWAL() {
}

// Apply implements Rule interface
func (r *UpdateMeasurementFieldTypeRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, _ := models.ParseKey(series)

	if measurement != r.measurement {
		return key, values, nil
	}

	var newValues []tsm1.Value

	count := 0

	for _, value := range values {
		v, ok, err := EnsureValueType(value, r.toType)
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			count++
		}

		newValues = append(newValues, v)
	}

	r.logger.Printf("Converting '%d' values to type '%s' for field '%s' of measurement '%s'", count, r.toType, r.fieldKey, measurement)
	return key, newValues, nil
}

// EnsureValueType casts a Value to a given data type
func EnsureValueType(value tsm1.Value, expectedType influxql.DataType) (tsm1.Value, bool, error) {
	switch expectedType {
	case influxql.Float:
		return castToFloat(value)
	case influxql.Integer:
		return castToInteger(value)
	case influxql.Boolean:
		return castToBoolean(value)
	case influxql.String:
		return castToString(value)
	default:
		return nil, false, fmt.Errorf("Invalid cast for data type '%s'", expectedType)
	}
}

func castToFloat(value tsm1.Value) (tsm1.Value, bool, error) {
	switch value.Value().(type) {
	case float64:
		return value, true, nil
	case int64:
		return tsm1.NewFloatValue(value.UnixNano(), float64(value.Value().(int64))), false, nil
	case uint64:
		return tsm1.NewFloatValue(value.UnixNano(), float64(value.Value().(uint64))), false, nil
	case bool:
		return nil, false, fmt.Errorf("Could not cast bool value to float")
	case string:
		v, err := strconv.ParseFloat(value.Value().(string), 64)
		if err != nil {
			return nil, false, err
		}

		return tsm1.NewFloatValue(value.UnixNano(), v), false, nil
	default:
		return nil, false, fmt.Errorf("Unknown value type")
	}
}

func castToInteger(value tsm1.Value) (tsm1.Value, bool, error) {
	switch value.Value().(type) {
	case float64:
		return tsm1.NewIntegerValue(value.UnixNano(), int64(value.Value().(float64))), false, nil
	case int64:
		return value, true, nil
	case uint64:
		return value, true, nil
	case bool:
		b := value.Value().(bool)
		return tsm1.NewIntegerValue(value.UnixNano(), int64(btoi(b))), false, nil
	case string:
		v, err := strconv.ParseInt(value.Value().(string), 10, 64)
		if err != nil {
			return nil, false, err
		}

		return tsm1.NewIntegerValue(value.UnixNano(), v), false, nil
	default:
		return nil, false, fmt.Errorf("Unknown value type")
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func castToBoolean(value tsm1.Value) (tsm1.Value, bool, error) {
	switch value.Value().(type) {
	case float64:
		v := value.Value().(float64)
		return tsm1.NewBooleanValue(value.UnixNano(), v != 0.0), false, nil
	case int64:
		v := value.Value().(int64)
		return tsm1.NewBooleanValue(value.UnixNano(), v == 1), false, nil
	case uint64:
		v := value.Value().(uint64)
		return tsm1.NewBooleanValue(value.UnixNano(), v == 1), false, nil
	case bool:
		return value, true, nil
	case string:
		v, err := strconv.ParseBool(value.Value().(string))
		if err != nil {
			return nil, false, err
		}

		return tsm1.NewBooleanValue(value.UnixNano(), v), false, nil
	default:
		return nil, false, fmt.Errorf("Unknown value type")
	}
}

func castToString(value tsm1.Value) (tsm1.Value, bool, error) {
	switch value.Value().(type) {
	case float64:
		v := value.Value().(float64)
		return tsm1.NewStringValue(value.UnixNano(), strconv.FormatFloat(v, 'f', 6, 64)), false, nil
	case int64:
		v := value.Value().(int64)
		return tsm1.NewStringValue(value.UnixNano(), strconv.FormatInt(v, 10)), false, nil
	case uint64:
		v := value.Value().(uint64)
		return tsm1.NewStringValue(value.UnixNano(), strconv.FormatUint(v, 10)), false, nil
	case bool:
		v := value.Value().(bool)
		return tsm1.NewStringValue(value.UnixNano(), strconv.FormatBool(v)), false, nil
	case string:
		return value, true, nil
	default:
		return nil, false, fmt.Errorf("Unknown value type")
	}
}
