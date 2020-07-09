package rules

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/logging"
	"github.com/Abc-Arbitrage/infix/storage"
)

// ErrUnknownType is raised when failing to parse an InfluxQL Type
var ErrUnknownType = errors.New("unknown InfluxQL type")

// UpdateFieldTypeRule will update a field type for a given measurement
type UpdateFieldTypeRule struct {
	check bool
	shard storage.ShardInfo

	measurementFilter filter.Filter
	fieldFilter       filter.Filter

	fromType influxql.DataType
	toType   influxql.DataType

	updates map[string][]string

	logger *log.Logger
}

// UpdateFieldTypeRuleConfig represents the toml configuration for UpdateFieldTypeRule
type UpdateFieldTypeRuleConfig struct {
	Measurement filter.Filter
	Field       filter.Filter

	FromType string
	ToType   string
}

// NewUpdateFieldType creates an UpdateFieldTypeRule
func NewUpdateFieldType(measurementFilter filter.Filter, fieldFilter filter.Filter, fromType influxql.DataType, toType influxql.DataType) *UpdateFieldTypeRule {
	return &UpdateFieldTypeRule{
		measurementFilter: measurementFilter,
		fieldFilter:       fieldFilter,
		fromType:          fromType,
		toType:            toType,
		updates:           make(map[string][]string),
		logger:            logging.GetLogger("UpdateFieldTypeRule"),
	}
}

// CheckMode sets the check mode on the rule
func (r *UpdateFieldTypeRule) CheckMode(check bool) {
	r.check = check
}

// Flags implements Rule interface
func (r *UpdateFieldTypeRule) Flags() int {
	return Standard
}

// WithLogger sets the logger on the rule
func (r *UpdateFieldTypeRule) WithLogger(logger *log.Logger) {
	r.logger = logger
}

// Start implements Rule interface
func (r *UpdateFieldTypeRule) Start() {

}

// End implements Rule interface
func (r *UpdateFieldTypeRule) End() {

}

// StartShard implements Rule interface
func (r *UpdateFieldTypeRule) StartShard(info storage.ShardInfo) bool {
	r.shard = info
	r.updates = make(map[string][]string)
	return true
}

// EndShard implements Rule interface
func (r *UpdateFieldTypeRule) EndShard() error {
	if len(r.updates) > 0 {
		shard := r.shard
		if shard.FieldsIndex == nil {
			return fmt.Errorf("No index for shard id %d", r.shard.ID)
		}

		for m, updates := range r.updates {
			fields := shard.FieldsIndex.FieldsByString(m)
			if fields == nil {
				return fmt.Errorf("Could not find fields. ShardId: %d Measurement: %s", shard.ID, m)
			}

			for _, f := range updates {
				field := fields.Field(f)
				if field == nil {
					return fmt.Errorf("Could not find field. ShardId: %d Measurement: %s Field: %s", shard.ID, m, f)
				}

				if field.Type != r.toType {
					r.logger.Printf("Converting type of field '%s' measurement '%s' from '%s' to '%s'", f, m, r.fromType, r.toType)
					field.Type = r.toType
				}
			}

		}

		if !r.check {
			return shard.FieldsIndex.Save()
		}
	}

	return nil
}

// StartTSM implements Rule interface
func (r *UpdateFieldTypeRule) StartTSM(path string) bool {
	return true
}

// EndTSM implements Rule interface
func (r *UpdateFieldTypeRule) EndTSM() {
}

// StartWAL implements Rule interface
func (r *UpdateFieldTypeRule) StartWAL(path string) bool {
	return true
}

// EndWAL implements Rule interface
func (r *UpdateFieldTypeRule) EndWAL() {
}

// Apply implements Rule interface
func (r *UpdateFieldTypeRule) Apply(key []byte, values []tsm1.Value) ([]byte, []tsm1.Value, error) {
	series, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, _ := models.ParseKey(series)

	if r.measurementFilter.Filter([]byte(measurement)) && r.fieldFilter.Filter(field) {
		var newValues []tsm1.Value

		if influxType, err := tsm1.Values(values).InfluxQLType(); err != nil {
			return nil, nil, err
		} else if influxType != r.fromType || influxType == r.toType {
			newValues = values
		} else {
			for _, value := range values {
				v, ok, err := EnsureValueType(value, r.toType)
				if err != nil {
					return nil, nil, err
				}

				if !ok {
					r.logger.Printf("Converting value to type '%s' for field '%s' of measurement '%s'", r.toType, field, measurement)
					fieldString := string(field)
					if updates, ok := r.updates[measurement]; !ok {
						r.updates[measurement] = append(r.updates[measurement], fieldString)
					} else {
						found := false
						for _, f := range updates {
							if f == fieldString {
								found = true
								break
							}
						}

						if !found {
							r.updates[measurement] = append(r.updates[measurement], fieldString)
						}
					}
				}

				newValues = append(newValues, v)
			}
		}

		return key, newValues, nil
	}

	return key, values, nil
}

// Sample implements Config interface
func (c *UpdateFieldTypeRuleConfig) Sample() string {
	return `
		 fromType="float"
		 toType="integer"
		 [measurement.strings]
			equal="cpu"
		 [field.pattern]
		 	pattern="^(idle|active)"
	`
}

// Build implements Config interface
func (c *UpdateFieldTypeRuleConfig) Build() (Rule, error) {
	fromType := influxql.DataTypeFromString(c.FromType)
	if fromType == influxql.Unknown {
		return nil, ErrUnknownType
	}

	toType := influxql.DataTypeFromString(c.ToType)
	if toType == influxql.Unknown {
		return nil, ErrUnknownType
	}

	if c.Measurement == nil {
		return nil, ErrMissingMeasurementFilter
	}

	if c.Field == nil {
		return nil, ErrMissingFieldFilter
	}

	return NewUpdateFieldType(c.Measurement, c.Field, fromType, toType), nil
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
