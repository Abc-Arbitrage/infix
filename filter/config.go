package filter

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/naoina/toml"
	"github.com/naoina/toml/ast"
)

// Config represents the toml configuration of a filter
type Config interface {
	Sample() string

	Build() (Filter, error)
}

// ManualConfig represents the toml configuration of a filter that must be unmarshaled manually
type ManualConfig interface {
	Config

	Unmarshal(table *ast.Table) error
}

// UnmarshalConfig will unmarshal a Config (either filter.Config or rules.Config) that might contain a Filter field
// from a toml ast.Table
func UnmarshalConfig(table *ast.Table, config interface{}) error {
	e := reflect.ValueOf(config).Elem()
	filterType := reflect.TypeOf((*Filter)(nil)).Elem()

	for i := 0; i < e.NumField(); i++ {
		field := e.Type().Field(i)
		varName := field.Name
		varType := field.Type

		if varType.Implements(filterType) {
			f, err := Unmarshal(table, varName)
			if err != nil {
				return err
			}
			if f != nil {
				e.Field(i).Set(reflect.ValueOf(f))
			}
		}

	}

	return nil
}

// Unmarshal will unmarshal a filter from a toml table
func Unmarshal(table *ast.Table, name string) (Filter, error) {
	for filterName, filterVal := range table.Fields {
		if strings.EqualFold(filterName, name) {
			subFilter, ok := filterVal.(*ast.Table)
			if !ok {
				return nil, fmt.Errorf("Invalid filter configuration %s", filterName)
			}

			var keys []string
			for k := range subFilter.Fields {
				keys = append(keys, k)
			}

			if len(keys) > 1 {
				return nil, fmt.Errorf("Invalid filter configuration %s", filterName)
			}

			filterField, ok := subFilter.Fields[keys[0]].(*ast.Table)
			if !ok {
				return nil, fmt.Errorf("Invalid filter configuration %s", filterName)
			}
			config, err := NewFilter(keys[0])
			if err != nil {
				return nil, err
			}
			err = UnmarshalConfig(filterField, config)
			if err != nil {
				return nil, err
			}
			if err := unmarshalTable(keys[0], filterField, config); err != nil {
				return nil, err
			}
			delete(table.Fields, filterName)

			return config.Build()
		}
	}

	return nil, nil
}

func unmarshalTable(name string, table *ast.Table, config Config) error {
	if manualConfig, ok := config.(ManualConfig); ok {
		return manualConfig.Unmarshal(table)
	}
	return toml.UnmarshalTable(table, config)
}
