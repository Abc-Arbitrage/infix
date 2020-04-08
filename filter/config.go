package filter

import (
	"fmt"
	"strings"

	"github.com/naoina/toml"
	"github.com/naoina/toml/ast"
)

// Config represents the toml configuration of a filter
type Config interface {
	Sample() string

	Build() (Filter, error)
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
			delete(table.Fields, filterName)
			config, err := NewFilter(keys[0])
			if err != nil {
				return nil, err
			}
			if err := toml.UnmarshalTable(filterField, config); err != nil {
				return nil, err
			}

			return config.Build()
		}
	}

	return nil, fmt.Errorf("Failed to unmarshal filter. Could not find %s in TOML", name)
}
