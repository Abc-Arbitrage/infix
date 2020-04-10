package filter

import (
	"fmt"
)

func init() {
	RegisterFilter("pattern", func() Config {
		return &PatternFilterConfig{}
	})
	RegisterFilter("file", func() Config {
		return &FileFilterConfiguration{}
	})
	RegisterFilter("serie", func() Config {
		return &SerieFilterConfig{}
	})
	RegisterFilter("where", func() Config {
		return &WhereFilterConfig{
			Where: make(map[string]string),
		}
	})
}

// NewFilterFunc represents a callback to register a filter's configuration to be able to load it from toml
type NewFilterFunc func() Config

var newFilterFuncs = make(map[string]NewFilterFunc)

// RegisterFilter registers a filter with the given name and config creation callback
func RegisterFilter(name string, fn NewFilterFunc) {
	if _, ok := newFilterFuncs[name]; ok {
		panic(fmt.Sprintf("filter %s has already been registered", name))
	}
	newFilterFuncs[name] = fn
}

// NewFilter creates a new rule configuration based on its registration name
func NewFilter(name string) (Config, error) {
	fn, ok := newFilterFuncs[name]
	if !ok {
		return nil, fmt.Errorf("No registered filter '%s'", name)
	}

	return fn(), nil
}
