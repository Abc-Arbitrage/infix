package rules

import (
	"fmt"
	"io"
	"sort"
)

func init() {
	RegisterRule("drop-measurement", func() Config {
		return &DropMeasurementRuleConfig{}
	})
	RegisterRule("drop-serie", func() Config {
		return &DropSerieRuleConfig{}
	})
    RegisterRule("drop-field", func()  Config {
        return &DropFieldRuleConfig{}
    })
	RegisterRule("old-serie", func() Config {
		return &OldSerieRuleConfig{}
	})
	RegisterRule("rename-field", func() Config {
		return &RenameFieldRuleConfig{}
	})
	RegisterRule("rename-measurement", func() Config {
		return &RenameMeasurementRuleConfig{}
	})
	RegisterRule("rename-tag", func() Config {
		return &RenameTagRuleConfig{}
	})
	RegisterRule("show-field-key-multiple-types", func() Config {
		return &ShowFieldKeyMultipleTypesConfig{}
	})
	RegisterRule("update-field-type", func() Config {
		return &UpdateFieldTypeRuleConfig{}
	})
	RegisterRule("update-tag-value", func() Config {
		return &UpdateTagValueRuleConfig{}
	})
}

// NewRuleFunc represents a callback to register a rule's configuration to be able to load it from toml
type NewRuleFunc func() Config

var newRuleFuncs = make(map[string]NewRuleFunc)

// RegisterRule registers a rule with the given name and config creation callback
func RegisterRule(name string, fn NewRuleFunc) {
	if _, ok := newRuleFuncs[name]; ok {
		panic(fmt.Sprintf("rule %s has already been registered", name))
	}
	newRuleFuncs[name] = fn
}

// NewRule creates a new rule configuration based on its registration name
func NewRule(name string) (Config, error) {
	fn, ok := newRuleFuncs[name]
	if !ok {
		return nil, fmt.Errorf("no registered rule '%s'", name)
	}

	return fn(), nil
}

// PrintList print a list of registered rules with a sample config
func PrintList(out io.Writer) {
	sortedKeys := make([]string, 0, len(newRuleFuncs))
	for key := range newRuleFuncs {
		sortedKeys = append(sortedKeys, key)
	}

	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		configFunc := newRuleFuncs[key]
		config := configFunc()
		fmt.Fprintln(out, key)
		fmt.Fprintln(out, config.Sample())
	}
}
