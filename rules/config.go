package rules

import (
	"fmt"
	"io/ioutil"

	"github.com/naoina/toml"
	"github.com/naoina/toml/ast"
	"github.com/Abc-Arbitrage/infix/filter"
)

// Config represents a configuration for a rule
type Config interface {
	Sample() string

	Build() (Rule, error)
}

// LoadConfig will load rules from a TOML configuration file
func LoadConfig(path string) ([]Rule, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	table, err := toml.Parse(data)
	if err != nil {
		return nil, err
	}

	var rules []Rule

	for name, val := range table.Fields {
		subTable, ok := val.(*ast.Table)
		if !ok {
			return nil, fmt.Errorf("%s: invalid configuration %s", path, name)
		}

		switch name {
		case "rules":
			for ruleName, ruleVal := range subTable.Fields {
				ruleSubTable, ok := ruleVal.([]*ast.Table)
				if !ok {
					return nil, fmt.Errorf("%s: invalid configuration %s", path, ruleName)
				}

				for _, r := range ruleSubTable {
					rule, err := loadRule(ruleName, r)
					if err != nil {
						return nil, fmt.Errorf("%s: %s: %s", path, ruleName, err)
					}
					rules = append(rules, rule)
				}
			}
		case "filters":
		default:
			return nil, fmt.Errorf("%s: unsupported config file format %s", path, name)
		}
	}

	return rules, nil
}

func loadRule(name string, table *ast.Table) (Rule, error) {
	config, err := NewRule(name)
	if err != nil {
		return nil, err
	}

	if err := filter.UnmarshalConfig(table, config); err != nil {
		return nil, err
	}

	return config.Build()
}
