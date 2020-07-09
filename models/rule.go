package models

import (
	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/rules"
)

// Rule represents a Rule with a potential filter
type Rule struct {
	Rule   rules.Rule
	Filter filter.Filter
}
