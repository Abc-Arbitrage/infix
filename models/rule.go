package models

import (
	"github.com/oktal/infix/filter"
	"github.com/oktal/infix/rules"
)

// Rule represents a Rule with a potential filter
type Rule struct {
	Rule   rules.Rule
	Filter filter.Filter
}
