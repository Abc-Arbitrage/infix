package rules

// Set defines a set of rules to apply
type Set interface {
	Rules() []Rule
}

// ChainingSet is a set of rules that are applied in chain
type ChainingSet struct {
	rules []Rule
}

// NewChainingSet creates a new chaining set
func NewChainingSet(rules []Rule) *ChainingSet {
	return &ChainingSet{
		rules: rules,
	}
}

// Rules implement the Set interface
func (c *ChainingSet) Rules() []Rule {
	return c.rules
}
