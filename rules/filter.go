package rules

import "regexp"

// Filter defines an interface to filter and skip keys when applying rules
type Filter interface {
	Filter(key []byte) bool
}

// FilterSet defines a set of filters that must pass
type FilterSet struct {
	filters []Filter
}

// Filter implements the Filter interface
func (f *FilterSet) Filter(key []byte) bool {
	for _, f := range f.filters {
		if f.Filter(key) {
			return true
		}
	}

	return false
}

// PatternFilter is a Filter based on regexp
type PatternFilter struct {
	pattern *regexp.Regexp
}

// Filter implements the Filter interface
func (f *PatternFilter) Filter(key []byte) bool {
	return f.pattern.Match(key)
}

// IncludeFilter defines a filter to only include a list of strings
type IncludeFilter struct {
	includes []string
}

// Filter implements the Filter interface
func (f *IncludeFilter) Filter(key []byte) bool {
	s := string(key)
	for _, inc := range f.includes {
		if inc == s {
			return true
		}
	}

	return false
}

// ExcludeFilter defines a filter to exclude a list of strings
type ExcludeFilter struct {
	excludes []string
}

// Filter implements the Filter interface
func (f *ExcludeFilter) Filter(key []byte) bool {
	s := string(key)
	for _, inc := range f.excludes {
		if inc == s {
			return false
		}
	}

	return true
}

// AlwaysTrueFilter is a Filter that always returns true
type AlwaysTrueFilter struct {
}

// Filter implements Filter interface
func (f *AlwaysTrueFilter) Filter(key []byte) bool {
	return true
}
