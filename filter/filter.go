package filter

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/naoina/toml/ast"
)

// Make sure that WhereFilterConfig is a ManualConfig
var _ ManualConfig = &WhereFilterConfig{}

// Filter defines an interface to filter and skip keys when applying rules
type Filter interface {
	Filter(key []byte) bool
}

// Set defines a set of filters that must pass
type Set struct {
	filters []Filter
}

// Filter implements the Filter interface
func (f *Set) Filter(key []byte) bool {
	for _, f := range f.filters {
		if f.Filter(key) {
			return true
		}
	}

	return false
}

// PatternFilter is a Filter based on regexp
type PatternFilter struct {
	Pattern *regexp.Regexp
}

// PatternFilterConfig represents the toml configuration of a pattern filter
type PatternFilterConfig struct {
	Pattern string
}

// NewPatternFilter creates a new PatternFilter with the given pattern
func NewPatternFilter(pattern string) (*PatternFilter, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	pf := &PatternFilter{
		Pattern: r,
	}

	return pf, nil
}

// Filter implements the Filter interface
func (f *PatternFilter) Filter(key []byte) bool {
	return f.Pattern.Match(key)
}

// Sample implement Config interface
func (c *PatternFilterConfig) Sample() string {
	return `
		pattern="^(cpu|disk)$"
	`
}

// Build implements Config interface
func (c *PatternFilterConfig) Build() (Filter, error) {
	if c.Pattern == "" {
		return nil, fmt.Errorf("pattern must not be empry")
	}
	f, err := NewPatternFilter(c.Pattern)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// IncludeFilter defines a filter to only include a list of strings
type IncludeFilter struct {
	includes []string
}

// NewIncludeFilter creates a new IncludeFilter
func NewIncludeFilter(includes []string) *IncludeFilter {
	return &IncludeFilter{
		includes: includes,
	}
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

// NewExcludeFilter creates a new ExcludeFilter
func NewExcludeFilter(excludes []string) *ExcludeFilter {
	return &ExcludeFilter{
		excludes: excludes,
	}
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

// AlwaysTrueFilter is a Filter that is always true
type AlwaysTrueFilter struct {
}

// Filter implements Filter interface
func (f *AlwaysTrueFilter) Filter(key []byte) bool {
	return true
}

// AlwaysFalseFilter is a Filter that is always false
type AlwaysFalseFilter struct {
}

// Filter implements Filter interface
func (f *AlwaysFalseFilter) Filter(key []byte) bool {
	return false
}


// FuncFilter is a filter based on a filtering function
type FuncFilter struct {
	filterFn func(key []byte) bool
}

// Filter implements Filter interface
func (f *FuncFilter) Filter(key []byte) bool {
	return f.filterFn(key)
}

// MeasurementFilter defines a filter restricted to measurement part of a key
type MeasurementFilter struct {
	filter Filter
}

// NewMeasurementFilter creates a new MeasurementFilter
func NewMeasurementFilter(filter Filter) *MeasurementFilter {
	return &MeasurementFilter{
		filter: filter,
	}
}

// Filter implements Filter interface
func (f *MeasurementFilter) Filter(key []byte) bool {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, _ := models.ParseKeyBytes(seriesKey)

	return f.filter.Filter(measurement)
}

// RawSerieFilter defines a filter restricted to a serie part of a key as raw bytes
type RawSerieFilter struct {
	filter Filter
}

// NewRawSerieFilter creates a new RawSerieFilter
func NewRawSerieFilter(filter Filter) *RawSerieFilter {
	return &RawSerieFilter{
		filter: filter,
	}
}

// Filter implements the Filter interface
func (f *RawSerieFilter) Filter(key []byte) bool {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	return f.filter.Filter(seriesKey)
}

// SerieFilter defines a filter restricted to the serie and field part of a key
type SerieFilter struct {
	measurementFilter Filter
	tagsFilter        Filter
	fieldFilter       Filter
}

// NewSerieFilter creates a new SerieFilter
func NewSerieFilter(measurementFilter Filter, tagsFilter Filter, fieldFilter Filter) *SerieFilter {
	return &SerieFilter{
		measurementFilter: measurementFilter,
		tagsFilter:        tagsFilter,
		fieldFilter:       fieldFilter,
	}
}

// SerieFilterConfig represents the toml configuration of a SerieFilter
type SerieFilterConfig struct {
	Measurement Filter
	Tag         Filter
	Field       Filter
}

// Filter implements Filter interface
func (f *SerieFilter) Filter(key []byte) bool {
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, _ := models.ParseKeyBytes(seriesKey)

	if f.fieldFilter == nil {
		return f.measurementFilter.Filter(measurement) && f.tagsFilter.Filter(seriesKey)
	}

	return f.measurementFilter.Filter(measurement) && f.tagsFilter.Filter(seriesKey) && f.fieldFilter.Filter(field)
}

// Sample implements Config interface
func (c *SerieFilterConfig) Sample() string {
	return `
		[measurement.strings]
			equal="cpu"
		[tag.where]
			cpu="cpu0"
		[field.pattern]
			pattern="^(idle|usage_idle)$"
	`
}

// Build implements Config interface
func (c *SerieFilterConfig) Build() (Filter, error) {
	if c.Tag == nil {
		return nil, fmt.Errorf("missing tag filter")
	}

	f := &SerieFilter{
		measurementFilter: c.Measurement,
		tagsFilter:        c.Tag,
		fieldFilter:       c.Field,
	}

	return f, nil
}

// WhereFilter defines a filter to restrict keys based on tag values
type WhereFilter struct {
	where map[string]*regexp.Regexp
}

// WhereFilterConfig represents toml configuration for WhereFilter
type WhereFilterConfig struct {
	Where map[string]string
}

// NewWhereFilter creates a new WhereFilter based on a map of tags key, value
func NewWhereFilter(where map[string]string) (*WhereFilter, error) {
	whereRe := make(map[string]*regexp.Regexp)

	for key, val := range where {
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		whereRe[key] = re
	}

	f := &WhereFilter{
		where: whereRe,
	}
	return f, nil
}

// Filter implements Filter interface
func (f *WhereFilter) Filter(key []byte) bool {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	_, tags := models.ParseKey(seriesKey)

	for _, tag := range tags {
		if val, ok := f.where[string(tag.Key)]; ok {
			if val.Match(tag.Value) {
				return true
			}
		}
	}

	return false
}

// Sample implements Config interface
func (c *WhereFilterConfig) Sample() string {
	return `
		cpu="^(cpu0|cpu1)$"
		host="my-host"
	`
}

// Unmarshal implements ManualConfig interface
func (c *WhereFilterConfig) Unmarshal(table *ast.Table) error {
	for key, keyVal := range table.Fields {
		subVal, ok := keyVal.(*ast.KeyValue)
		if !ok {
			return fmt.Errorf("%s: invalid configuration. Expected key-value pair", key)
		}

		stringVal, ok := subVal.Value.(*ast.String)
		if !ok {
			return fmt.Errorf("%s:%d invalid configuration. Expected string value", key, subVal.Line)
		}

		c.Where[subVal.Key] = stringVal.Value
	}
	return nil
}

// Build implements Config interface
func (c *WhereFilterConfig) Build() (Filter, error) {
	return NewWhereFilter(c.Where)
}

// FileFilter defines a filter based on a file content
type FileFilter struct {
	lines map[string]bool
}

// NewFileFilter creates a new FileFilter from a path
func NewFileFilter(path string) (*FileFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	lines := make(map[string]bool)

	for scanner.Scan() {
		lines[scanner.Text()] = true
	}

	return &FileFilter{lines: lines}, nil
}

// Filter implements Filter interface
func (f *FileFilter) Filter(key []byte) bool {
	k := string(key)
	_, ok := f.lines[k]
	return ok
}

// FileFilterConfig represents the toml configuration for a filter based on file content
type FileFilterConfig struct {
	Path string
}

// Sample implements Config interface
func (c *FileFilterConfig) Sample() string {
	return `
		path="file.log"
	`
}

// Build implements Config interface
func (c *FileFilterConfig) Build() (Filter, error) {
	return NewFileFilter(c.Path)
}

// StringFilterConfig represents the toml configuration for a filter based on strings functions
type StringFilterConfig struct {
	Contains    string
	ContainsAny string
	Equal       string
	EqualFold   string
	HasPrefix   string
	HasSuffix   string
}

// NewStringFilter creates a new StringFilter
func NewStringFilter(config *StringFilterConfig) (Filter, error) {
	type filterFn func(key []byte) bool

	var filterFns []filterFn

	count := 0
	if config.Contains != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return strings.Contains(string(key), config.Contains)
		})
		count++
	}
	if config.ContainsAny != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return strings.ContainsAny(string(key), config.ContainsAny)
		})
		count++
	}
	if config.Equal != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return string(key) == config.Equal
		})
		count++
	}
	if config.EqualFold != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return strings.EqualFold(string(key), config.EqualFold)
		})
		count++
	}
	if config.HasPrefix != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return strings.HasPrefix(string(key), config.HasPrefix)
		})
		count++
	}
	if config.HasSuffix != "" {
		filterFns = append(filterFns, func(key []byte) bool {
			return strings.HasSuffix(string(key), config.HasSuffix)
		})
		count++
	}

	fn := func(key []byte) bool {
		for _, f := range filterFns {
			if f(key) {
				return true
			}
		}
		return false
	}

	if count == 0 {
		return nil, fmt.Errorf("expected at least one parameter, got 0")
	}

	f := &FuncFilter{
		filterFn: fn,
	}
	return f, nil
}

// Sample implements Config interface
func (c *StringFilterConfig) Sample() string {
	return `
		hasprefix="linux."
		hassuffix=".gauge"
	`
}

// Build implements Config interface
func (c *StringFilterConfig) Build() (Filter, error) {
	return NewStringFilter(c)
}
