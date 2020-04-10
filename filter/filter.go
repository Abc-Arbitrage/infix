package filter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"

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
		 [[rules.rename-measurement]]
			  [rules.rename-measurement.from.pattern]
			        pattern="^(cpu|disk)$"
	`
}

// Build implements Config interface
func (c *PatternFilterConfig) Build() (Filter, error) {
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

// PassFilter is a Filter that always pass
type PassFilter struct {
}

// Filter implements Filter interface
func (f *PassFilter) Filter(key []byte) bool {
	return false
}

// RawFilter is a filter based on raw bytes
type RawFilter struct {
	content []byte
}

// Filter implements Filter interface
func (f *RawFilter) Filter(key []byte) bool {
	return bytes.Contains(f.content, key)
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
	return ``
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

// FileFilterConfiguration represents the toml configuration for a filter based on file content
type FileFilterConfiguration struct {
	Path string
}

// Sample implements Config interface
func (c *FileFilterConfiguration) Sample() string {
	return `
	[[rules.drop-serie]]
		 [rules.drop-serie.dropFilter.file]
		 	path="file.log"
	`
}

// Build implements Config interface
func (c *FileFilterConfiguration) Build() (Filter, error) {
	content, err := ioutil.ReadFile(c.Path)
	if err != nil {
		return nil, err
	}

	f := &RawFilter{
		content: content,
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
	[filters.serie]
		[filters.serie.tag.where]
			cpu="^(cpu0|cpu1)"
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
	where := make(map[string]*regexp.Regexp)

	for key, val := range c.Where {
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		where[key] = re
	}

	f := &WhereFilter{
		where: where,
	}
	return f, nil
}
