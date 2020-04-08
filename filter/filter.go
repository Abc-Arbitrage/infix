package filter

import (
	"bytes"
	"io/ioutil"
	"regexp"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Filter defines an interface to filter and skip keys when applying rules
type Filter interface {
	Filter(key []byte) bool
}

// TagsFilter defines an interface to filter tags
type TagsFilter interface {
	Filter(tags models.Tags) bool
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

// SerieFilter defines a filter restricted to the serie part of a key
type SerieFilter struct {
	measurementFilter Filter
	tagsFilter        TagsFilter
}

// Filter implements Filter interface
func (f *SerieFilter) Filter(key []byte) bool {
	seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, tags := models.ParseKeyBytes(seriesKey)

	return f.measurementFilter.Filter(measurement) && f.tagsFilter.Filter(tags)
}

// SerieFieldFilter defines a filter based on (measurement, tags, field) components of a key
type SerieFieldFilter struct {
	measurementFilter Filter
	tagsFilter        TagsFilter
	fieldFilter       Filter
}

// Filter implements Filter interface
func (f *SerieFieldFilter) Filter(key []byte) bool {
	seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
	measurement, tags := models.ParseKeyBytes(seriesKey)

	return f.measurementFilter.Filter(measurement) &&
		f.tagsFilter.Filter(tags) &&
		f.fieldFilter.Filter(field)
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
