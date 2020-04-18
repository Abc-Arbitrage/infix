package filter

import (
	"testing"

	"github.com/naoina/toml"
	"github.com/stretchr/testify/assert"
)

func TestPatternFilter_ShouldCreate(t *testing.T) {
	filter, err := NewPatternFilter("^(cpu|disk)$")
	assert.NotNil(t, filter)
	assert.NoError(t, err)
}

func TestPatternFilter_ShouldCreateFail(t *testing.T) {
	filter, err := NewPatternFilter("^(cpu|disk$")
	assert.Nil(t, filter)
	assert.Error(t, err)
}

func TestPatternFilterConfig_ShouldBuildFromSample(t *testing.T) {
	config := &PatternFilterConfig{}

	table, err := toml.Parse([]byte(config.Sample()))
	assert.NoError(t, err)
	assert.NoError(t, UnmarshalConfig(table, config))

	filter, err := config.Build()
	assert.NoError(t, err)
	assert.NotNil(t, filter)
}

func TestPatternFilterConfig_ShouldBuildFail(t *testing.T) {
	config := &PatternFilterConfig{
		Pattern: "^(cpu|disk$",
	}
	filter, err := config.Build()
	assert.Nil(t, filter)
	assert.Error(t, err)
}

func TestPatternFilter_ShouldFilter(t *testing.T) {
	filter, _ := NewPatternFilter("^(cpu|disk)$")
	var data = []struct {
		key      string
		expected bool
	}{
		{"disk", true},
		{"cpu", true},
		{"diskio", false},
		{"mem", false},
	}

	for _, d := range data {
		assert.Equal(t, filter.Filter([]byte(d.key)), d.expected)
	}
}

func TestMeasurementFilter_ShouldFilter(t *testing.T) {
	innerFilterConfig := &StringFilterConfig{
		HasPrefix: "linux.",
	}
	innerFilter, err := innerFilterConfig.Build()
	assert.NotNil(t, innerFilter)
	assert.NoError(t, err)

	filter := NewMeasurementFilter(innerFilter)

	var data = []struct {
		key      string
		expected bool
	}{
		{"linux.cpu,cpu=cpu0,host=my-host", true},
		{"linux.disk,path=/", true},
		{"diskio,name=sda", false},
		{"mem", false},
	}

	for _, d := range data {
		assert.Equal(t, filter.Filter([]byte(d.key)), d.expected)
	}
}

func TestWhereFilterConfig_ShouldBuildFromSample(t *testing.T) {
	config := &WhereFilterConfig{
		Where: make(map[string]string),
	}

	table, err := toml.Parse([]byte(config.Sample()))
	assert.NoError(t, err)
	assert.NoError(t, UnmarshalConfig(table, config))

	filter, err := config.Build()
	assert.NotNil(t, filter)
	assert.NoError(t, err)
}

func TestWhereFilter_ShouldFilter(t *testing.T) {
	where := map[string]string{
		"cpu":  "^(cpu0|cpu1)",
		"host": "my-host",
	}
	filter, err := NewWhereFilter(where)
	assert.NotNil(t, filter)
	assert.NoError(t, err)

	var data = []struct {
		key      string
		expected bool
	}{
		{"cpu,cpu=cpu0", true},
		{"cpu,cpu=cpu1", true},
		{"cpu,cpu=cpu6", false},
		{"mem,host=my-host", true},
		{"disk,path=/", false},
	}

	for _, d := range data {
		assert.Equal(t, filter.Filter([]byte(d.key)), d.expected)
	}
}

func TestStringFilterConfig_ShouldBuild(t *testing.T) {
	config := &StringFilterConfig{
		HasPrefix: "linux.",
		HasSuffix: ".gauge",
	}

	filter, err := config.Build()

	assert.NotNil(t, filter)
	assert.NoError(t, err)
}

func TestStringFilterConfig_ShouldBuildFromSample(t *testing.T) {
	config := &StringFilterConfig{}

	table, err := toml.Parse([]byte(config.Sample()))
	assert.NoError(t, err)
	assert.NoError(t, UnmarshalConfig(table, config))

	filter, err := config.Build()
	assert.NotNil(t, filter)
	assert.NoError(t, err)
}

func TestStringFilter_ShouldBuildFail(t *testing.T) {
	config := &StringFilterConfig{}

	filter, err := config.Build()

	assert.Nil(t, filter)
	assert.Error(t, err)
}

func TestStringFilter_ShouldFilter(t *testing.T) {
	config := &StringFilterConfig{
		HasPrefix: "linux.",
		HasSuffix: ".gauge",
	}

	filter, err := config.Build()

	assert.NoError(t, err)

	var data = []struct {
		key      string
		expected bool
	}{
		{"linux.cpu", true},
		{"linux.disk", true},
		{"gc_bytes.gauge", true},
		{"mem", false},
		{"linux.mem.gauge", true},
		{"gauge.mem", false},
	}

	for _, d := range data {
		assert.Equal(t, filter.Filter([]byte(d.key)), d.expected)
	}
}

func TestSerieFilterConfig_ShouldBuildFromSample(t *testing.T) {
	config := &SerieFilterConfig{}

	table, err := toml.Parse([]byte(config.Sample()))
	assert.NoError(t, err)

	err = UnmarshalConfig(table, config)
	assert.NoError(t, err)

	filter, err := config.Build()
	assert.NoError(t, err)
	assert.NotNil(t, filter)
}
