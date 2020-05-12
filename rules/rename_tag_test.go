package rules

import (
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/oktal/infix/filter"

	"github.com/stretchr/testify/assert"
)

func TestRenameTag_ShouldBuildFromSample(t *testing.T) {
	assertBuildFromSample(t, &RenameTagRuleConfig{})
}

func TestRenameTag_ShouldBuildFail(t *testing.T) {
	data := []struct {
		name   string
		config string

		expectedError error
	}{
		{
			"missing measurement",
			`
			to="hostname"
			[tag.strings]
				equal="host"
			`,
			ErrMissingMeasurementFilter,
		},
		{
			"missing tag",
			`
			to="hostname"
			[measurement.strings]
				hasprefix="linux."
			`,
			ErrMissingTagFilter,
		},
		{
			"missing to",
			`
			[measurement.strings]
				hasprefix="linux."
			[tag.strings]
				equal="host"
			`,
			ErrMissingRenameTo,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assertBuildFromStringCallback(t, d.config, &RenameTagRuleConfig{}, func(r Rule, err error) {
				assert.Nil(t, r)
				assert.Equal(t, err, d.expectedError)
			})
		})
	}
}

func TestRenameTag_ShouldApply(t *testing.T) {
	measurementFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{HasPrefix: "linux."})
	assert.NoError(t, err)
	tagFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{Equal: "host"})
	assert.NoError(t, err)

	renameFn := func(key string) string {
		return "hostname"
	}

	rule := NewRenameTagRule(measurementFilter, tagFilter, renameFn)

	key := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	data := []struct {
		key    []byte
		values []tsm1.Value

		expectedKey []byte
	}{
		{
			key("linux.cpu,host=my-host,cpu=cpu0", "idle"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.cpu,hostname=my-host,cpu=cpu0", "idle"),
		},
		{
			key("linux.cpu,host=my-host,cpu=cpu0", "usage"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.cpu,hostname=my-host,cpu=cpu0", "usage"),
		},
		{
			key("linux.mem,host=my-host", "available"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 4.2), tsm1.NewFloatValue(1, 3.6)},
			key("linux.mem,hostname=my-host", "available"),
		},
		{
			key("linux.cpu,hostname=my-host,cpu=cpu0", "usage"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.9), tsm1.NewFloatValue(1, 3.7)},
			key("linux.cpu,hostname=my-host,cpu=cpu0", "usage"),
		},
		{
			key("linux.disk,path=/", "usage"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.disk,path=/", "usage"),
		},
		{
			key("diskio,host=my-host,name=sda1", "reads"),
			[]tsm1.Value{tsm1.NewIntegerValue(0, 8712), tsm1.NewIntegerValue(1, 9817)},
			key("diskio,host=my-host,name=sda1", "reads"),
		},
	}

	for _, d := range data {
		key, values, err := rule.Apply(d.key, d.values)

		assert.NoError(t, err)
		assert.Equal(t, values, d.values)
		assert.Equal(t, key, d.expectedKey)
	}
}
