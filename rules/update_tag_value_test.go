package rules

import (
	"testing"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
)

func TestUpdateTagValue_ShouldBuildFromSample(t *testing.T) {
	assertBuildFromSample(t, &UpdateTagValueRuleConfig{})
}

func TestUpdateTagValue_ShouldBuildFail(t *testing.T) {
	data := []struct {
		name   string
		config string

		expectedError error
	}{
		{
			"missing measurement",
			`
			to="aws"
			`,
			ErrMissingMeasurementFilter,
		},
		{
			"missing measurement",
			`
			to="aws"
			[key.strings]
			    equal="region"
			[value.strings]
			    equal="amazon"
			`,
			ErrMissingMeasurementFilter,
		},
		{
			"missing tag key",
			`
			to="aws"
			[measurement.strings]
			    hasprefix="linux."
			[value.strings]
			    equal="amazon"
			`,
			ErrMissingTagKeyFilter,
		},
		{
			"missing tag value",
			`
			to="aws"
			[measurement.strings]
			    hasprefix="linux."
			[key.strings]
			    equal="region"
			`,
			ErrMissingTagValueFilter,
		},
		{
			"missing to",
			`
			[measurement.strings]
			    hasprefix="linux."
			[key.strings]
			    equal="region"
			`,
			ErrMissingRenameTo,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assertBuildFromStringCallback(t, d.config, &UpdateTagValueRuleConfig{}, func(r Rule, err error) {
				assert.Nil(t, r)
				assert.Equal(t, err, d.expectedError)
			})
		})
	}
}

func TestUpdateTagValue_ShouldApply(t *testing.T) {
	measurementFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{HasPrefix: "linux."})
	assert.NoError(t, err)
	keyFilter, err := filter.NewStringFilter(&filter.StringFilterConfig{Equal: "region"})
	assert.NoError(t, err)
	valueFilter, err := filter.NewPatternFilter("amazon-(.+)")

	renameFn := RenameFnFromFilter(valueFilter, "aws-$1")

	rule := NewUpdateTagValueRule(measurementFilter, keyFilter, valueFilter, renameFn)

	key := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	data := []struct {
		key    []byte
		values []tsm1.Value

		expectedKey []byte
	}{
		{
			key("linux.cpu,host=my-host,region=amazon-eu-west", "idle"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.cpu,host=my-host,region=aws-eu-west", "idle"),
		},
		{
			key("linux.cpu,host=my-host,dc=eu-west", "idle"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.cpu,host=my-host,dc=eu-west", "idle"),
		},
		{
			key("linux.cpu,host=my-host,region=eu-west", "idle"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("linux.cpu,host=my-host,region=eu-west", "idle"),
		},
		{
			key("requests,host=my-host,region=amazon-eu-west", "idle"),
			[]tsm1.Value{tsm1.NewFloatValue(0, 3.5), tsm1.NewFloatValue(1, 3.6)},
			key("requests,host=my-host,region=amazon-eu-west", "idle"),
		},
	}

	for _, d := range data {
		key, values, err := rule.Apply(d.key, d.values)

		assert.NoError(t, err)
		assert.Equal(t, string(key), string(d.expectedKey))
		assert.Equal(t, values, d.values)
	}
}
