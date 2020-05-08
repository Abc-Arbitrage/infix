package rules

import (
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/oktal/infix/filter"
	"github.com/stretchr/testify/assert"
)

func TestDropSerie_ShouldBuildFromSample(t *testing.T) {
	assertBuildFromSample(t, &DropSerieRuleConfig{})
}

func TestDropSerie_ShouldApplyAndDrop(t *testing.T) {
	measurementFilter := filter.NewIncludeFilter([]string{"cpu"})
	tagsFilter, err := filter.NewWhereFilter(map[string]string{
		"cpu": "^(cpu7|cpu8)$",
	})

	assert.NoError(t, err)

	filter := filter.NewSerieFilter(measurementFilter, tagsFilter, nil)
	rule := NewDropSerieRule(filter)

	makeKey := func(serie string, field string) []byte {
		return tsm1.SeriesFieldKeyBytes(serie, field)
	}

	values := []tsm1.Value{
		tsm1.NewFloatValue(0, 2.5),
		tsm1.NewFloatValue(1, 3.0),
	}

	data := []struct {
		key            []byte
		values         []tsm1.Value
		expectedKey    []byte
		expectedValues []tsm1.Value
	}{
		{makeKey("cpu,host=my-host,cpu=cpu0", "idle"), values, makeKey("cpu,host=my-host,cpu=cpu0", "idle"), values},
		{makeKey("cpu,host=my-host,cpu=cpu0", "active"), values, makeKey("cpu,host=my-host,cpu=cpu0", "active"), values},
		{makeKey("cpu,host=my-host,cpu=cpu7", "idle"), values, nil, nil},
		{makeKey("cpu,host=my-host,cpu=cpu7", "active"), values, nil, nil},
		{makeKey("cpu,host=my-host,cpu=cpu8", "idle"), values, nil, nil},
		{makeKey("cpu,host=my-host,cpu=cpu8", "idle"), values, nil, nil},
		{makeKey("mem,host=my-host", "used"), values, makeKey("mem,host=my-host", "used"), values},
	}

	for _, d := range data {
		newKey, newValues, err := rule.Apply(d.key, d.values)
		assert.NoError(t, err)
		assert.Equal(t, newKey, d.expectedKey)
		assert.Equal(t, newValues, d.expectedValues)
	}
}
