package rules

import (
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
)

func TestDropMeasurement_ShouldBuildFromSample(t *testing.T) {
	assertBuildFromSample(t, &DropMeasurementRuleConfig{})
}

func TestDropMeasurement_ShouldApply(t *testing.T) {
	rule := NewDropMeasurement("cpu")

	var tags = map[string]string{
		"host": "my-host",
	}

	var data = []struct {
		key    []byte
		newKey []byte

		values []tsm1.Value
	}{
		{makeKey("cpu", tags, "idle"), nil, []tsm1.Value{tsm1.NewValue(0, 10.0)}},
		{makeKey("disk", tags, "usage"), makeKey("disk", tags, "usage"), []tsm1.Value{tsm1.NewValue(0, 20.0)}},
	}

	for _, d := range data {
		newKey, _, err := rule.Apply([]byte(d.key), d.values)
		assert.NoError(t, err)
		assert.Equal(t, newKey, d.newKey)
	}
}

func makeKey(measurement string, tags map[string]string, field string) []byte {
	t := models.NewTags(tags)
	seriesKey := models.MakeKey([]byte(measurement), t)
	return tsm1.SeriesFieldKeyBytes(string(seriesKey), field)
}
