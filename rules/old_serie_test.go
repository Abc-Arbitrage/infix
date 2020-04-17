package rules

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type captureWriter struct {
	captured []string
}

func (w *captureWriter) Write(p []byte) (int, error) {
	w.captured = append(w.captured, string(p))
	return len(p), nil
}

func TestOldSerie_ShouldDetectAndWriteOldSerie(t *testing.T) {
	ts := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

	w := &captureWriter{}
	rule, err := NewOldSerieRule(ts, false, w, "text")
	assert.NoError(t, err)

	var tags1 = map[string]string{
		"host": "my-host",
	}

	var tags2 = map[string]string{
		"host": "my-other-host",
	}

	var data = []struct {
		key    []byte
		values []tsm1.Value

		expectedOld bool
	}{
		{makeKey("cpu", tags1, "idle"), generateValuesBefore(ts, 10), true},
		{makeKey("disk", tags2, "usage"), generateValuesBefore(ts, 10), true},
		{makeKey("disk", tags1, "usage"), generateValuesAfter(ts, 10), false},
		{makeKey("mem", tags1, "available"), generateValuesBeforeAndAfter(ts, 10), false},
	}

	totalExpectedOld := 0

	rule.Start()

	for _, d := range data {
		if d.expectedOld {
			totalExpectedOld++
		}
		key, values, err := rule.Apply(d.key, d.values)
		assert.NoError(t, err)
		assert.Nil(t, key)
		assert.Nil(t, values)
	}

	rule.End()
	assert.Len(t, w.captured, totalExpectedOld)

	for _, d := range data {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(d.key)
		if d.expectedOld {
			assert.Contains(t, w.captured, fmt.Sprintf("%s\n", seriesKey))
		} else {
			assert.NotContains(t, w.captured, fmt.Sprintf("%s\n", seriesKey))
		}
	}
}

func TestOldSerie_ShouldDetectAndWriteOldSerieByField(t *testing.T) {
	ts := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

	w := &captureWriter{}
	rule, err := NewOldSerieRule(ts, true, w, "text")
	assert.NoError(t, err)

	var tags1 = map[string]string{
		"host": "my-host",
	}

	var tags2 = map[string]string{
		"host": "my-other-host",
	}

	var data = []struct {
		key    []byte
		values []tsm1.Value

		expectedOld bool
	}{
		{makeKey("cpu", tags1, "idle"), generateValuesBefore(ts, 10), true},
		{makeKey("cpu", tags1, "usage_idle"), generateValuesAfter(ts, 10), false},
		{makeKey("mem", tags2, "available"), generateValuesBeforeAndAfter(ts, 10), false},
	}

	totalExpectedOld := 0

	rule.Start()

	for _, d := range data {
		if d.expectedOld {
			totalExpectedOld++
		}
		key, values, err := rule.Apply(d.key, d.values)
		assert.NoError(t, err)
		assert.Nil(t, key)
		assert.Nil(t, values)
	}

	rule.End()
	assert.Len(t, w.captured, totalExpectedOld)

	for _, d := range data {
		if d.expectedOld {
			assert.Contains(t, w.captured, fmt.Sprintf("%s\n", d.key))
		} else {
			assert.NotContains(t, w.captured, fmt.Sprintf("%s\n", d.key))
		}
	}
}

func generateValuesBefore(ts time.Time, count int) (values []tsm1.Value) {
	for i := 0; i < count; i++ {
		before := ts.Add(time.Duration(-1) * time.Hour)
		values = append(values, tsm1.NewFloatValue(before.UnixNano(), rand.Float64()))
	}

	return values
}

func generateValuesAfter(ts time.Time, count int) (values []tsm1.Value) {
	for i := 0; i < count; i++ {
		before := ts.Add(time.Duration(1) * time.Hour)
		values = append(values, tsm1.NewFloatValue(before.UnixNano(), rand.Float64()))
	}

	return values
}

func generateValuesBeforeAndAfter(ts time.Time, count int) (values []tsm1.Value) {
	values = append(values, generateValuesBefore(ts, count)...)
	values = append(values, generateValuesAfter(ts, count)...)
	return values
}
