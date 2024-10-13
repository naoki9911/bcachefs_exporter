package sysfs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSysFsTimeStat(t *testing.T) {
	assert := assert.New(t)
	input := `count:     484251
                       since mount        recent
duration of events
  min:                       88 us
  max:                        2 m
  total:                     47 h
  mean:                     353 ms         11 ms
  stddev:                     2 s           5 ms
time between events
  min:                       10 ns
  max:                        9 m
  mean:                     475 ms       1586 ms
  stddev:                  1909 ms       1679 us
`
	stat, err := parseSysFsTimeStat(input, false)
	assert.Nil(err)
	assert.Equal(float64(88)*math.Pow10(-6), stat.Duration.Min)
	assert.Equal(float64(2*60), stat.Duration.Max)
	assert.Equal(float64(47*3600), stat.Duration.Total)
	assert.Equal(float64(353)*math.Pow10(-3), stat.Duration.Mean)
	assert.Equal(float64(11)*math.Pow10(-3), stat.Duration.RecentMean)
	assert.Equal(float64(2), stat.Duration.Stddev)
	assert.Equal(float64(5)*math.Pow10(-3), stat.Duration.RecentStddev)

	assert.Equal(float64(10)*math.Pow10(-9), stat.Interval.Min)
	assert.Equal(float64(9*60), stat.Interval.Max)
	assert.Equal(float64(475)*math.Pow10(-3), stat.Interval.Mean)
	assert.Equal(float64(1586)*math.Pow10(-3), stat.Interval.RecentMean)
	assert.Equal(float64(1909)*math.Pow10(-3), stat.Interval.Stddev)
	assert.Equal(float64(1679)*math.Pow10(-6), stat.Interval.RecentStddev)
}
