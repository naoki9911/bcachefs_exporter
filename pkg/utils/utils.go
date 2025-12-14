package utils

import (
	"fmt"
	"math"
	"strconv"
)

func ParseSizeWithUnit(s []string) (int64, error) {
	if len(s) != 2 {
		return 0, fmt.Errorf("slice length is not 2: %d", len(s))
	}

	size, err := strconv.ParseFloat(s[0], 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse '%s': %v", s[0], err)
	}
	sizeUnit, err := stringByteUnitToInt(s[1])
	if err != nil {
		return 0, fmt.Errorf("failed to parase '%s': %v", s[1], err)
	}

	return int64(size * float64(sizeUnit)), nil
}

var byteUnits10 = []string{"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"}
var units10 = []string{"", "k", "M", "G", "T", "P", "E", "Z", "Y"}
var byteUnits2 = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"}

func stringByteUnitToInt(u string) (int64, error) {
	ret10 := int64(1)
	ret2 := int64(1)
	for i := range byteUnits10 {
		if u == byteUnits10[i] {
			return ret10, nil
		}
		if u == byteUnits2[i] {
			return ret2, nil
		}
		if u == units10[i] {
			return ret10, nil
		}

		ret10 *= 1000
		ret2 *= 1024
	}

	return 0, fmt.Errorf("unexpected unit: %s", u)
}

func ParseTimeWithUnit(s []string) (float64, error) {
	if len(s) != 2 {
		return 0, fmt.Errorf("slice length is not 2: %d", len(s))
	}

	time, err := strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse '%s': %v", s[0], err)
	}
	timeUnit, err := stringTimeUnitToInt(s[1])
	if err != nil {
		return 0, fmt.Errorf("failed to parase '%s': %v", s[1], err)
	}

	return float64(time) * timeUnit, nil
}

var timeUnits = map[string]float64{
	"ns":  math.Pow10(-9),
	"us":  math.Pow10(-6),
	"ms":  math.Pow10(-3),
	"s":   1,
	"m":   60,
	"h":   60 * 60,
	"d":   60 * 60 * 24,
	"w":   60 * 60 * 24 * 7,
	"y":   60 * 60 * 24 * 365.25,
	"eon": math.NaN(),
}

func stringTimeUnitToInt(u string) (float64, error) {
	ret, ok := timeUnits[u]
	if !ok {
		return 0, fmt.Errorf("invalid time unit '%s'", u)
	}
	return ret, nil
}
