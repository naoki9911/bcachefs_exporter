package sysfs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/naoki9911/bcachefs_exporter/pkg/utils"
)

type SysFsTimeStats map[string]SysFsTimeStat

type SysFsTimeStat struct {
	Count    int64
	Duration SysFsTimeStatItem
	Interval SysFsTimeStatItem
}

type SysFsTimeStatItem struct {
	Min    float64
	Max    float64
	Total  float64
	Mean   float64
	Stddev float64

	RecentMean   float64
	RecentStddev float64
}

func ParseSysFsTimeStats(uuid string) (SysFsTimeStats, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "time_stats")
	items, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	res := SysFsTimeStats{}
	for _, item := range items {
		p := filepath.Join(path, item.Name())
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", item.Name(), err)
		}
		s, err := parseSysFsTimeStat(string(data), false)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", item.Name(), err)
		}
		res[item.Name()] = *s
	}

	return res, nil
}

func parseSysFsTimeStat(s string, ignoreQuantiles bool) (*SysFsTimeStat, error) {
	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(s, "\n")
	stat := &SysFsTimeStat{}
	lineIdx := 0
	var err error
	for lineIdx < len(lines) {
		line := re.ReplaceAllString(lines[lineIdx], " ")
		seps := strings.Split(line, " ")
		switch seps[0] {
		case "count:":
			lineIdx += 2
			stat.Count, err = strconv.ParseInt(seps[1], 10, 64)
		case "duration":
			stat.Duration, err = parseSysFsTimeStatItem(lines[lineIdx+1 : lineIdx+6])
			lineIdx += 6
		case "time":
			stat.Interval, err = parseSysFsTimeStatItem(lines[lineIdx+1 : lineIdx+5])
			lineIdx += 5
		case "":
			lineIdx += 1
		case "quantiles":
			if ignoreQuantiles {
				lineIdx += 1
			} else {
				return nil, fmt.Errorf("unexpected line '%s'", line)
			}
		default:
			return nil, fmt.Errorf("unexpected line '%s'", line)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", line, err)
		}
	}

	return stat, nil
}

func parseSysFsTimeStatItem(lines []string) (SysFsTimeStatItem, error) {
	re := regexp.MustCompile(`\s+`)
	si := SysFsTimeStatItem{}
	for _, l := range lines {
		l = re.ReplaceAllString(l, " ")
		seps := strings.Split(l, " ")
		time, err := utils.ParseTimeWithUnit(seps[2:4])
		if err != nil {
			return si, fmt.Errorf("failed to parse '%s': %v", l, err)
		}
		switch seps[1] {
		case "min:":
			si.Min = time
		case "max:":
			si.Max = time
		case "total:":
			si.Total = time
		case "mean:", "stddev:":
			timeRecent, err := utils.ParseTimeWithUnit(seps[4:6])
			if err != nil {
				return si, fmt.Errorf("failed to parse '%s': %v", l, err)
			}
			if seps[1] == "mean:" {
				si.Mean = time
				si.RecentMean = timeRecent
			} else {
				si.Stddev = time
				si.RecentStddev = timeRecent
			}
		default:
			return si, fmt.Errorf("unexpected value '%s' in line '%s'", seps[1], l)
		}
	}

	return si, nil
}
