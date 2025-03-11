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

type SysFsCounter struct {
	Mount    int64 // since mount
	Creation int64 // since file system creation
}

func ParseSysFsCounters(uuid string) (map[string]SysFsCounter, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "counters")
	items, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	res := map[string]SysFsCounter{}
	for _, item := range items {
		p := filepath.Join(path, item.Name())
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", item.Name(), err)
		}
		s, err := parseSysFsCounter(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", item.Name(), err)
		}
		res[item.Name()] = *s
	}

	return res, nil
}

func parseSysFsCounter(s string) (*SysFsCounter, error) {
	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(s, "\n")
	cnt := &SysFsCounter{}
	var err error
	for _, l := range lines {
		if l == "" {
			continue
		}

		l := re.ReplaceAllString(l, " ")
		seps := strings.Split(l, " ")
		if len(seps) < 3 {
			return nil, fmt.Errorf("unexpected line '%s'", l)
		}
		var c int64
		switch seps[1] {
		case "mount:":
			if len(seps) == 4 {
				c, err = utils.ParseSizeWithUnit(seps[2:])
			} else {
				c, err = strconv.ParseInt(seps[2], 10, 64)
			}
			cnt.Mount = c
		case "filesystem":
			if len(seps) < 4 {
				return nil, fmt.Errorf("unexpected line '%s'", l)
			}
			if len(seps) == 5 {
				c, err = utils.ParseSizeWithUnit(seps[3:])
			} else {
				c, err = strconv.ParseInt(seps[3], 10, 64)
			}
			cnt.Creation = c
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", l, err)
		}
	}

	return cnt, nil
}
