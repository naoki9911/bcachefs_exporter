package sysfs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
		switch seps[1] {
		case "mount:":
			cnt.Mount, err = parseSizeWithUnitWithoutSpace(seps[2])
		case "filesystem":
			cnt.Creation, err = parseSizeWithUnitWithoutSpace(seps[3])
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", l, err)
		}
	}

	return cnt, nil
}
