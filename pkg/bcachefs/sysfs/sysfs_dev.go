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

type SysFsDev struct {
	Label          string
	Uuid           string
	BucketSize     int64
	NBuckets       int64
	FirstBucket    int64
	Durability     int64
	IoDone         *SysFsDevIoDone
	IoErrors       *SysFsDevIoErrors
	IoLatencyRead  *SysFsTimeStat
	IoLatencyWrite *SysFsTimeStat
}

type SysFsDevIoDone struct {
	Read  map[string]int64
	Write map[string]int64
}

type SysFsDevIoErrors struct {
	Read     int64
	Write    int64
	Checksum int64
}

func ParseSysFsDevs(uuid string) (map[string]SysFsDev, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid)
	items, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	res := map[string]SysFsDev{}
	for _, item := range items {
		name := item.Name()
		if !strings.HasPrefix(name, "dev-") {
			continue
		}

		p := filepath.Join(path, name)
		d, err := parseSysFsDev(p)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", p, err)
		}
		res[name] = *d
	}

	return res, nil
}

func parseSysFsDev(path string) (*SysFsDev, error) {
	res := SysFsDev{}

	p := filepath.Join(path, "label")
	labelBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.Label = strings.Split(string(labelBytes), "\n")[0]

	p = filepath.Join(path, "uuid")
	uuidBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.Uuid = strings.Split(string(uuidBytes), "\n")[0]

	res.BucketSize, err = parseReadInt(filepath.Join(path, "bucket_size"))
	if err != nil {
		return nil, fmt.Errorf("bucket_size: %v", err)
	}

	res.FirstBucket, err = parseReadInt(filepath.Join(path, "first_bucket"))
	if err != nil {
		return nil, fmt.Errorf("first_bucket: %v", err)
	}

	res.NBuckets, err = parseReadInt(filepath.Join(path, "nbuckets"))
	if err != nil {
		return nil, fmt.Errorf("nbuckets: %v", err)
	}

	res.Durability, err = parseReadInt(filepath.Join(path, "durability"))
	if err != nil {
		return nil, fmt.Errorf("durability: %v", err)
	}

	p = filepath.Join(path, "io_done")
	ioDoneBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.IoDone, err = parseSysFsDevIoDone(string(ioDoneBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s': %v", p, err)
	}

	p = filepath.Join(path, "io_errors")
	ioErrorsBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.IoErrors, err = parseSysFsDevIoErrors(string(ioErrorsBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s': %v", p, err)
	}

	p = filepath.Join(path, "io_latency_stats_read")
	ioLatencyReadBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.IoLatencyRead, err = parseSysFsTimeStat(string(ioLatencyReadBytes), true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s': %v", p, err)
	}

	p = filepath.Join(path, "io_latency_stats_write")
	ioLatencyWriteBytes, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res.IoLatencyWrite, err = parseSysFsTimeStat(string(ioLatencyWriteBytes), true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s': %v", p, err)
	}

	return &res, nil
}

func parseReadInt(p string) (int64, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return 0, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res, err := parseSizeWithUnitWithoutSpace(strings.Split(string(b), "\n")[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse '%s': %v", string(b), err)
	}

	return res, nil
}

func parseReadIntWithUnit(p string) (int64, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return 0, fmt.Errorf("failed to read '%s': %v", p, err)
	}
	res, err := utils.ParseSizeWithUnit(strings.Split(strings.Split(string(b), "\n")[0], " "))
	if err != nil {
		return 0, fmt.Errorf("failed to parse '%s': %v", string(b), err)
	}

	return res, nil
}

func parseSysFsDevIoDone(s string) (*SysFsDevIoDone, error) {
	res := &SysFsDevIoDone{
		Read:  map[string]int64{},
		Write: map[string]int64{},
	}

	mode := ""
	re := regexp.MustCompile(`\s+`)
	for _, l := range strings.Split(s, "\n") {
		if l == "" {
			continue
		}
		if l == "read:" || l == "write:" {
			mode = l
			continue
		}

		seps := strings.Split(l, ":")
		name := re.ReplaceAllString(seps[0], "")
		count, err := strconv.ParseInt(re.ReplaceAllString(seps[1], ""), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to read '%s': %v", l, err)
		}

		switch mode {
		case "read:":
			res.Read[name] = count
		case "write:":
			res.Write[name] = count
		default:
			return nil, fmt.Errorf("mode is not specified")
		}
	}

	return res, nil
}

func parseSysFsDevIoErrors(s string) (*SysFsDevIoErrors, error) {
	re := regexp.MustCompile(`\s+`)
	res := &SysFsDevIoErrors{}

	lines := strings.Split(s, "\n")
	if lines[0] != "IO errors since filesystem creation" {
		return nil, fmt.Errorf("unexpected line '%s'", lines[0])
	}

	for _, l := range lines[1:4] {
		seps := strings.Split(l, ":")
		count, err := strconv.ParseInt(re.ReplaceAllString(seps[1], ""), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s': %v", l, err)
		}
		item := re.ReplaceAllString(seps[0], "")
		switch item {
		case "read":
			res.Read = count
		case "write":
			res.Write = count
		case "checksum":
			res.Checksum = count
		default:
			return nil, fmt.Errorf("invalid item '%s' in '%s'", item, l)
		}
	}

	return res, nil
}
