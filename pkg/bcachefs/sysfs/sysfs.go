package sysfs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/naoki9911/bcachefs_exporter/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var SYSFS_PATH_PREFIX = "/sys/fs/bcachefs"

type SysFsStat struct {
	BtreeWriteStat  []SysFsBtreeWriteStat
	BtreeCacheSize  int64
	CompressionStat []SysFsCompressionStat
	RebalanceStatus *SysFsRebalanceStatus
}

type SysFsBtreeWriteStat struct {
	Stat string
	NR   int64
	Size int64
}

type SysFsCompressionStat struct {
	CompressionType   string
	Comporessed       int64
	Uncompressed      int64
	AverageExtentSize int64
}

type SysFsRebalanceStatus struct {
	State      string
	DataType   string
	KeysMoved  int64
	KeysRaced  int64
	BytesSeen  int64
	BytesMoved int64
	BytesRaced int64
}

func ParseSysFs(uuid string) (*SysFsStat, error) {
	res := &SysFsStat{
		BtreeWriteStat:  nil,
		CompressionStat: nil,
		RebalanceStatus: nil,
	}

	var err error
	res.BtreeWriteStat, err = ParseSysFsBtreeWriteStats(uuid)
	if err != nil && os.IsExist(err) {
		return nil, fmt.Errorf("failed to parse 'btree_write_stats': %v", err)
	}

	res.BtreeCacheSize, err = ParseSysFsBtreeCacheSize(uuid)
	if err != nil && os.IsExist(err) {
		return nil, fmt.Errorf("failed to parse 'btree_cache_size': %v", err)
	}

	res.CompressionStat, err = ParseSysFsCompressionStats(uuid)
	if err != nil && os.IsExist(err) {
		return nil, fmt.Errorf("failed to parse 'compression_stats': %v", err)
	}

	res.RebalanceStatus, err = ParseSysFsRebalanceStatus(uuid)
	if err != nil && os.IsExist(err) {
		return nil, fmt.Errorf("failed to parse 'rebalance_status': %v", err)
	}

	return res, nil
}

func ParseSysFsBtreeWriteStats(uuid string) ([]SysFsBtreeWriteStat, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "btree_write_stats")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseSysFsBtreeWriteStats(string(data)), nil
}

func ParseSysFsBtreeCacheSize(uuid string) (int64, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "btree_cache_size")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	return parseSysFsBtreeCacheSize(string(data)), nil
}

func ParseSysFsCompressionStats(uuid string) ([]SysFsCompressionStat, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "compression_stats")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseSysFsCompressionStats(string(data)), nil
}

func ParseSysFsRebalanceStatus(uuid string) (*SysFsRebalanceStatus, error) {
	path := filepath.Join(SYSFS_PATH_PREFIX, uuid, "rebalance_status")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseSysFsRebalanceStatus(string(data)), nil
}

func parseSysFsBtreeWriteStats(s string) []SysFsBtreeWriteStat {
	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(s, "\n")
	line := re.ReplaceAllString(lines[0], " ")
	if line != " nr size" {
		log.Fatalf("unexpected format: %s", line)
	}

	res := []SysFsBtreeWriteStat{}
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		line = re.ReplaceAllString(line, " ")
		seps := strings.Split(line, " ")
		nr, err := strconv.ParseInt(seps[1], 10, 64)
		if err != nil {
			log.Fatalf("failed to parse 'nr': %v", err)
		}
		size, err := utils.ParseSizeWithUnit(seps[2:4])
		if err != nil {
			log.Fatalf("failed to parse 'size': %v", err)
		}
		res = append(res, SysFsBtreeWriteStat{
			Stat: strings.ReplaceAll(seps[0], ":", ""),
			NR:   int64(nr),
			Size: size,
		})
	}

	return res
}

func parseSysFsBtreeCacheSize(s string) int64 {
	seps := strings.Split(strings.Split(s, "\n")[0], " ")
	size, err := utils.ParseSizeWithUnit(seps[0:2])
	if err != nil {
		log.Fatalf("failed to parse 'size': %v", err)
	}
	return size
}

func parseSysFsCompressionStats(s string) []SysFsCompressionStat {
	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(s, "\n")
	line := re.ReplaceAllString(lines[0], " ")
	if line != "typetype compressed uncompressed average extent size" {
		log.Fatalf("unexpected format: %s", line)
	}

	res := []SysFsCompressionStat{}
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		line = re.ReplaceAllString(line, " ")
		seps := strings.Split(line, " ")

		compType := seps[0]
		compressed, err := utils.ParseSizeWithUnit(seps[1:3])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
		uncompressed, err := utils.ParseSizeWithUnit(seps[3:5])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
		avgExtent, err := utils.ParseSizeWithUnit(seps[5:7])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}

		res = append(res, SysFsCompressionStat{
			CompressionType:   compType,
			Comporessed:       compressed,
			Uncompressed:      uncompressed,
			AverageExtentSize: avgExtent,
		})
	}

	return res
}

func parseSysFsRebalanceStatus(s string) *SysFsRebalanceStatus {
	var err error
	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(s, "\n")
	line := re.ReplaceAllString(lines[0], " ")
	res := &SysFsRebalanceStatus{
		State: line,
	}
	switch line {
	case "waiting":
		return res
	case "scanning", "working":
		// ok
	default:
		log.Fatalf("unknown rebalance state '%s'", line)
	}

	// parse 'user' from ' rebalance_scan: data type==user pos=extents:1752400415:4096:U32_MAX'
	re2 := regexp.MustCompile(`.*data type==|\spos.*`)
	res.DataType = re2.ReplaceAllString(lines[1], "")
	for _, line := range lines[2:] {
		line = re.ReplaceAllString(line, " ")
		if line == "" || line == " " {
			continue
		}
		seps := strings.Split(line, ":")
		switch seps[0] {
		case " keys moved":
			res.KeysMoved, err = strconv.ParseInt(strings.ReplaceAll(seps[1], " ", ""), 10, 64)
		case " keys raced":
			res.KeysRaced, err = strconv.ParseInt(strings.ReplaceAll(seps[1], " ", ""), 10, 64)
		case " bytes seen":
			res.BytesSeen, err = utils.ParseSizeWithUnit(strings.Split(seps[1], " ")[1:3])
		case " bytes moved":
			res.BytesMoved, err = utils.ParseSizeWithUnit(strings.Split(seps[1], " ")[1:3])
		case " bytes raced":
			res.BytesRaced, err = utils.ParseSizeWithUnit(strings.Split(seps[1], " ")[1:3])
		default:
			log.Fatalf("unknown type '%s' in line '%s'", seps[0], line)
		}
		if err != nil {
			log.Fatalf("unexpected line: '%s': %v", line, err)
		}
	}
	return res
}
