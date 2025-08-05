package bcachefs

import (
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type FsUsage struct {
	FileSystem     string
	Path           string
	Capacity       int
	Used           int
	OnlineReserved int
	Replicas       []FsUsageReplica
	Compressions   []FsUsageCompression
	Btrees         []FsUsageBtree
	Rebalance      FsUsageRebalance
	Devices        []FsUsageDevice
}

type FsUsageReplica struct {
	DataType      string
	RequiredTotal string
	Durability    string
	Devices       string
	Size          int
}

type FsUsageCompression struct {
	CompressionType   string
	Comporessed       int64
	Uncompressed      int64
	AverageExtentSize int64
}

type FsUsageBtree struct {
	DataType string
	Size     int
}

type FsUsageRebalance struct {
	PendingSize int
}

type FsUsageDevice struct {
	Device string
	Label  string
	Datas  []FsUsageDeviceData
}

type FsUsageDeviceData struct {
	DataType      string
	Size          int
	Buckets       int
	HasFragmented bool
	Fragmented    int
}

func ParseFsUsage(path, results string) *FsUsage {
	fs := &FsUsage{
		Path: path,
	}

	var err error
	count := 0

	re := regexp.MustCompile(`\s+`)
	lines := strings.Split(results, "\n")
	idx := 0
	for idx < len(lines) {
		line := lines[idx]
		line = re.ReplaceAllString(line, " ")
		if line == "" {
			idx += 1
			continue
		}

		seps := strings.Split(line, " ")

		if strings.HasPrefix(line, "Filesystem:") {
			fs.FileSystem = seps[1]
			idx += 1
		} else if strings.HasPrefix(line, "Size:") {
			fs.Capacity, err = strconv.Atoi(seps[1])
			if err != nil {
				log.Fatalf("Failed to parse '%s': %v", line, err)
			}
			idx += 1
		} else if strings.HasPrefix(line, "Used:") {
			fs.Used, err = strconv.Atoi(seps[1])
			if err != nil {
				log.Fatalf("Failed to parse '%s': %v", line, err)
			}
			idx += 1
		} else if strings.HasPrefix(line, "Online reserved:") {
			fs.OnlineReserved, err = strconv.Atoi(seps[2])
			if err != nil {
				log.Fatalf("Failed to parse '%s': %v", line, err)
			}
			idx += 1
		} else if strings.HasPrefix(line, "Data type") {
			fs.Replicas, count = collectAccountings(lines[idx:])
			idx += count
		} else if strings.HasPrefix(line, "Compression:") {
			fs.Compressions, count = collectCompressions(lines[idx:])
			idx += count
		} else if strings.HasPrefix(line, "Btree usage:") {
			fs.Btrees, count = collectBtree(lines[idx:])
			idx += count
		} else if strings.HasPrefix(line, "Pending rebalance work:") {
			fs.Rebalance, count = collectRebalance(lines[idx:])
			idx += count
		} else {
			d, count := collectDevice(lines[idx:])
			fs.Devices = append(fs.Devices, d)
			idx += count
		}
	}
	return fs
}

// return the number of processed lines
func collectAccountings(lines []string) ([]FsUsageReplica, int) {
	re := regexp.MustCompile(`\s+`)
	line := re.ReplaceAllString(lines[0], " ")
	if line != "Data type Required/total Durability Devices" {
		log.Fatalf("unexpected format: %s", line)
	}

	res := []FsUsageReplica{}
	count := 1
	for {
		line := re.ReplaceAllString(lines[count], " ")
		if line == "" {
			break
		}
		count += 1

		seps := strings.Split(line, " ")
		//fmt.Println(seps)

		dataType := strings.ReplaceAll(seps[0], ":", "")
		requiredTotal := seps[1]
		if dataType == "reserved" {
			devices := seps[2]
			devices = devices[1 : len(devices)-1]
			size, err := strconv.Atoi(seps[3])
			if err != nil {
				log.Fatalf("failed to parse '%s': %v", line, err)
			}
			res = append(res, FsUsageReplica{
				DataType:      dataType,
				RequiredTotal: requiredTotal,
				Durability:    "",
				Devices:       devices,
				Size:          size,
			})
		} else {
			durability := seps[2]
			devicesIdx := 2
			devices := ""
			for {
				devicesIdx += 1

				str := seps[devicesIdx]
				if strings.HasSuffix(str, "]") {
					if devices == "" {
						devices = str[1 : len(str)-1]
					} else {
						devices += " " + str[:len(str)-1]
					}
					break
				}
				if devices == "" {
					devices = str[1:]
				} else {
					devices += " " + str
				}
			}
			//fmt.Println(devices)

			size, err := strconv.Atoi(seps[devicesIdx+1])
			if err != nil {
				log.Fatalf("failed to parse '%s': %v", line, err)
			}

			res = append(res, FsUsageReplica{
				DataType:      dataType,
				RequiredTotal: requiredTotal,
				Durability:    durability,
				Devices:       devices,
				Size:          size,
			})
		}
	}

	return res, count
}

// return the number of processed lines
func collectCompressions(lines []string) ([]FsUsageCompression, int) {
	re := regexp.MustCompile(`\s+`)
	line := re.ReplaceAllString(lines[1], " ")
	if line != "type compressed uncompressed average extent size" {
		log.Fatalf("unexpected format: %s", line)
	}

	compTypeCandidates := []string{"none", "lz4", "zstd", "gzip", "incompressible"}
	count := 2
	res := []FsUsageCompression{}
	for {
		line := re.ReplaceAllString(lines[count], " ")
		if line == "" {
			break
		}
		count += 1
		seps := strings.Split(line, " ")
		compType := ""
		if len(seps) < 3 {
			log.Fatalf("unexpected line: %s", line)
		} else if len(seps) == 3 {
			for _, candidate := range compTypeCandidates {
				if strings.HasPrefix(seps[0], candidate) {
					compType = candidate
					seps[0] = strings.TrimPrefix(seps[0], candidate)
					break
				}
			}
			if compType == "" {
				log.Fatalf("line with unknown compression type: %s", line)
			}

		} else if len(seps) == 4 {
			compType = seps[0]
			seps = seps[1:]
		}

		compressed, err := strconv.ParseInt(seps[0], 10, 64)
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
		uncompressed, err := strconv.ParseInt(seps[1], 10, 64)
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
		avgExtent, err := strconv.ParseInt(seps[2], 10, 64)
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}

		res = append(res, FsUsageCompression{
			CompressionType:   compType,
			Comporessed:       compressed,
			Uncompressed:      uncompressed,
			AverageExtentSize: avgExtent,
		})
	}

	return res, count
}

func collectBtree(lines []string) ([]FsUsageBtree, int) {
	re := regexp.MustCompile(`\s+`)
	res := []FsUsageBtree{}
	count := 1
	for {
		line := re.ReplaceAllString(lines[count], " ")
		if line == "" {
			break
		}
		count += 1
		seps := strings.Split(line, " ")
		dataType := strings.ReplaceAll(seps[0], ":", "")
		size, err := strconv.Atoi(seps[1])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
		res = append(res, FsUsageBtree{
			DataType: dataType,
			Size:     size,
		})
	}
	return res, count
}

func collectRebalance(lines []string) (FsUsageRebalance, int) {
	re := regexp.MustCompile(`\s+`)
	res := FsUsageRebalance{}
	count := 1
	var err error
	for {
		line := re.ReplaceAllString(lines[count], " ")
		if count == 2 {
			break
		}
		count += 1
		res.PendingSize, err = strconv.Atoi(line)
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}
	}
	return res, count
}

func collectDevice(lines []string) (FsUsageDevice, int) {
	re := regexp.MustCompile(`\s+`)
	line := re.ReplaceAllString(lines[0], " ")
	seps := strings.Split(line, ":")
	res := FsUsageDevice{
		Datas: []FsUsageDeviceData{},
	}

	if strings.HasPrefix(seps[0], "(no label)") {
		// '(no label) (device 0)'
		device := strings.ReplaceAll(seps[0], "(no label) ", "")
		res.Device = regexp.MustCompile(`\(|\)`).ReplaceAllString(device, "")
	} else {
		// 'hdd.hdd1 (device 0)'
		deviceSep := strings.SplitN(seps[0], " ", 2)
		res.Label = deviceSep[0]
		res.Device = regexp.MustCompile(`\(|\)`).ReplaceAllString(deviceSep[1], "")
	}

	count := 1
	line = lines[count]
	count += 1
	line = re.ReplaceAllString(line, " ")
	if line != " data buckets fragmented" {
		log.Fatalf("unexpected format: %s", line)
	}

	//fmt.Printf("label=%s device=%s\n", label, device)
	for {
		line = lines[count]
		count += 1
		if line == "" {
			break
		}
		line = re.ReplaceAllString(line, " ")
		seps = strings.Split(line, " ")

		dataType := strings.ReplaceAll(seps[1], ":", "")
		dataSize, err := strconv.Atoi(seps[2])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}

		buckets, err := strconv.Atoi(seps[3])
		if err != nil {
			log.Fatalf("unexpected line: %s: %v", line, err)
		}

		data := FsUsageDeviceData{
			DataType: dataType,
			Size:     dataSize,
			Buckets:  buckets,
		}
		if len(seps) > 4 {
			fragmented, err := strconv.Atoi(seps[4])
			if err != nil {
				log.Fatalf("unexpected line: %s: %v", line, err)
			}
			data.HasFragmented = true
			data.Fragmented = fragmented
		}
		res.Datas = append(res.Datas, data)
	}

	return res, count
}
