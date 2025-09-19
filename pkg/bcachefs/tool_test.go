package bcachefs

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	input := `Filesystem: a9da1e6e-d4e5-4717-a520-408c8af4b084
Size:                 89243210303488
Used:                 69551428518400
Online reserved:            13135872

Data by durability desired and amount degraded:
          undegraded
1x:    55338970490880
2x:    14201918887936
cached:1720875831296
reserved: 5181931520

Data type      Required/total  Durability    Devices
reserved:      1/2               [] 5181931520
btree:         1/2             2             [sdd sde]          1551368192
btree:         1/2             2             [nvme1n1 nvme0n1]211112427520
user:          1/1             1             [sdh]           9045660372480
user:          1/2             2             [sdg sdi]        176730659328
cached:        1/1             1             [nvme1n1]        578525953536

Compression:
type              compressed    uncompressed     average extent size
zstd           3629187407872  10558477742080                  123627
incompressible35909528035328  35909528035328                   79440

Btree usage:
extents:        195974660096
inodes:          57868288000

Pending rebalance work:
8216518656

hdd.hdd1 (device 0):             sdd              rw    79%
                                data         buckets    fragmented
  free:                2320309420032         4425639
  sb:                        3149824               7        520192
  bucket size:                524288
`

	assert := assert.New(t)
	fsUsage := ParseFsUsage("/tank", input)

	assert.Equal("a9da1e6e-d4e5-4717-a520-408c8af4b084", fsUsage.FileSystem)
	assert.Equal(89243210303488, fsUsage.Capacity)
	assert.Equal(69551428518400, fsUsage.Used)
	assert.Equal(13135872, fsUsage.OnlineReserved)
	assert.Equal(6, len(fsUsage.Replicas))

	replicas := [][]string{
		{"reserved", "1/2", "", "", "5181931520"},
		{"btree", "1/2", "2", "sdd sde", "1551368192"},
		{"btree", "1/2", "2", "nvme1n1 nvme0n1", "211112427520"},
		{"user", "1/1", "1", "sdh", "9045660372480"},
		{"user", "1/2", "2", "sdg sdi", "176730659328"},
		{"cached", "1/1", "1", "nvme1n1", "578525953536"},
	}
	for idx, replica := range replicas {
		assert.Equal(replica[0], fsUsage.Replicas[idx].DataType)
		assert.Equal(replica[1], fsUsage.Replicas[idx].RequiredTotal)
		assert.Equal(replica[2], fsUsage.Replicas[idx].Durability)
		assert.Equal(replica[3], fsUsage.Replicas[idx].Devices)
		assert.Equal(replica[4], strconv.Itoa(fsUsage.Replicas[idx].Size))
	}

	comps := [][]string{
		{"zstd", "3629187407872", "10558477742080", "123627"},
		{"incompressible", "35909528035328", "35909528035328", "79440"},
	}
	for idx, comp := range comps {
		assert.Equal(comp[0], fsUsage.Compressions[idx].CompressionType)
		assert.Equal(comp[1], fmt.Sprintf("%d", fsUsage.Compressions[idx].Comporessed))
		assert.Equal(comp[2], fmt.Sprintf("%d", fsUsage.Compressions[idx].Uncompressed))
		assert.Equal(comp[3], fmt.Sprintf("%d", fsUsage.Compressions[idx].AverageExtentSize))
	}

	btrees := [][]string{
		{"extents", "195974660096"},
		{"inodes", "57868288000"},
	}
	for idx, b := range btrees {
		assert.Equal(b[0], fsUsage.Btrees[idx].DataType)
		assert.Equal(b[1], strconv.Itoa(fsUsage.Btrees[idx].Size))
	}

	assert.Equal("8216518656", strconv.Itoa(fsUsage.Rebalance.PendingSize))

	devices := [][]string{
		{"hdd.hdd1", "device 0"},
	}
	for idx, d := range devices {
		assert.Equal(d[0], fsUsage.Devices[idx].Label)
		assert.Equal(d[1], fsUsage.Devices[idx].Device)
	}

	devicesData := [][][]string{
		{
			{"free", "2320309420032", "4425639", ""},
			{"sb", "3149824", "7", "520192"},
		},
	}

	for devIdx := range devicesData {
		for dataIdx := range devicesData[devIdx] {
			d := devicesData[devIdx][dataIdx]
			assert.Equal(d[0], fsUsage.Devices[devIdx].Datas[dataIdx].DataType)
			assert.Equal(d[1], strconv.Itoa(fsUsage.Devices[devIdx].Datas[dataIdx].Size))
			assert.Equal(d[2], strconv.Itoa(fsUsage.Devices[devIdx].Datas[dataIdx].Buckets))
			assert.Equal(d[3] != "", fsUsage.Devices[devIdx].Datas[dataIdx].HasFragmented)
			if d[3] != "" {
				assert.Equal(d[3], strconv.Itoa(fsUsage.Devices[devIdx].Datas[dataIdx].Fragmented))
			}
		}
	}
}
