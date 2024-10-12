package bcachefs

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	input := `Filesystem: a9da1e6e-d4e5-4717-a520-408c8af4b084
Size:                 54282477161984
Used:                 17951715585024
Online reserved:          3275280384

Data type       Required/total  Durability    Devices
reserved:       1/2                [] 24068096
btree:          1/2             2             [sdc sdd]       112229089280
btree:          1/2             2             [sdc nvme1n1]        4718592
btree:          1/2             2             [sdc nvme0n1]         524288
btree:          1/2             2             [sdd nvme1n1]        3145728
btree:          1/2             2             [sdd nvme0n1]         524288
btree:          1/2             2             [nvme1n1 nvme0n1] 21587558400
btree:          1/2             2             [nvme1n1 nvme2n1] 21621637120
btree:          1/2             2             [nvme0n1 nvme2n1] 21596471296
user:           1/1             1             [sdc]          2408928206848
user:           1/1             1             [sdd]          2409040628224
user:           1/1             1             [nvme1n1]       179646521344
user:           1/1             1             [nvme0n1]       179691065344
user:           1/1             1             [nvme2n1]       179731595264
user:           1/1             1             [sde]           814648845824
user:           1/1             1             [sdf]           814639497728
user:           1/2             2             [sdc sdd]     10614365934592
user:           1/2             2             [sdc sde]        17294372864
user:           1/2             2             [sdc sdf]        11436670976
user:           1/2             2             [sdd sde]        11436785664
user:           1/2             2             [sdd sdf]        17295777792
user:           1/2             2             [nvme1n1 nvme0n1] 16560447488
user:           1/2             2             [nvme1n1 nvme2n1] 16561463296
user:           1/2             2             [nvme0n1 nvme2n1] 16560461824
user:           1/2             2             [sde sdf]        63458295808
cached:         1/1             1             [sdc]           978659688448
cached:         1/1             1             [sdd]           978454016000
cached:         1/1             1             [nvme1n1]        63288115200
cached:         1/1             1             [nvme0n1]        63229325312
cached:         1/1             1             [nvme2n1]        63226006016

Compression:
type              compressed    uncompressed     average extent size
zstd                2.85 TiB        8.14 TiB                 124 KiB
incompressible      9.13 TiB        9.13 TiB                90.1 KiB

Btree usage:
extents:         48907681792
inodes:          19482017792
dirents:         11814830080
xattrs:               524288
alloc:           11367612416
reflink:              524288
subvolumes:           524288
snapshots:            524288
lru:               421527552
freespace:          11010048
need_discard:        1048576
backpointers:    78548828160
bucket_gens:        44040192
snapshot_trees:       524288
deleted_inodes:       524288
logged_ops:           524288
rebalance_work:    655360000
accounting:       5786042368

Pending rebalance work:
1179740172288

(no label) (device 0):             sdc              rw
                                data         buckets    fragmented
  free:                4969107816448         9477821
  sb:                        3149824               7        520192
  journal:                4294967296            8192
  btree:                 56117166080          207817   52838793216
  user:                7730476697088        15043399  156596916736
  cached:               873978970112         1966636  157104685056
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:                    0               0
  unstriped:                       0               0
  capacity:           14000519643136        26703872

hdd.hdd2 (device 1):             sdd              rw
                                data         buckets    fragmented
  free:                4969263529984         9478118
  sb:                        3149824               7        520192
  journal:                4294967296            8192
  btree:                 56116379648          207815   52838531072
  user:                7730589876224        15043142  156348995584
  cached:               874068447232         1966598  156995284992
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:                    0               0
  unstriped:                       0               0
  capacity:           14000519643136        26703872

hdd.hdd3 (device 5):             sde              rw
                                data         buckets    fragmented
  free:               13119031083008        12511283
  sb:                        3149824               4       1044480
  journal:                8589934592            8192
  btree:                           0               0
  user:                 860743572992          832457   12150859264
  cached:                          0               0
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:                    0               0
  unstriped:                       0               0
  capacity:           14000519643136        13351936

hdd.hdd4 (device 6):             sdf              rw
                                data         buckets    fragmented
  free:               13119253381120        12511495
  sb:                        3149824               4       1044480
  journal:                8589934592            8192
  btree:                           0               0
  user:                 860734870016          832245   11937264128
  cached:                          0               0
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:                    0               0
  unstriped:                       0               0
  capacity:           14000519643136        13351936

ssd.ssd1 (device 3):         nvme0n1              rw
                                data         buckets    fragmented
  free:                 701282910208         1337591
  sb:                        3149824               7        520192
  journal:                4294967296            8192
  btree:                 21592539136           58538    9098231808
  user:                 196251520000          390249    8351347712
  cached:                59093206016          113160     235226112
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:              1048576               2
  unstriped:                       0               0
  capacity:            1000204664832         1907739

ssd.ssd2 (device 2):         nvme1n1              rw
                                data         buckets    fragmented
  free:                 701336387584         1337693
  sb:                        3149824               7        520192
  journal:                4294967296            8192
  btree:                 21608529920           58458    9040297984
  user:                 196207476736          390211    8375468032
  cached:                59114453504          113175     221840896
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:              1572864               3
  unstriped:                       0               0
  capacity:            1000204664832         1907739

ssd.ssd3 (device 4):         nvme2n1              rw
                                data         buckets    fragmented
  free:                 701245161472         1337519
  sb:                        3149824               7        520192
  journal:                4294967296            8192
  btree:                 21609054208           58552    9089056768
  user:                 196292557824          390225    8297726976
  cached:                59123746304          113243     248203264
  parity:                          0               0
  stripe:                          0               0
  need_gc_gens:                    0               0
  need_discard:               524288               1
  unstriped:                       0               0
  capacity:            1000204664832         1907739
`

	assert := assert.New(t)
	fsUsage := ParseFsUsage("/tank", input)

	assert.Equal("a9da1e6e-d4e5-4717-a520-408c8af4b084", fsUsage.FileSystem)
	assert.Equal(54282477161984, fsUsage.Capacity)
	assert.Equal(17951715585024, fsUsage.Used)
	assert.Equal(3275280384, fsUsage.OnlineReserved)
	assert.Equal(30, len(fsUsage.Replicas))

	replicas := [][]string{
		{"reserved", "1/2", "", "", "24068096"},
		{"btree", "1/2", "2", "sdc sdd", "112229089280"},
		{"btree", "1/2", "2", "sdc nvme1n1", "4718592"},
		{"btree", "1/2", "2", "sdc nvme0n1", "524288"},
		{"btree", "1/2", "2", "sdd nvme1n1", "3145728"},
		{"btree", "1/2", "2", "sdd nvme0n1", "524288"},
		{"btree", "1/2", "2", "nvme1n1 nvme0n1", "21587558400"},
		{"btree", "1/2", "2", "nvme1n1 nvme2n1", "21621637120"},
		{"btree", "1/2", "2", "nvme0n1 nvme2n1", "21596471296"},
		{"user", "1/1", "1", "sdc", "2408928206848"},
		{"user", "1/1", "1", "sdd", "2409040628224"},
		{"user", "1/1", "1", "nvme1n1", "179646521344"},
		{"user", "1/1", "1", "nvme0n1", "179691065344"},
		{"user", "1/1", "1", "nvme2n1", "179731595264"},
		{"user", "1/1", "1", "sde", "814648845824"},
		{"user", "1/1", "1", "sdf", "814639497728"},
		{"user", "1/2", "2", "sdc sdd", "10614365934592"},
		{"user", "1/2", "2", "sdc sde", "17294372864"},
		{"user", "1/2", "2", "sdc sdf", "11436670976"},
		{"user", "1/2", "2", "sdd sde", "11436785664"},
		{"user", "1/2", "2", "sdd sdf", "17295777792"},
		{"user", "1/2", "2", "nvme1n1 nvme0n1", "16560447488"},
		{"user", "1/2", "2", "nvme1n1 nvme2n1", "16561463296"},
		{"user", "1/2", "2", "nvme0n1 nvme2n1", "16560461824"},
		{"user", "1/2", "2", "sde sdf", "63458295808"},
		{"cached", "1/1", "1", "sdc", "978659688448"},
		{"cached", "1/1", "1", "sdd", "978454016000"},
		{"cached", "1/1", "1", "nvme1n1", "63288115200"},
		{"cached", "1/1", "1", "nvme0n1", "63229325312"},
		{"cached", "1/1", "1", "nvme2n1", "63226006016"},
	}
	for idx, replica := range replicas {
		assert.Equal(replica[0], fsUsage.Replicas[idx].DataType)
		assert.Equal(replica[1], fsUsage.Replicas[idx].RequiredTotal)
		assert.Equal(replica[2], fsUsage.Replicas[idx].Durability)
		assert.Equal(replica[3], fsUsage.Replicas[idx].Devices)
		assert.Equal(replica[4], strconv.Itoa(fsUsage.Replicas[idx].Size))
	}

	comps := [][]string{
		{"zstd", "3133608139161", "8950024650096", "126976"},
		{"incompressible", "10038541161594", "10038541161594", "92262"},
	}
	for idx, comp := range comps {
		assert.Equal(comp[0], fsUsage.Compressions[idx].CompressionType)
		assert.Equal(comp[1], strconv.Itoa(fsUsage.Compressions[idx].Comporessed))
		assert.Equal(comp[2], strconv.Itoa(fsUsage.Compressions[idx].Uncompressed))
		assert.Equal(comp[3], strconv.Itoa(fsUsage.Compressions[idx].AverageExtentSize))
	}

	btrees := [][]string{
		{"extents", "48907681792"},
		{"inodes", "19482017792"},
		{"dirents", "11814830080"},
		{"xattrs", "524288"},
		{"alloc", "11367612416"},
		{"reflink", "524288"},
		{"subvolumes", "524288"},
		{"snapshots", "524288"},
		{"lru", "421527552"},
		{"freespace", "11010048"},
		{"need_discard", "1048576"},
		{"backpointers", "78548828160"},
		{"bucket_gens", "44040192"},
		{"snapshot_trees", "524288"},
		{"deleted_inodes", "524288"},
		{"logged_ops", "524288"},
		{"rebalance_work", "655360000"},
		{"accounting", "5786042368"},
	}
	for idx, b := range btrees {
		assert.Equal(b[0], fsUsage.Btrees[idx].DataType)
		assert.Equal(b[1], strconv.Itoa(fsUsage.Btrees[idx].Size))
	}

	assert.Equal("1179740172288", strconv.Itoa(fsUsage.Rebalance.PendingSize))

	devices := [][]string{
		{"", "device 0"},
		{"hdd.hdd2", "device 1"},
		{"hdd.hdd3", "device 5"},
		{"hdd.hdd4", "device 6"},
		{"ssd.ssd1", "device 3"},
		{"ssd.ssd2", "device 2"},
		{"ssd.ssd3", "device 4"},
	}
	for idx, d := range devices {
		assert.Equal(d[0], fsUsage.Devices[idx].Label)
		assert.Equal(d[1], fsUsage.Devices[idx].Device)
	}

	devicesData := [][][]string{
		{
			{"free", "4969107816448", "9477821", ""},
			{"sb", "3149824", "7", "520192"},
			{"journal", "4294967296", "8192", ""},
			{"btree", "56117166080", "207817", "52838793216"},
			{"user", "7730476697088", "15043399", "156596916736"},
			{"cached", "873978970112", "1966636", "157104685056"},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "0", "0", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "14000519643136", "26703872", ""},
		},
		{
			{"free", "4969263529984", "9478118", ""},
			{"sb", "3149824", "7", "520192"},
			{"journal", "4294967296", "8192", ""},
			{"btree", "56116379648", "207815", "52838531072"},
			{"user", "7730589876224", "15043142", "156348995584"},
			{"cached", "874068447232", "1966598", "156995284992"},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "0", "0", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "14000519643136", "26703872", ""},
		},
		{
			{"free", "13119031083008", "12511283", ""},
			{"sb", "3149824", "4", "1044480"},
			{"journal", "8589934592", "8192", ""},
			{"btree", "0", "0", ""},
			{"user", "860743572992", "832457", "12150859264"},
			{"cached", "0", "0", ""},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "0", "0", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "14000519643136", "13351936", ""},
		},
		{
			{"free", "13119253381120", "12511495", ""},
			{"sb", "3149824", "4", "1044480"},
			{"journal", "8589934592", "8192", ""},
			{"btree", "0", "0", ""},
			{"user", "860734870016", "832245", "11937264128"},
			{"cached", "0", "0", ""},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "0", "0", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "14000519643136", "13351936", ""},
		},
		{
			{"free", "701282910208", "1337591", ""},
			{"sb", "3149824", "7", "520192"},
			{"journal", "4294967296", "8192", ""},
			{"btree", "21592539136", "58538", "9098231808"},
			{"user", "196251520000", "390249", "8351347712"},
			{"cached", "59093206016", "113160", "235226112"},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "1048576", "2", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "1000204664832", "1907739", ""},
		},
		{
			{"free", "701336387584", "1337693", ""},
			{"sb", "3149824", "7", "520192"},
			{"journal", "4294967296", "8192", ""},
			{"btree", "21608529920", "58458", "9040297984"},
			{"user", "196207476736", "390211", "8375468032"},
			{"cached", "59114453504", "113175", "221840896"},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "1572864", "3", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "1000204664832", "1907739", ""},
		},
		{
			{"free", "701245161472", "1337519", ""},
			{"sb", "3149824", "7", "520192"},
			{"journal", "4294967296", "8192", ""},
			{"btree", "21609054208", "58552", "9089056768"},
			{"user", "196292557824", "390225", "8297726976"},
			{"cached", "59123746304", "113243", "248203264"},
			{"parity", "0", "0", ""},
			{"stripe", "0", "0", ""},
			{"need_gc_gens", "0", "0", ""},
			{"need_discard", "524288", "1", ""},
			{"unstriped", "0", "0", ""},
			{"capacity", "1000204664832", "1907739", ""},
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
