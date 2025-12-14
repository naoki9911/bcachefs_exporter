package sysfs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSysFsBtreeWriteStats(t *testing.T) {
	assert := assert.New(t)
	input := `                   nr        size
initial:           4243      129k
init_next_bset:    132       24.6k
cache_reclaim:     0         0
journal_reclaim:   28651     215
interior:          4078      285
`

	stat := parseSysFsBtreeWriteStats(input)
	expectedStats := [][]string{
		{"initial", "4243", "129000"},
		{"init_next_bset", "132", "24600"},
		{"cache_reclaim", "0", "0"},
		{"journal_reclaim", "28651", "215"},
		{"interior", "4078", "285"},
	}

	for i, s := range expectedStats {
		assert.Equal(s[0], stat[i].Stat)
		assert.Equal(s[1], fmt.Sprintf("%d", stat[i].NR))
		assert.Equal(s[2], fmt.Sprintf("%d", stat[i].Size))
	}
}

func TestParseSysFsBtreeCacheSize(t *testing.T) {
	assert := assert.New(t)
	input := `19.1G
`

	assert.Equal(int64(19100000000), parseSysFsBtreeCacheSize(input))
}

func TestParseSysFsCompressionStats(t *testing.T) {
	assert := assert.New(t)
	input := `typetype          compressed    uncompressed     average extent size
lz4_old                    0               0                       0
gzip                       0               0                       0
lz4                        0               0                       0
zstd                   3.32T           9.69T                    119k
incompressible         11.4T           11.4T                   94.7k
`

	stat := parseSysFsCompressionStats(input)
	expectedStats := [][]string{
		{"lz4_old", "0", "0", "0"},
		{"gzip", "0", "0", "0"},
		{"lz4", "0", "0", "0"},
		{"zstd", "3320000000000", "9690000000000", "119000"},
		{"incompressible", "11400000000000", "11400000000000", "94700"},
	}

	for i, s := range expectedStats {
		assert.Equal(s[0], stat[i].CompressionType)
		assert.Equal(s[1], fmt.Sprintf("%d", stat[i].Comporessed))
		assert.Equal(s[2], fmt.Sprintf("%d", stat[i].Uncompressed))
		assert.Equal(s[3], fmt.Sprintf("%d", stat[i].AverageExtentSize))
	}
}

func TestParseSysFsRebalanceStatus(t *testing.T) {
	assert := assert.New(t)
	input := `scanning
  rebalance_scan: data type==user pos=extents:1752400415:4096:U32_MAX
    keys moved:  74602530
    keys raced:  0
    bytes seen:  12.0 TiB
    bytes moved: 3.57 TiB
    bytes raced: 0 B

`

	stat := parseSysFsRebalanceStatus(input)
	assert.Equal("scanning", stat.State)
	assert.Equal("user", stat.DataType)
	assert.Equal(int64(74602530), stat.KeysMoved)
	assert.Equal(int64(0), stat.KeysRaced)
	assert.Equal(int64(13194139533312), stat.BytesSeen)
	assert.Equal(int64(3925256511160), stat.BytesMoved)
	assert.Equal(int64(0), stat.BytesRaced)
}

func TestParseSysFsRebalanceStatus2(t *testing.T) {
	assert := assert.New(t)
	input := `pending work:                  10.7 TiB

working
  rebalance_work: data type==user pos=extents:296331:3240:U32_MAX
    keys moved:                89
    keys raced:                0
    bytes seen:                3.76 MiB
    bytes moved:               3.76 MiB
    bytes raced:               0 B

  [<0>] bch2_rebalance_thread+0x65/0xb0 [bcachefs]
  [<0>] kthread+0xf9/0x240
  [<0>] ret_from_fork+0x31/0x50
  [<0>] ret_from_fork_asm+0x1a/0x30
`

	stat := parseSysFsRebalanceStatus(input)
	assert.Equal("working", stat.State)
	assert.Equal("user", stat.DataType)
	assert.Equal(int64(89), stat.KeysMoved)
	assert.Equal(int64(0), stat.KeysRaced)
	assert.Equal(int64(3942645), stat.BytesSeen)
	assert.Equal(int64(3942645), stat.BytesMoved)
	assert.Equal(int64(0), stat.BytesRaced)
}
