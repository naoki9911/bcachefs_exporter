package sysfs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSysFsBtreeWriteStats(t *testing.T) {
	assert := assert.New(t)
	input := `                   nr        size
initial:           364286    118 KiB
init_next_bset:    369063    24.9 KiB
cache_reclaim:     11667     306 B
journal_reclaim:   6045506   664 B
interior:          254768    998 B
`

	stat := parseSysFsBtreeWriteStats(input)
	expectedStats := [][]string{
		{"initial", "364286", "120832"},
		{"init_next_bset", "369063", "25497"},
		{"cache_reclaim", "11667", "306"},
		{"journal_reclaim", "6045506", "664"},
		{"interior", "254768", "998"},
	}

	for i, s := range expectedStats {
		assert.Equal(s[0], stat[i].Stat)
		assert.Equal(s[1], fmt.Sprintf("%d", stat[i].NR))
		assert.Equal(s[2], fmt.Sprintf("%d", stat[i].Size))
	}
}

func TestParseSysFsBtreeCacheSize(t *testing.T) {
	assert := assert.New(t)
	input := `19.1 GiB
	`

	assert.Equal(int64(20508468838), parseSysFsBtreeCacheSize(input))
}

func TestParseSysFsCompressionStats(t *testing.T) {
	assert := assert.New(t)
	input := `typetype          compressed    uncompressed     average extent size
lz4_old                  0 B             0 B                     0 B
gzip                     0 B             0 B                     0 B
lz4                      0 B             0 B                     0 B
zstd                2.86 TiB        8.15 TiB                 123 KiB
incompressible      10.5 TiB        10.5 TiB                82.2 KiB
`

	stat := parseSysFsCompressionStats(input)
	expectedStats := [][]string{
		{"lz4_old", "0", "0", "0"},
		{"gzip", "0", "0", "0"},
		{"lz4", "0", "0", "0"},
		{"zstd", "3144603255439", "8961019766374", "125952"},
		{"incompressible", "11544872091648", "11544872091648", "84172"},
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
