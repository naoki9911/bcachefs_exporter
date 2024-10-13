package sysfs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSysFsDevIoDone(t *testing.T) {
	assert := assert.New(t)
	input := `read:
sb          :      856064
journal     :           0
btree       :200642920448
user        :2338190483456
cached      :           0
parity      :           0
stripe      :           0
need_gc_gens:           0
need_discard:           0
unstriped   :           0
write:
sb          :     4088832
journal     :     7426048
btree       : 10873405440
user        :746460282880
cached      :           0
parity      :           0
stripe      :           0
need_gc_gens:           0
need_discard:           0
unstriped   :           0
`
	ioDone, err := parseSysFsDevIoDone(input)
	assert.Nil(err)
	expectedRead := [][]string{
		{"sb", "856064"},
		{"journal", "0"},
		{"btree", "200642920448"},
		{"user", "2338190483456"},
		{"cached", "0"},
		{"parity", "0"},
		{"stripe", "0"},
		{"need_gc_gens", "0"},
		{"need_discard", "0"},
		{"unstriped", "0"},
	}
	expectedWrite := [][]string{
		{"sb", "4088832"},
		{"journal", "7426048"},
		{"btree", "10873405440"},
		{"user", "746460282880"},
		{"cached", "0"},
		{"parity", "0"},
		{"stripe", "0"},
		{"need_gc_gens", "0"},
		{"need_discard", "0"},
		{"unstriped", "0"},
	}

	for _, e := range expectedRead {
		assert.Equal(fmt.Sprintf("%d", ioDone.Read[e[0]]), e[1])
	}
	for _, e := range expectedWrite {
		assert.Equal(fmt.Sprintf("%d", ioDone.Write[e[0]]), e[1])
	}
}

func TestParseSysFsDevIoErrors(t *testing.T) {
	assert := assert.New(t)
	input := `IO errors since filesystem creation
  read:    1
  write:   2
  checksum:3
IO errors since 7 y ago
  read:    0
  write:   0
  checksum:0
`
	ioErrors, err := parseSysFsDevIoErrors(input)
	assert.Nil(err)
	assert.Equal(int64(1), ioErrors.Read)
	assert.Equal(int64(2), ioErrors.Write)
	assert.Equal(int64(3), ioErrors.Checksum)
}
