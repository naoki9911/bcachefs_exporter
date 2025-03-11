package sysfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSysFsCounter(t *testing.T) {
	assert := assert.New(t)
	input := `since mount:                   1
since filesystem creation:     551
`
	c, err := parseSysFsCounter(input)
	assert.Nil(err)
	assert.Equal(int64(1), c.Mount)
	assert.Equal(int64(551), c.Creation)
}

func TestParseSysFsCounterBytes(t *testing.T) {
	assert := assert.New(t)
	input := `since mount:                   1.32 TiB
since filesystem creation:     81.2 TiB
`
	c, err := parseSysFsCounter(input)
	assert.Nil(err)
	assert.Equal(int64(1451355348664), c.Mount)
	assert.Equal(int64(89280344175411), c.Creation)
}
