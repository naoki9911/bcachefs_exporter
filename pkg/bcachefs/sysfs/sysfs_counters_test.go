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
	input := `since mount:                   7.69G
since filesystem creation:     176T
`
	c, err := parseSysFsCounter(input)
	assert.Nil(err)
	assert.Equal(int64(7690000000), c.Mount)
	assert.Equal(int64(176000000000000), c.Creation)
}
