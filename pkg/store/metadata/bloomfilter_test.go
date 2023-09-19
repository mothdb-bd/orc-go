package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var TEST_STRING = []byte("ORC_STRING")
var TEST_STRING_NOT_WRITTEN = []byte("ORC_STRING_not")
var TEST_INTEGER = 12345

func TestBloomFilterTest(t *testing.T) {
	bloomFilter := NewBloomFilter(1_000_000, 0.05)

	// String 测试MURMUR3 hash算法
	bloomFilter.Add(TEST_STRING)
	assert.True(t, bloomFilter.Test(TEST_STRING))
	assert.False(t, bloomFilter.Test(TEST_STRING_NOT_WRITTEN))

	// INTEGER 测试Thomas Wang's integer hash function
	bloomFilter.AddLong(int64(TEST_INTEGER))
	assert.True(t, bloomFilter.TestLong(int64(TEST_INTEGER)))
	assert.False(t, bloomFilter.TestLong(int64(TEST_INTEGER+1)))
}
