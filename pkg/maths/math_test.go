package maths

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsignedRightShift(t *testing.T) {
	t1 := 12345
	t2 := -12345
	assert.Equal(t, int64(3086), UnsignedRightShift(int64(t1), 2))
	assert.Equal(t, int64(4611686018427384817), UnsignedRightShift(int64(t2), 2))

	assert.Equal(t, int32(3086), UnsignedRightShiftInt32(int32(t1), 2))
	assert.Equal(t, int32(1073738737), UnsignedRightShiftInt32(int32(t2), 2))
}

func TestMaxIntNum(t *testing.T) {
	test := []struct {
		a, b   int
		expect int
	}{
		{
			a:      1,
			b:      2,
			expect: 2,
		},
		{
			a:      math.MaxInt,
			b:      math.MaxInt - 1,
			expect: math.MaxInt,
		},
	}
	for idx, tt := range test {
		t.Run("maxIntNum"+strconv.Itoa(idx), func(t *testing.T) {
			actual := MaxIntNum(tt.a, tt.b)
			if tt.expect != actual {
				t.Errorf("expect: %d,but actual: %d\n", tt.expect, actual)
			}
		})
	}
}

func TestMax(t *testing.T) {
	test := []struct {
		a, b   int64
		expect int64
	}{
		{
			a:      1,
			b:      2,
			expect: 2,
		},
		{
			a:      math.MaxInt64,
			b:      math.MaxInt64 - 1,
			expect: math.MaxInt64,
		},
	}
	for idx, tt := range test {
		t.Run("maxIntNum"+strconv.Itoa(idx), func(t *testing.T) {
			actual := Max(tt.a, tt.b)
			if tt.expect != actual {
				t.Errorf("expect: %d,but actual: %d\n", tt.expect, actual)
			}
		})
	}
}
