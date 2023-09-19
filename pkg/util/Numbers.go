package util

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

const (
	BYTE_BYTES = 1

	INT8_BYTES   = 1
	INT16_BYTES  = 2
	INT32_BYTES  = 4
	INT64_BYTES  = 8
	INT128_BYTES = 16

	FLOAT32_BYTES = 4
	FLOAT64_BYTES = 8

	INT64_SIZE = 64
)

var (
	INT16_ZERO   int16   = 0
	INT32_ZERO   int32   = 0
	INT64_ZERO   int64   = 0
	FLOAT64_ZERO float64 = 0
)

func ToIntExact(value int64) (int, error) {
	if value > math.MaxInt || value < math.MinInt {
		return 0, errors.New("integer overflow")
	}
	return int(value), nil
}

func ToInt32Exact(value int64) (int32, error) {
	if value > math.MaxInt32 || value < math.MinInt32 {
		return 0, errors.New("integer overflow")
	}
	return int32(value), nil
}

func Int32ExactU(value uint64) int32 {
	if value > math.MaxInt32 {
		panic("integer overflow")
	}
	return int32(value)
}

func Int32Exact(value int64) int32 {
	if value > math.MaxInt32 || value < math.MinInt32 {
		panic("integer overflow")
	}
	return int32(value)
}

func ToInt16Exact(value int64) (int16, error) {
	if value > math.MaxInt16 || value < math.MinInt16 {
		return 0, errors.New("integer overflow")
	}
	return int16(value), nil
}

func ToInt8Exact(value int64) (int8, error) {
	if value > math.MaxInt8 || value < math.MinInt8 {
		return 0, errors.New("integer overflow")
	}
	return int8(value), nil
}

func ToByteExact(value int64) (byte, error) {
	if value > math.MaxInt8 || value < math.MinInt8 {
		return 0, errors.New("integer overflow")
	}
	return byte(value), nil
}

func AddExactInt64(x, y int64) int64 {
	r := x + y
	// HD 2-12 Overflow iff both arguments have the opposite sign of the result
	if ((x ^ r) & (y ^ r)) < 0 {
		panic("long overflow")
	}
	return r
}

func ParseInt(str string, offset int32, length int32, base int32) int32 {
	// value :=  str.substring(offset, length)
	var nStr = []rune(str)
	value := nStr[offset : offset+length : length]
	// return Integer.parseInt(value, offset+base)
	i, _ := strconv.ParseInt(string(value), 10, 32)
	return int32(i)
}

func ParseInt64(str string, offset int32, length int32, base int32) int64 {
	// value :=  str.substring(offset, length)
	var nStr = []rune(str)
	value := nStr[offset : offset+length : length]
	// return Integer.parseInt(value, offset+base)
	i, _ := strconv.ParseInt(string(value), 10, 64)
	return i
}

func BytesToInt64(buf []byte, i int) int64 {
	return int64(binary.LittleEndian.Uint64(buf[i : i+INT64_BYTES]))
}

func ReverseNums[T basic.Object](array []T, fromIndex int32, toIndex int32) {
	i := fromIndex
	j := toIndex - 1
	for ; i < j; reverseOption(&i, &j) {
		tmp := array[i]
		array[i] = array[j]
		array[j] = tmp
	}
}

func reverseOption(a, b *int32) {
	(*a)++
	(*b)--
}
