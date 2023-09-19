package hashcode

import (
	"fmt"
	"hash/crc32"
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
)

func ObjectHashCode(value basic.Object) int32 {

	switch t := value.(type) {
	case float64:
		ft := value.(float64)
		return Float64HashCode(ft)
	case float32:
		ft := value.(float32)
		return Float32HashCode(ft)
	case int:
		it := value.(int)
		return IntHashCode(it)
	case uint:
		it := value.(uint)
		return UIntHashCode(it)
	case int8:
		it := value.(int8)
		return Int8HashCode(it)
	case uint8:
		it := value.(uint8)
		return UInt8HashCode(it)
	case int16:
		it := value.(int16)
		return Int16HashCode(it)
	case uint16:
		it := value.(uint16)
		return UInt16HashCode(it)
	case int32:
		it := value.(int32)
		return Int32HashCode(it)
	case uint32:
		it := value.(uint32)
		return UInt32HashCode(it)
	case int64:
		it := value.(int64)
		return Int64HashCode(it)
	case uint64:
		it := value.(uint64)
		return UInt64HashCode(it)
	case string:
		s := value.(string)
		return StringHashCode(s)
	case []byte:
		b := value.([]byte)
		return BytesHashCode(b)
	case uintptr:
		s, flag := value.(*slice.Slice)
		if flag {
			return s.HashCode()
		} else {
			return int32(reflect.ValueOf(value).UnsafeAddr())
		}
	case nil:
		return 0
	default:
		panic(fmt.Sprintf("Noknow type %t", t))
	}
}

// 比较两个指针是否相同
func PointerEqual(a, b basic.Object) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

func IntHashCode(i int) int32 {
	return int32(i)
}

func UIntHashCode(i uint) int32 {
	return int32(i)
}

func Int8HashCode(i int8) int32 {
	return int32(i)
}

func UInt8HashCode(i uint8) int32 {
	return int32(i)
}

func Int16HashCode(i int16) int32 {
	return int32(i)
}

func UInt16HashCode(i uint16) int32 {
	return int32(i)
}

func Int32HashCode(i int32) int32 {
	return i
}

func UInt32HashCode(i uint32) int32 {
	return int32(i)
}

func Int64HashCode(i int64) int32 {
	return (int32)(uint64(i) ^ (uint64(i) >> 32))
}

func UInt64HashCode(i uint64) int32 {
	return Int64HashCode(int64(i))
}

func Float32HashCode(f float32) int32 {
	return UInt32HashCode(math.Float32bits(f))
}

func Float64HashCode(f float64) int32 {
	return UInt64HashCode(math.Float64bits(f))
}

func StringHashCode(s string) int32 {
	return BytesHashCode([]byte(s))
}

func BytesHashCode(b []byte) int32 {
	v := int32(crc32.ChecksumIEEE(b))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == Min
	return 0
}
