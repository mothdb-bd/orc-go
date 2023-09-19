package util

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
)

/**
 * 计算指定数组的长度
 */
func Lens[T basic.Object](a []T) int32 {
	return int32(len(a))
}

func LensInt64[T basic.Object](a []T) int64 {
	return int64(len(a))
}

/**
 *
 */
func BoolsLenInt32(b []bool) int32 {
	return int32(len(b))
}

/**
 *
 */
func BytesLenInt32(b []byte) int32 {
	return int32(len(b))
}

func Int16sLenInt32(b []int16) int32 {
	return int32(len(b))
}

/**
 *
 */
func Int32sLenInt32(b []int32) int32 {
	return int32(len(b))
}

func Int64sLenInt32(b []int64) int32 {
	return int32(len(b))
}

func BoolArraySame(array1 []bool, array2 []bool) bool {
	if array1 == nil || array2 == nil || len(array1) != len(array2) {
		panic("array1 and array2 cannot be null and should have same length")
	}
	for i := 0; i < len(array1); i++ {
		if array1[i] != array2[i] {
			return false
		}
	}
	return true
}

func CopyOfBools(original []bool, newLength int32) []bool {
	copy := make([]bool, newLength)
	ol := len(original)
	CopyBools(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyBoolsOfRange(original []bool, from int32, to int32) []bool {
	newLength := to - from
	if newLength < 0 {
		panic(strconv.Itoa(int(from)) + " > " + strconv.Itoa(int(to)))
	}
	newLength = maths.MinInt32(int32(len(original)), newLength)
	copy := make([]bool, newLength)
	CopyBools(original, from, copy, 0, newLength)
	return copy
}

func CopyBools(from []bool, srcPos int32, dest []bool, destPos int, length int32) {
	n := 0
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyOfBytes(original []byte, newLength int32) []byte {
	copy := make([]byte, newLength)
	ol := len(original)
	CopyBytes(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyBytes(from []byte, srcPos int32, dest []byte, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyOfInt16s(original []int16, newLength int32) []int16 {
	copy := make([]int16, newLength)
	ol := len(original)
	CopyInt16s(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyInt16sOfRange(original []int16, from int32, to int32) []int16 {
	newLength := to - from
	if newLength < 0 {
		panic(strconv.Itoa(int(from)) + " > " + strconv.Itoa(int(to)))
	}
	newLength = maths.MinInt32(int32(len(original)), newLength)
	copy := make([]int16, newLength)
	CopyInt16s(original, from, copy, 0, newLength)
	return copy
}

func CopyInt16s(from []int16, srcPos int32, dest []int16, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyInt32sOfRange(original []int32, from int32, to int32) []int32 {
	newLength := to - from
	if newLength < 0 {
		panic(strconv.Itoa(int(from)) + " > " + strconv.Itoa(int(to)))
	}
	newLength = maths.MinInt32(int32(len(original)), newLength)
	copy := make([]int32, newLength)
	CopyInt32s(original, from, copy, 0, newLength)
	return copy
}

func CopyOfInt32(original []int32, newLength int32) []int32 {
	copy := make([]int32, newLength)
	ol := len(original)
	CopyInt32s(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyInt32s(from []int32, srcPos int32, dest []int32, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyOfInt64s(original []int64, newLength int32) []int64 {
	copy := make([]int64, newLength)
	ol := len(original)
	CopyInt64s(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyInt64s(from []int64, srcPos int32, dest []int64, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyOfFloat64s(original []float64, newLength int32) []float64 {
	copy := make([]float64, newLength)
	ol := len(original)
	CopyFloat64s(original, 0, copy, 0,
		maths.MinInt32(int32(ol), newLength))
	return copy
}

func CopyFloat64s(from []float64, srcPos int32, dest []float64, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func CopyArrays[T basic.Object](from []T, srcPos int32, dest []T, destPos int32, length int32) {
	n := INT32_ZERO
	for i := srcPos; i < length && int(i) < len(from); i++ {
		dest[n+destPos] = from[i]
		n++
	}
}

func FillArrays[T basic.Object](array []T, index int32, length int32, value T) {

	l := maths.MinInt32(Lens(array), index+length)
	for i := index; i < l; i++ {
		array[i] = value
	}
}

func FillInt32s(array []int32, value int32) {
	for i := 0; i < len(array); i++ {
		array[i] = value
	}
}

func FillInt64s(array []int64, value int64) {
	for i := 0; i < len(array); i++ {
		array[i] = value
	}
}

func rangeCheck(arrayLength int32, fromIndex int32, toIndex int32) {
	if fromIndex > toIndex {
		panic(fmt.Sprintf("fromIndex(%d) > toIndex(%d)", fromIndex, toIndex))
	}
	if fromIndex < 0 {
		panic(fmt.Sprintf("fromIndex(%d) < 0", fromIndex))
	}
	if toIndex > arrayLength {
		panic(fmt.Sprintf("toIndex(%d) > arrayLength(%d)", toIndex, arrayLength))
	}
}

func FillInt32Range(a []int32, start int32, end int32, value int32) {
	rangeCheck(int32(len(a)), start, end)
	for i := start; i < end; i++ {
		a[i] = value
	}
}

func JoinNums[T basic.Numbers](elems []T, sep string) string {
	ns := make([]string, len(elems))
	for i, e := range elems {
		ns[i] = basic.NumString(e)
	}
	return strings.Join(ns, sep)
}
