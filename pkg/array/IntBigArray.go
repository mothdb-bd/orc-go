package array

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type IntBigArray struct {
	initialValue int32
	array        [][]int32
	capacity     int64
	segments     int32
}

// var (
// 	intBigArrayiNSTANCE_SIZE   int32 = ClassLayout.parseClass(IntBigArray.class).instanceSize()
// 	intBigArraysIZE_OF_SEGMENT int64 = sizeOfIntArray(SEGMENT_SIZE)
// )

var (
	INT_INSTANCE_SIZE   int32 = SizeOf(&IntBigArray{})
	INT_SIZE_OF_SEGMENT int64 = int64(SEGMENT_SIZE) * int64(INT_INSTANCE_SIZE)
)

func NewIntBigArray() *IntBigArray {
	return NewByIntVal(0)
}
func NewByIntVal(initialValue int32) *IntBigArray {
	iy := new(IntBigArray)
	iy.initialValue = initialValue
	iy.array = make([][]int32, INITIAL_SEGMENTS)
	iy.allocateNewSegment()
	return iy
}

func (iy *IntBigArray) GetSegments() [][]int32 {
	return iy.array
}

func (iy *IntBigArray) SizeOf() int64 {
	return int64(INT_INSTANCE_SIZE) + int64(SizeOf(iy.array)) + (int64(iy.segments) * INT_SIZE_OF_SEGMENT)
}

func (iy *IntBigArray) Get(index int64) int32 {
	return iy.array[BigArrays_Segment(index)][BigArrays_Offset(index)]
}

func (iy *IntBigArray) Set(index int64, value int32) {
	iy.array[BigArrays_Segment(index)][BigArrays_Offset(index)] = value
}

func (iy *IntBigArray) Increment(index int64) {
	iy.array[BigArrays_Segment(index)][BigArrays_Offset(index)]++
}

func (iy *IntBigArray) Add(index int64, value int32) {
	iy.array[BigArrays_Segment(index)][BigArrays_Offset(index)] += value
}

func (iy *IntBigArray) EnsureCapacity(length int64) {
	if iy.capacity > length {
		return
	}
	iy.grow(length)
}

func (iy *IntBigArray) Fill(value int32) {
	for _, segment := range iy.array {
		if segment == nil {
			return
		}
		fillInts(segment, value)
	}
}

func (iy *IntBigArray) CopyTo(sourceIndex int64, destination *IntBigArray, destinationIndex int64, length int64) {
	for length > 0 {
		startSegment := BigArrays_Segment(sourceIndex)
		startOffset := BigArrays_Offset(sourceIndex)
		destinationStartSegment := BigArrays_Segment(destinationIndex)
		destinationStartOffset := BigArrays_Offset(destinationIndex)
		copyLength := maths.Min(int64(SEGMENT_SIZE-startOffset), int64(SEGMENT_SIZE-destinationStartOffset))

		var len = length
		if len > math.MaxInt32 {
			len = math.MaxInt32
		}
		copyLength = maths.Min(copyLength, len)

		CopyInts(iy.array[startSegment], startOffset, destination.array[destinationStartSegment], destinationStartOffset, copyLength)
		sourceIndex += copyLength
		destinationIndex += copyLength
		length -= copyLength
	}
}

func (iy *IntBigArray) grow(length int64) {
	requiredSegments := BigArrays_Segment(length) + 1
	if int32(len(iy.array)) < requiredSegments {
		iy.array = copyInt(iy.array, int64(requiredSegments))
	}
	for iy.segments < requiredSegments {
		iy.allocateNewSegment()
	}
}

func (iy *IntBigArray) allocateNewSegment() {
	newSegment := make([]int32, SEGMENT_SIZE)
	if iy.initialValue != 0 {
		fillInts(newSegment, iy.initialValue)
	}
	iy.array[iy.segments] = newSegment
	iy.capacity += int64(SEGMENT_SIZE)
	iy.segments++
}

// func (iy *IntBigArray) Sort(from int32, to int32, comparator *IntComparator) {
// 	// IntBigArrays.quickSort(array, from, to, comparator)
// }

func fillInts(array []int32, value int32) {
	for i := 0; i < len(array); i++ {
		array[i] = value
	}
}

func copyInt(src [][]int32, length int64) [][]int32 {
	var srcLen = len(src)
	var nLen = maths.Min(int64(srcLen), length)

	re := make([][]int32, nLen)
	for i := util.INT32_ZERO; int64(i) < nLen; i++ {
		re[i] = src[i]
	}
	return re
}

func CopyInts(src []int32, srcPos int32, dest []int32, destPos int32, length int64) {
	for i := util.INT32_ZERO; int64(i) < length; i++ {
		dest[destPos+int32(i)] = src[srcPos+int32(i)]
	}
}
