package array

import (
	"unsafe"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

const (
	// Initial number of segments to support in array.
	INITIAL_SEGMENTS int32 = 1024

	// The shift used to compute the segment associated with an index (equivalently, the logarithm of the segment size).
	SEGMENT_SHIFT int32 = 10

	// Size of a single segment of a BigArray.
	SEGMENT_SIZE int32 = 1 << SEGMENT_SHIFT

	// The mask used to compute the offset associated to an index.
	SEGMENT_MASK int32 = SEGMENT_SIZE - 1
)

type T interface{}

type BigArrays struct {
}

/**
 * Computes the segment associated with a given index.
 */
func BigArrays_Segment(index int64) int32 {
	return int32(uint64(index) >> SEGMENT_SHIFT)
}

/**
 *
 *Computes the offset associated with a given index.
 */
func BigArrays_Offset(index int64) int32 {
	return int32(uint64(index) & uint64(SEGMENT_MASK))
}

func CopyArrays(src []*T, srcPos int32, dest []*T, destPos int32, length int64) {
	for i := util.INT32_ZERO; int64(i) < length; i++ {
		dest[destPos+int32(i)] = src[srcPos+int32(i)]
	}
}

func CopyOf(src [][]*T, length int64) [][]*T {
	var srcLen = len(src)
	var nLen = maths.Min(int64(srcLen), length)

	re := make([][]*T, nLen)
	for i := util.INT32_ZERO; int64(i) < nLen; i++ {
		re[i] = src[i]
	}
	return re
}

func SizeOf(instance T) int32 {
	return int32(unsafe.Sizeof(instance))
}
