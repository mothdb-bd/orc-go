package block

import (
	"fmt"
	"math"
	"strconv"

	clone "github.com/huandu/go-clone"
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	BLOCK_RESET_SKEW float64 = 1.25
	DEFAULT_CAPACITY int32   = 64
	MAX_ARRAY_SIZE   int32   = math.MaxInt32 - 8

	// Byte.BYTES + Long.BYTES
	BYTE_LONG_BYTES int32 = util.INT64_BYTES + util.BYTE_BYTES

	// Byte.BYTES + Byte.BYTES
	BYTE_BYTE_BYTES int32 = util.BYTE_BYTES + util.BYTE_BYTES

	// Byte.BYTES + Short.BYTES
	BYTE_SHORT_BYTES int32 = util.BYTE_BYTES + util.INT16_BYTES

	// Integer.BYTES+Byte.BYTES
	INT32_BYTE_BYTES int32 = util.INT32_BYTES + util.BYTE_BYTES
)

func checkArrayRange(array []int32, offset int32, length int32) bool {
	if array == nil {
		return false
	}
	if offset < 0 || length < 0 || int(offset+length) > len(array) {
		// panic(NewIndexOutOfBoundsException(format("Invalid offset %s and length %s in array with %s elements", offset, length, array.length)))
		// return false
		panic("Invalid offset")
	}
	return true
}

func checkValidRegion(positionCount int32, positionOffset int32, length int32) bool {
	if positionOffset < 0 || length < 0 || (positionOffset+length) > positionCount {
		// panic(NewIndexOutOfBoundsException(format("Invalid position %s and length %s in block with %s positions", positionOffset, length, positionCount)))
		// return false
		panic("Invalid positions")
	}
	return true
}

func checkValidPositions(positions []bool, positionCount int32) bool {
	if len(positions) != int(positionCount) {
		// panic(NewIllegalArgumentException(format("Invalid positions array size %d, actual position count is %d", positions.length, positionCount)))
		// return false
		panic("Invalid positions")
	}
	return true
}

func checkValidPosition(position int32, positionCount int32) bool {
	if position < 0 || position >= positionCount {
		// panic(NewIllegalArgumentException(format("Invalid position %s in block with %s positions", position, positionCount)))
		// return false
		panic(fmt.Sprintf("Invalid position %d", position))
	}
	return true
}

func calculateNewArraySize(currentSize int32) int32 {
	newSize := currentSize + (currentSize >> 1)
	if newSize < DEFAULT_CAPACITY {
		newSize = DEFAULT_CAPACITY
	} else if newSize > MAX_ARRAY_SIZE {
		newSize = MAX_ARRAY_SIZE
		if newSize == currentSize {
			// panic(NewIllegalArgumentException(format("Cannot grow array beyond '%s'", MAX_ARRAY_SIZE)))
			panic("Cannot grow array beyond " + strconv.Itoa(int(MAX_ARRAY_SIZE)))
		}
	}
	return int32(newSize)
}

func calculateBlockResetSize(currentSize int32) int32 {
	newSize := math.Ceil(float64(currentSize) * BLOCK_RESET_SKEW)
	if newSize < float64(DEFAULT_CAPACITY) {
		newSize = float64(DEFAULT_CAPACITY)
	} else if newSize > float64(MAX_ARRAY_SIZE) {
		newSize = float64(MAX_ARRAY_SIZE)
	}
	return int32(newSize)
}

func calculateBlockResetBytes(currentBytes int32) int32 {
	newBytes := int64(math.Ceil(float64(currentBytes) * BLOCK_RESET_SKEW))
	if newBytes > int64(MAX_ARRAY_SIZE) {
		return MAX_ARRAY_SIZE
	}
	return int32(newBytes)
}

func compactOffsets(offsets []int32, index int32, length int32) []int32 {
	if index == 0 && len(offsets) == int(length+1) {
		return offsets
	}
	newOffsets := make([]int32, length+1)
	for i := 1; i <= int(length); i++ {
		newOffsets[i] = offsets[index+int32(i)] - offsets[index]
	}
	return newOffsets
}

func compactSlice(slice *slice.Slice, index int32, length int32) *slice.Slice {
	if index == 0 && length == slice.SizeInt32() {
		return slice
	}
	s, _ := slice.MakeSlice(int(index), int(length))
	return s
}

func compactBoolArray(array []bool, index int32, length int32) []bool {
	if index == 0 && int(length) == len(array) {
		return array
	}
	dest := make([]bool, length)
	util.CopyBools(array, index, dest, 0, length)
	return dest
}

func compactByteArray(array []byte, index int32, length int32) []byte {
	if index == 0 && int(length) == len(array) {
		return array
	}
	dest := make([]byte, length)
	util.CopyBytes(array, index, dest, 0, length)
	return dest
}

func compactInt16Array(array []int16, index int32, length int32) []int16 {
	if index == 0 && int(length) == len(array) {
		return array
	}
	dest := make([]int16, length)
	util.CopyInt16s(array, index, dest, 0, length)
	return dest
}

func compactInt32Array(array []int32, index int32, length int32) []int32 {
	if index == 0 && int(length) == len(array) {
		return array
	}
	dest := make([]int32, length)
	util.CopyInt32s(array, index, dest, 0, length)
	return dest
}

func compactInt64Array(array []int64, index int32, length int32) []int64 {
	if index == 0 && int(length) == len(array) {
		return array
	}
	dest := make([]int64, length)
	util.CopyInt64s(array, index, dest, 0, length)
	return dest
}

func countUsedPositions(positions []bool) int32 {
	used := 0
	for _, position := range positions {
		if position {
			used += 1
		} else {
			used += 0
		}
	}
	return int32(used)
}

func ensureBlocksAreLoaded(blocks []Block) []Block {
	for i := 0; i < len(blocks); i++ {
		loaded := blocks[i].GetLoadedBlock()
		if loaded != blocks[i] {
			loadedBlocks := clone.Clone(blocks).([]Block)
			loadedBlocks[i] = loaded
			i++
			for ; i < len(blocks); i++ {
				loadedBlocks[i] = blocks[i].GetLoadedBlock()
			}
			return loadedBlocks
		}
	}
	return blocks
}

func blockArraySame(array1 []Block, array2 []Block) bool {
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

func corvertToBlocks(obj basic.Object) []Block {
	return obj.([]Block)
}
