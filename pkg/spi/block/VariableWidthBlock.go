package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type VariableWidthBlock struct {
	// 继承
	AbstractVariableWidthBlock

	arrayOffset         int32
	positionCount       int32
	slice               *slice.Slice
	offsets             []int32 //@Nullable
	valueIsNull         []bool
	retainedSizeInBytes int64
	sizeInBytes         int64
}

// var variableWidthBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize()
var VW_INSTANCE_SIZE = util.SizeOf(&VariableWidthBlock{})

func NewVariableWidthBlock(positionCount int32, slice *slice.Slice, offsets []int32, valueIsNull *optional.Optional[[]bool]) *VariableWidthBlock {
	return NewVariableWidthBlock2(0, positionCount, slice, offsets, valueIsNull.OrElse(nil))
}

func NewVariableWidthBlock2(arrayOffset int32, positionCount int32, slice *slice.Slice, offsets []int32, valueIsNull []bool) *VariableWidthBlock {
	vk := new(VariableWidthBlock)
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	vk.arrayOffset = arrayOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	vk.positionCount = positionCount
	if slice == nil {
		panic("slice is null")
	}
	vk.slice = slice
	if int32(len(offsets))-arrayOffset < positionCount+1 {
		panic("offsets length is less than positionCount")
	}
	vk.offsets = offsets
	if valueIsNull != nil && int32(len(valueIsNull))-arrayOffset < positionCount {
		panic("valueIsNull length is less than positionCount")
	}
	vk.valueIsNull = valueIsNull
	// vk.sizeInBytes = offsets[arrayOffset+positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES) * positionCount.(int64))
	vk.sizeInBytes = int64(offsets[arrayOffset+positionCount] - offsets[arrayOffset] + (util.INT32_BYTES+util.BYTE_BYTES)*positionCount)
	// retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets)
	vk.retainedSizeInBytes = int64(VW_INSTANCE_SIZE + slice.GetRetainedSize() + util.SizeOf(valueIsNull) + util.SizeOf(offsets))
	return vk
}

// @Override
func (vk *VariableWidthBlock) getPositionOffset(position int32) int32 {
	return vk.offsets[position+vk.arrayOffset]
}

// @Override
func (vk *VariableWidthBlock) GetSliceLength(position int32) int32 {
	vk.checkReadablePosition(position)
	return vk.getPositionOffset(position+1) - vk.getPositionOffset(position)
}

// @Override
func (vk *VariableWidthBlock) MayHaveNull() bool {
	return vk.valueIsNull != nil
}

// @Override
func (vk *VariableWidthBlock) isEntryNull(position int32) bool {
	return vk.valueIsNull != nil && vk.valueIsNull[position+vk.arrayOffset]
}

// @Override
func (vk *VariableWidthBlock) GetPositionCount() int32 {
	return vk.positionCount
}

// @Override
func (vk *VariableWidthBlock) IsNull(position int32) bool {
	vk.checkReadablePosition(position)
	return vk.isEntryNull(position)
}

// @Override
func (vk *VariableWidthBlock) GetSizeInBytes() int64 {
	return vk.sizeInBytes
}

// @Override
func (vk *VariableWidthBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return vk.offsets[vk.arrayOffset+position+length] - vk.offsets[vk.arrayOffset+position] + ((Integer.BYTES + Byte.BYTES) * length.(int64))
	return int64(vk.offsets[vk.arrayOffset+position+length] - vk.offsets[vk.arrayOffset+position] + ((util.INT32_BYTES + util.BYTE_BYTES) * length))
}

// @Override
func (vk *VariableWidthBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	sizeInBytes := 0
	usedPositionCount := 0
	for i := 0; i < len(positions); i++ {
		if positions[i] {
			usedPositionCount++
			sizeInBytes += int(vk.offsets[int(vk.arrayOffset)+i+1] - vk.offsets[int(vk.arrayOffset)+i])
		}
	}
	// return sizeInBytes + (Integer.BYTES+Byte.BYTES)*usedPositionCount.(int64)
	return int64(sizeInBytes + (util.INT32_BYTES+util.BYTE_BYTES)*usedPositionCount)
}

// @Override
func (vk *VariableWidthBlock) GetRetainedSizeInBytes() int64 {
	return vk.retainedSizeInBytes
}

// @Override
func (vk *VariableWidthBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if length == 0 {
		return NewVariableWidthBlock2(0, 0, slice.EMPTY_SLICE, make([]int32, 1), nil)
	}
	newOffsets := make([]int32, length+1)
	finalLength := 0
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		finalLength += int(vk.GetSliceLength(position))
		newOffsets[i+1] = int32(finalLength)
	}
	// newSlice := Slices.allocate(finalLength).getOutput()
	newSlice := slice.NewWithSize(finalLength)
	var newValueIsNull []bool = nil
	firstPosition := positions[offset]
	if vk.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
		newValueIsNull[0] = vk.valueIsNull[firstPosition+vk.arrayOffset]
	}
	currentStart := vk.getPositionOffset(firstPosition)
	currentEnd := vk.getPositionOffset(firstPosition + 1)
	for i := 1; i < int(length); i++ {
		position := positions[int(offset)+i]
		if vk.valueIsNull != nil {
			newValueIsNull[i] = vk.valueIsNull[position+vk.arrayOffset]
		}
		currentOffset := vk.getPositionOffset(position)
		if currentOffset != currentEnd {
			newSlice.WriteSlice(vk.slice, int(currentStart), int(currentEnd-currentStart))
			currentStart = currentOffset
		}
		currentEnd = vk.getPositionOffset(position + 1)
	}
	newSlice.WriteSlice(vk.slice, int(currentStart), int(currentEnd-currentStart))
	return NewVariableWidthBlock2(0, length, newSlice, newOffsets, newValueIsNull)
}

// @Override
func (vk *VariableWidthBlock) getRawSlice(position int32) *slice.Slice {
	return vk.slice
}

// @Override
func (vk *VariableWidthBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(vk.GetPositionCount(), positionOffset, length)
	return NewVariableWidthBlock2(positionOffset+vk.arrayOffset, length, vk.slice, vk.offsets, vk.valueIsNull)
}

// @Override
func (vk *VariableWidthBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(vk.GetPositionCount(), positionOffset, length)
	positionOffset += vk.arrayOffset
	newOffsets := compactOffsets(vk.offsets, positionOffset, length)
	newSlice := compactSlice(vk.slice, vk.offsets[positionOffset], newOffsets[length])

	var newValueIsNull []bool = nil
	if vk.valueIsNull != nil {
		newValueIsNull = compactBoolArray(vk.valueIsNull, positionOffset, length)
	}
	if reflect.DeepEqual(newOffsets, vk.offsets) && newSlice == vk.slice && reflect.DeepEqual(newValueIsNull, vk.valueIsNull) {
		return vk
	}
	return NewVariableWidthBlock2(0, length, newSlice, newOffsets, newValueIsNull)
}

// 所有父类
// @Override
func (ak *VariableWidthBlock) GetByte(position int32, offset int32) byte {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetByte(int(ak.getPositionOffset(position) + offset))
	if err == nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlock) GetShort(position int32, offset int32) int16 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt16LE(int(ak.getPositionOffset(position) + offset))
	if err != nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlock) GetInt(position int32, offset int32) int32 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt32LE(int(ak.getPositionOffset(position) + offset))
	if err == nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlock) GetLong(position int32, offset int32) int64 {
	ak.checkReadablePosition(position)
	b, err := ak.getRawSlice(position).GetInt64LE(int(ak.getPositionOffset(position) + offset))
	if err != nil {
		return b
	} else {
		panic(err.Error())
	}
}

// @Override
func (ak *VariableWidthBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	ak.checkReadablePosition(position)
	s, err := ak.getRawSlice(position).MakeSlice(int(ak.getPositionOffset(position)+offset), int(length))
	if err != nil {
		panic(err.Error())
	}
	return s
}

// @Override
func (ak *VariableWidthBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	rawSlice := ak.getRawSlice(position)
	if ak.GetSliceLength(position) < length {
		return false
	}
	return otherBlock.BytesEqual(otherPosition, otherOffset, rawSlice, int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	return ak.getRawSlice(position).Equal2(int(ak.getPositionOffset(position)+offset), otherSlice, int(otherOffset), int(length))
}

// @Override
func (ak *VariableWidthBlock) Hash(position int32, offset int32, length int32) int64 {
	ak.checkReadablePosition(position)
	// return XxHash64.hash(ak.getRawSlice(position), ak.getPositionOffset(position)+offset, length)
	return int64(ak.getRawSlice(position).HashCodeValue(int(ak.getPositionOffset(position)+offset), int(length)))
}

// @Override
func (ak *VariableWidthBlock) CompareTo(position int32, offset int32, length int32, otherBlock Block, otherPosition int32, otherOffset int32, otherLength int32) int32 {
	ak.checkReadablePosition(position)
	rawSlice := ak.getRawSlice(position)
	if ak.GetSliceLength(position) < length {
		panic("Length longer than value length")
	}
	return -otherBlock.BytesCompare(otherPosition, otherOffset, otherLength, rawSlice, int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	ak.checkReadablePosition(position)
	return int32(ak.getRawSlice(position).CompareTo2(int(ak.getPositionOffset(position)+offset), int(length), otherSlice, int(otherOffset), int(otherLength)))
}

// @Override
func (ak *VariableWidthBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	ak.checkReadablePosition(position)
	blockBuilder.WriteBytes(ak.getRawSlice(position), int32(ak.getPositionOffset(position))+offset, length)
}

// @Override
func (ak *VariableWidthBlock) GetSingleValueBlock(position int32) Block {
	if ak.IsNull(position) {
		return NewVariableWidthBlock2(0, 1, slice.EMPTY_SLICE, []int32{0, 0}, []bool{true})
	}
	offset := ak.getPositionOffset(position)
	entrySize := ak.GetSliceLength(position)
	// copy := Slices.copyOf(ak.getRawSlice(position), offset, entrySize)
	copy, _ := ak.getRawSlice(position).MakeSlice(int(offset), int(entrySize))
	return NewVariableWidthBlock2(0, 1, copy, []int32{0, int32(copy.Size())}, nil)
}

// @Override
func (ak *VariableWidthBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	if ak.IsNull(position) {
		return 0
	} else {
		return int64(ak.GetSliceLength(position))
	}
}

func (ak *VariableWidthBlock) checkReadablePosition(position int32) {
	checkValidPosition(position, ak.GetPositionCount())
}

func (ak *VariableWidthBlock) GetLoadedBlock() Block {
	return ak
}
func (ak *VariableWidthBlock) IsLoaded() bool {
	return true
}
func (ik *VariableWidthBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
