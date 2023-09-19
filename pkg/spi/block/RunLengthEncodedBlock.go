package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var RUNLENGTH_INSTANCE_SIZE int32 = util.SizeOf(&RunLengthEncodedBlock{})

type RunLengthEncodedBlock struct {
	// 继承
	Block

	value         Block
	positionCount int32
}

func CreateRunLengthEncodedBlock(kind Type, value basic.Object, positionCount int32) Block {
	block := NativeValueToBlock(kind, value)
	rlb, flag := value.(*RunLengthEncodedBlock)
	if flag {
		block = rlb.GetValue()
	}
	return NewRunLengthEncodedBlock(block, positionCount)
}
func NewRunLengthEncodedBlock(value Block, positionCount int32) *RunLengthEncodedBlock {
	rk := new(RunLengthEncodedBlock)
	if value.GetPositionCount() != 1 {
		panic(fmt.Sprintf("Expected value to contain a single position but has %d positions", value.GetPositionCount()))
	}

	rlb, flag := value.(*RunLengthEncodedBlock)
	if flag {
		rk.value = rlb.GetValue()
	} else {
		rk.value = value
	}
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	rk.positionCount = positionCount
	return rk
}

// @Override
func (rk *RunLengthEncodedBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(rk.value)
}

func (rk *RunLengthEncodedBlock) GetValue() Block {
	return rk.value
}

// @Override
func (rk *RunLengthEncodedBlock) GetPositionCount() int32 {
	return rk.positionCount
}

// @Override
func (rk *RunLengthEncodedBlock) GetSizeInBytes() int64 {
	return rk.value.GetSizeInBytes()
}

// @Override
func (rk *RunLengthEncodedBlock) GetLogicalSizeInBytes() int64 {
	return int64(rk.positionCount) * rk.value.GetLogicalSizeInBytes()
}

// @Override
func (rk *RunLengthEncodedBlock) GetRetainedSizeInBytes() int64 {
	return int64(RUNLENGTH_INSTANCE_SIZE) + rk.value.GetRetainedSizeInBytes()
}

// @Override
func (rk *RunLengthEncodedBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return rk.value.GetEstimatedDataSizeForStats(0)
}

// @Override
func (rk *RunLengthEncodedBlock) GetPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	for i := offset; i < offset+length; i++ {
		checkValidPosition(positions[i], rk.positionCount)
	}
	return NewRunLengthEncodedBlock(rk.value, length)
}

// @Override
func (rk *RunLengthEncodedBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	for i := offset; i < offset+length; i++ {
		checkValidPosition(positions[i], rk.positionCount)
	}
	return NewRunLengthEncodedBlock(rk.value.CopyRegion(0, 1), length)
}

// @Override
func (rk *RunLengthEncodedBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(rk.positionCount, positionOffset, length)
	return NewRunLengthEncodedBlock(rk.value, length)
}

// @Override
func (rk *RunLengthEncodedBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	return rk.value.GetSizeInBytes()
}

// @Override
func (rk *RunLengthEncodedBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return rk.value.GetSizeInBytes()
}

// @Override
func (rk *RunLengthEncodedBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(rk.positionCount, positionOffset, length)
	return NewRunLengthEncodedBlock(rk.value.CopyRegion(0, 1), length)
}

// @Override
func (rk *RunLengthEncodedBlock) GetSliceLength(position int32) int32 {
	rk.checkReadablePosition(position)
	return rk.value.GetSliceLength(0)
}

// @Override
func (rk *RunLengthEncodedBlock) GetByte(position int32, offset int32) byte {
	rk.checkReadablePosition(position)
	return rk.value.GetByte(0, offset)
}

// @Override
func (rk *RunLengthEncodedBlock) GetShort(position int32, offset int32) int16 {
	rk.checkReadablePosition(position)
	return rk.value.GetShort(0, offset)
}

// @Override
func (rk *RunLengthEncodedBlock) GetInt(position int32, offset int32) int32 {
	rk.checkReadablePosition(position)
	return rk.value.GetInt(0, offset)
}

// @Override
func (rk *RunLengthEncodedBlock) GetLong(position int32, offset int32) int64 {
	rk.checkReadablePosition(position)
	return rk.value.GetLong(0, offset)
}

// @Override
func (rk *RunLengthEncodedBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	rk.checkReadablePosition(position)
	return rk.value.GetSlice(0, offset, length)
}

// @Override
func (rk *RunLengthEncodedBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	rk.checkReadablePosition(position)
	return rk.value.GetObject(0, clazz)
}

// @Override
func (rk *RunLengthEncodedBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	rk.checkReadablePosition(position)
	return rk.value.BytesEqual(0, offset, otherSlice, otherOffset, length)
}

// @Override
func (rk *RunLengthEncodedBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	rk.checkReadablePosition(position)
	return rk.value.BytesCompare(0, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (rk *RunLengthEncodedBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	rk.checkReadablePosition(position)
	rk.value.WriteBytesTo(0, offset, length, blockBuilder)
}

// @Override
func (rk *RunLengthEncodedBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	rk.checkReadablePosition(position)
	return rk.value.Equals(0, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (rk *RunLengthEncodedBlock) Hash(position int32, offset int32, length int32) int64 {
	rk.checkReadablePosition(position)
	return rk.value.Hash(0, offset, length)
}

// @Override
func (rk *RunLengthEncodedBlock) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	rk.checkReadablePosition(leftPosition)
	return rk.value.CompareTo(0, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (rk *RunLengthEncodedBlock) GetSingleValueBlock(position int32) Block {
	rk.checkReadablePosition(position)
	return rk.value
}

// @Override
func (rk *RunLengthEncodedBlock) MayHaveNull() bool {
	return rk.positionCount > 0 && rk.value.IsNull(0)
}

// @Override
func (rk *RunLengthEncodedBlock) IsNull(position int32) bool {
	rk.checkReadablePosition(position)
	return rk.value.IsNull(0)
}

// @Override
func (rk *RunLengthEncodedBlock) IsLoaded() bool {
	return rk.value.IsLoaded()
}

// @Override
func (rk *RunLengthEncodedBlock) GetLoadedBlock() Block {
	loadedValueBlock := rk.value.GetLoadedBlock()
	if loadedValueBlock == rk.value {
		return rk
	}
	return NewRunLengthEncodedBlock(loadedValueBlock, rk.positionCount)
}

func (rk *RunLengthEncodedBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= rk.positionCount {
		panic("position is not valid")
	}
}
