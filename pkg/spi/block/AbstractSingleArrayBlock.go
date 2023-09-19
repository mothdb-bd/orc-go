package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractSingleArrayBlock struct {
	Block // 继承block

	start int32
}

func NewAbstractSingleArrayBlock(start int32) *AbstractSingleArrayBlock {
	ak := new(AbstractSingleArrayBlock)
	ak.start = start
	return ak
}

// protected abstract Block getBlock();
func (b *AbstractSingleArrayBlock) getBlock() Block {
	return nil
}

// @Override
func (ak *AbstractSingleArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getBlock())
}

func (ak *AbstractSingleArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ak.GetPositionCount() {
		panic("position is not valid")
	}
}

// @Override
func (ak *AbstractSingleArrayBlock) GetSliceLength(position int32) int32 {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetSliceLength(position + ak.start)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetByte(position int32, offset int32) byte {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetByte(position+ak.start, offset)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetShort(position int32, offset int32) int16 {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetShort(position+ak.start, offset)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetInt(position int32, offset int32) int32 {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetInt(position+ak.start, offset)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetLong(position int32, offset int32) int64 {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetLong(position+ak.start, offset)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetSlice(position+ak.start, offset, length)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetObject(position+ak.start, clazz)
}

// @Override
func (ak *AbstractSingleArrayBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	return ak.getBlock().BytesEqual(position+ak.start, offset, otherSlice, otherOffset, length)
}

// @Override
func (ak *AbstractSingleArrayBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	ak.checkReadablePosition(position)
	return ak.getBlock().BytesCompare(position+ak.start, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (ak *AbstractSingleArrayBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	ak.checkReadablePosition(position)
	ak.getBlock().WriteBytesTo(position+ak.start, offset, length, blockBuilder)
}

// @Override
func (ak *AbstractSingleArrayBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	ak.checkReadablePosition(position)
	return ak.getBlock().Equals(position+ak.start, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (ak *AbstractSingleArrayBlock) Hash(position int32, offset int32, length int32) int64 {
	ak.checkReadablePosition(position)
	return ak.getBlock().Hash(position+ak.start, offset, length)
}

// @Override
func (ak *AbstractSingleArrayBlock) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	ak.checkReadablePosition(leftPosition)
	return ak.getBlock().CompareTo(leftPosition+ak.start, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetSingleValueBlock(position int32) Block {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetSingleValueBlock(position + ak.start)
}

// @Override
func (ak *AbstractSingleArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	ak.checkReadablePosition(position)
	return ak.getBlock().GetEstimatedDataSizeForStats(position + ak.start)
}

// @Override
func (ak *AbstractSingleArrayBlock) IsNull(position int32) bool {
	ak.checkReadablePosition(position)
	return ak.getBlock().IsNull(position + ak.start)
}

// @Override
func (ak *AbstractSingleArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	panic("Unsupported")
}

// @Override
func (ak *AbstractSingleArrayBlock) GetRegion(position int32, length int32) Block {
	panic("Unsupported")
}

// @Override
func (ak *AbstractSingleArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	panic("Unsupported")
}

// @Override
func (ak *AbstractSingleArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	panic("Unsupported")
}

// @Override
func (ak *AbstractSingleArrayBlock) CopyRegion(position int32, length int32) Block {
	panic("Unsupported")
}
