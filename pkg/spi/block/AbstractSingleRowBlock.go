package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractSingleRowBlock struct {
	Block // 继承block

}

// abstract
func (ak *AbstractSingleRowBlock) getRawFieldBlocks() []Block {
	return nil
}

// abstract Block getRawFieldBlock(int fieldIndex);
func (ak *AbstractSingleRowBlock) getRawFieldBlock(fieldIndex int32) Block {
	return nil
}

// abstract int getRowIndex();
func (ak *AbstractSingleRowBlock) getRowIndex() int32 {
	return 0
}

// @Override
func (ak *AbstractSingleRowBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawFieldBlocks()...)
}

func (ak *AbstractSingleRowBlock) checkFieldIndex(position int32) {
	if position < 0 || position >= ak.GetPositionCount() {
		panic(fmt.Sprintf("position is not valid: %d", position))
	}
}

// @Override
func (ak *AbstractSingleRowBlock) IsNull(position int32) bool {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).IsNull(ak.getRowIndex())
}

// @Override
func (ak *AbstractSingleRowBlock) GetByte(position int32, offset int32) byte {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetByte(ak.getRowIndex(), offset)
}

// @Override
func (ak *AbstractSingleRowBlock) GetShort(position int32, offset int32) int16 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetShort(ak.getRowIndex(), offset)
}

// @Override
func (ak *AbstractSingleRowBlock) GetInt(position int32, offset int32) int32 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetInt(ak.getRowIndex(), offset)
}

// @Override
func (ak *AbstractSingleRowBlock) GetLong(position int32, offset int32) int64 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetLong(ak.getRowIndex(), offset)
}

// @Override
func (ak *AbstractSingleRowBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetSlice(ak.getRowIndex(), offset, length)
}

// @Override
func (ak *AbstractSingleRowBlock) GetSliceLength(position int32) int32 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetSliceLength(ak.getRowIndex())
}

// @Override
func (ak *AbstractSingleRowBlock) CompareTo(position int32, offset int32, length int32, otherBlock Block, otherPosition int32, otherOffset int32, otherLength int32) int32 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).CompareTo(ak.getRowIndex(), offset, length, otherBlock, otherPosition, otherOffset, otherLength)
}

// @Override
func (ak *AbstractSingleRowBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).BytesEqual(ak.getRowIndex(), offset, otherSlice, otherOffset, length)
}

// @Override
func (ak *AbstractSingleRowBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).BytesCompare(ak.getRowIndex(), offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (ak *AbstractSingleRowBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	ak.checkFieldIndex(position)
	ak.getRawFieldBlock(position).WriteBytesTo(ak.getRowIndex(), offset, length, blockBuilder)
}

// @Override
func (ak *AbstractSingleRowBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).Equals(ak.getRowIndex(), offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (ak *AbstractSingleRowBlock) Hash(position int32, offset int32, length int32) int64 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).Hash(ak.getRowIndex(), offset, length)
}

// @Override
func (ak *AbstractSingleRowBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetObject(ak.getRowIndex(), clazz)
}

// @Override
func (ak *AbstractSingleRowBlock) GetSingleValueBlock(position int32) Block {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetSingleValueBlock(ak.getRowIndex())
}

// @Override
func (ak *AbstractSingleRowBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	ak.checkFieldIndex(position)
	return ak.getRawFieldBlock(position).GetEstimatedDataSizeForStats(ak.getRowIndex())
}

// @Override
func (ak *AbstractSingleRowBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	panic("Unsupported region size")
}

// @Override
func (ak *AbstractSingleRowBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	panic("Unsupported positions size")
}

// @Override
func (ak *AbstractSingleRowBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	panic("Unsupported positions copy")
}

// @Override
func (ak *AbstractSingleRowBlock) GetRegion(positionOffset int32, length int32) Block {
	panic("Unsupported region get")
}

// @Override
func (ak *AbstractSingleRowBlock) CopyRegion(position int32, length int32) Block {
	panic("Unsupported region copy")
}
