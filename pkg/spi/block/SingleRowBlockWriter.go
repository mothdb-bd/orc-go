package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var SINGLEROW_INSTANCE_SIZE int32 = util.SizeOf(&SingleRowBlockWriter{})

type SingleRowBlockWriter struct {
	// 继承
	AbstractSingleRowBlock
	// 继承
	BlockBuilder

	fieldBlockBuilders        []BlockBuilder
	currentFieldIndexToWrite  int32
	rowIndex                  int32
	fieldBlockBuilderReturned bool
}

func NewSingleRowBlockWriter(fieldBlockBuilders []BlockBuilder) *SingleRowBlockWriter {
	sr := new(SingleRowBlockWriter)
	sr.fieldBlockBuilders = fieldBlockBuilders
	return sr
}

func (sr *SingleRowBlockWriter) GetFieldBlockBuilder(fieldIndex int32) BlockBuilder {
	if sr.currentFieldIndexToWrite != 0 {
		panic("field block builder can only be obtained before any sequential write has done")
	}
	sr.fieldBlockBuilderReturned = true
	return sr.fieldBlockBuilders[fieldIndex]
}

// @Override
func (sr *SingleRowBlockWriter) getRawFieldBlocks() []Block {
	return corvertToBlocks(sr.fieldBlockBuilders)
}

// @Override
func (sr *SingleRowBlockWriter) getRawFieldBlock(fieldIndex int32) Block {
	return sr.fieldBlockBuilders[fieldIndex]
}

// @Override
func (sr *SingleRowBlockWriter) getRowIndex() int32 {
	return sr.rowIndex
}

// @Override
func (sr *SingleRowBlockWriter) GetSizeInBytes() int64 {
	currentBlockBuilderSize := util.INT64_ZERO
	for _, fieldBlockBuilder := range sr.fieldBlockBuilders {
		currentBlockBuilderSize += fieldBlockBuilder.GetSizeInBytes() - fieldBlockBuilder.GetRegionSizeInBytes(0, sr.rowIndex)
	}
	return currentBlockBuilderSize
}

// @Override
func (sr *SingleRowBlockWriter) GetRetainedSizeInBytes() int64 {
	size := int64(SINGLEROW_INSTANCE_SIZE)
	for _, fieldBlockBuilder := range sr.fieldBlockBuilders {
		size += fieldBlockBuilder.GetRetainedSizeInBytes()
	}
	return size
}

// @Override
func (sr *SingleRowBlockWriter) WriteByte(value byte) BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].WriteByte(value)
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) WriteShort(value int16) BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].WriteShort(value)
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) WriteInt(value int32) BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].WriteInt(value)
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) WriteLong(value int64) BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].WriteLong(value)
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) WriteBytes(source *slice.Slice, sourceIndex int32, length int32) BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].WriteBytes(source, sourceIndex, length)
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) BeginBlockEntry() BlockBuilder {
	sr.checkFieldIndexToWrite()
	return sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].BeginBlockEntry()
}

// @Override
func (sr *SingleRowBlockWriter) AppendNull() BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].AppendNull()
	sr.entryAdded()
	return sr
}

// @Override
func (sr *SingleRowBlockWriter) CloseEntry() BlockBuilder {
	sr.checkFieldIndexToWrite()
	sr.fieldBlockBuilders[sr.currentFieldIndexToWrite].CloseEntry()
	sr.entryAdded()
	return sr
}

func (sr *SingleRowBlockWriter) entryAdded() {
	sr.currentFieldIndexToWrite++
}

// @Override
func (sr *SingleRowBlockWriter) GetPositionCount() int32 {
	if sr.fieldBlockBuilderReturned {
		panic("field block builder has been returned")
	}
	return sr.currentFieldIndexToWrite
}

// @Override
func (sr *SingleRowBlockWriter) Build() Block {
	panic("Unsupported")
}

// @Override
func (sr *SingleRowBlockWriter) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	panic("Unsupported")
}

func (sr *SingleRowBlockWriter) setRowIndex(rowIndex int32) {
	if sr.rowIndex != -1 {
		panic("SingleRowBlockWriter should be reset before usage")
	}
	sr.rowIndex = rowIndex
}

func (sr *SingleRowBlockWriter) reset() {
	if sr.rowIndex == -1 {
		panic("SingleRowBlockWriter is already reset")
	}
	sr.rowIndex = -1
	sr.currentFieldIndexToWrite = 0
	sr.fieldBlockBuilderReturned = false
}

func (sr *SingleRowBlockWriter) checkFieldIndexToWrite() {
	if sr.fieldBlockBuilderReturned {
		panic("cannot do sequential write after getFieldBlockBuilder is called")
	}
	if sr.currentFieldIndexToWrite >= int32(len(sr.fieldBlockBuilders)) {
		panic("currentFieldIndexToWrite is not valid")
	}
}

// / 继承类 Block
// @Override
func (sr *SingleRowBlockWriter) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return sr.AbstractSingleRowBlock.BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (sr *SingleRowBlockWriter) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return sr.AbstractSingleRowBlock.BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (sr *SingleRowBlockWriter) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return sr.AbstractSingleRowBlock.CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (sr *SingleRowBlockWriter) CopyPositions(positions []int32, offset int32, length int32) Block {
	return sr.AbstractSingleRowBlock.CopyPositions(positions, offset, length)
}

// @Override
func (sr *SingleRowBlockWriter) CopyRegion(position int32, length int32) Block {
	return sr.AbstractSingleRowBlock.CopyRegion(position, length)
}

// @Override
func (sr *SingleRowBlockWriter) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return sr.AbstractSingleRowBlock.Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (sr *SingleRowBlockWriter) GetChildren() *util.ArrayList[Block] {
	return sr.AbstractSingleRowBlock.GetChildren()
}

// @Override
func (sr *SingleRowBlockWriter) GetEstimatedDataSizeForStats(position int32) int64 {
	return sr.AbstractSingleRowBlock.GetEstimatedDataSizeForStats(position)
}

// @Override
func (sr *SingleRowBlockWriter) GetByte(position int32, offset int32) byte {
	return sr.AbstractSingleRowBlock.GetByte(position, offset)
}

// @Override
func (sr *SingleRowBlockWriter) GetShort(position int32, offset int32) int16 {
	return sr.AbstractSingleRowBlock.GetShort(position, offset)
}

// @Override
func (sr *SingleRowBlockWriter) GetInt(position int32, offset int32) int32 {
	return sr.AbstractSingleRowBlock.GetInt(position, offset)
}

// @Override
func (sr *SingleRowBlockWriter) GetLong(position int32, offset int32) int64 {
	return sr.AbstractSingleRowBlock.GetLong(position, offset)
}

// @Override
func (sr *SingleRowBlockWriter) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return sr.AbstractSingleRowBlock.GetSlice(position, offset, length)
}

// @Override
func (sr *SingleRowBlockWriter) GetObject(position int32, clazz reflect.Type) basic.Object {
	return sr.AbstractSingleRowBlock.GetObject(position, clazz)
}

// @Override
func (sr *SingleRowBlockWriter) GetPositionsSizeInBytes(positions []bool) int64 {
	return sr.AbstractSingleRowBlock.GetPositionsSizeInBytes(positions)
}

// @Override
func (sr *SingleRowBlockWriter) GetRegion(positionOffset int32, length int32) Block {
	return sr.AbstractSingleRowBlock.GetRegion(positionOffset, length)
}

// @Override
func (sr *SingleRowBlockWriter) GetRegionSizeInBytes(position int32, length int32) int64 {
	return sr.AbstractSingleRowBlock.GetRegionSizeInBytes(position, length)
}

// @Override
func (sr *SingleRowBlockWriter) GetSingleValueBlock(position int32) Block {
	return sr.AbstractSingleRowBlock.GetSingleValueBlock(position)
}

// @Override
func (sr *SingleRowBlockWriter) GetSliceLength(position int32) int32 {
	return sr.AbstractSingleRowBlock.GetSliceLength(position)
}

// @Override
func (sr *SingleRowBlockWriter) Hash(position int32, offset int32, length int32) int64 {
	return sr.AbstractSingleRowBlock.Hash(position, offset, length)
}

// @Override
func (sr *SingleRowBlockWriter) IsNull(position int32) bool {
	return sr.AbstractSingleRowBlock.IsNull(position)
}

// @Override
func (sr *SingleRowBlockWriter) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	sr.AbstractSingleRowBlock.WriteBytesTo(position, offset, length, blockBuilder)
}
func (ak *SingleRowBlockWriter) GetLoadedBlock() Block {
	return ak
}

func (ik *SingleRowBlockWriter) IsLoaded() bool {
	return true
}
