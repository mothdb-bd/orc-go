package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SingleArrayBlockWriter struct {
	// 继承
	AbstractSingleArrayBlock
	// 继承
	BlockBuilder

	blockBuilder            BlockBuilder
	initialBlockBuilderSize int64
	positionsWritten        int32
}

// var singleArrayBlockWriteriNSTANCE_SIZE int32 = ClassLayout.parseClass(SingleArrayBlockWriter.class).instanceSize()
var SINGLE_ARRAY_INSTANCE_SIZE int32 = util.SizeOf(&SingleArrayBlockWriter{})

func NewSingleArrayBlockWriter(blockBuilder BlockBuilder, start int32) *SingleArrayBlockWriter {
	sr := new(SingleArrayBlockWriter)
	sr.AbstractSingleArrayBlock = *NewAbstractSingleArrayBlock(start)
	sr.blockBuilder = blockBuilder
	sr.initialBlockBuilderSize = blockBuilder.GetSizeInBytes()
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) getBlock() Block {
	return sr.blockBuilder
}

// @Override
func (sr *SingleArrayBlockWriter) GetSizeInBytes() int64 {
	return sr.blockBuilder.GetSizeInBytes() - sr.initialBlockBuilderSize
}

// @Override
func (sr *SingleArrayBlockWriter) GetRetainedSizeInBytes() int64 {
	return int64(SINGLE_ARRAY_INSTANCE_SIZE) + sr.blockBuilder.GetRetainedSizeInBytes()
}

// @Override
func (sr *SingleArrayBlockWriter) WriteByte(value byte) BlockBuilder {
	sr.blockBuilder.WriteByte(value)
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) WriteShort(value int16) BlockBuilder {
	sr.blockBuilder.WriteShort(value)
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) WriteInt(value int32) BlockBuilder {
	sr.blockBuilder.WriteInt(value)
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) WriteLong(value int64) BlockBuilder {
	sr.blockBuilder.WriteLong(value)
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) WriteBytes(source *slice.Slice, sourceIndex int32, length int32) BlockBuilder {
	sr.blockBuilder.WriteBytes(source, sourceIndex, length)
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) BeginBlockEntry() BlockBuilder {
	return sr.blockBuilder.BeginBlockEntry()
}

// @Override
func (sr *SingleArrayBlockWriter) AppendNull() BlockBuilder {
	sr.blockBuilder.AppendNull()
	sr.entryAdded()
	return sr
}

// @Override
func (sr *SingleArrayBlockWriter) CloseEntry() BlockBuilder {
	sr.blockBuilder.CloseEntry()
	sr.entryAdded()
	return sr
}

func (sr *SingleArrayBlockWriter) entryAdded() {
	sr.positionsWritten++
}

// @Override
func (sr *SingleArrayBlockWriter) GetPositionCount() int32 {
	return sr.positionsWritten
}

// @Override
func (sr *SingleArrayBlockWriter) Build() Block {
	panic("Unsupported")
}

// @Override
func (sr *SingleArrayBlockWriter) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	panic("Unsupported")
}

// / 继承类 Block
// @Override
func (sr *SingleArrayBlockWriter) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return sr.AbstractSingleArrayBlock.BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (sr *SingleArrayBlockWriter) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return sr.AbstractSingleArrayBlock.BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return sr.AbstractSingleArrayBlock.CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (sr *SingleArrayBlockWriter) CopyPositions(positions []int32, offset int32, length int32) Block {
	return sr.AbstractSingleArrayBlock.CopyPositions(positions, offset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) CopyRegion(position int32, length int32) Block {
	return sr.AbstractSingleArrayBlock.CopyRegion(position, length)
}

// @Override
func (sr *SingleArrayBlockWriter) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return sr.AbstractSingleArrayBlock.Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) GetChildren() *util.ArrayList[Block] {
	return sr.AbstractSingleArrayBlock.GetChildren()
}

// @Override
func (sr *SingleArrayBlockWriter) GetEstimatedDataSizeForStats(position int32) int64 {
	return sr.AbstractSingleArrayBlock.GetEstimatedDataSizeForStats(position)
}

// @Override
func (sr *SingleArrayBlockWriter) GetByte(position int32, offset int32) byte {
	return sr.AbstractSingleArrayBlock.GetByte(position, offset)
}

// @Override
func (sr *SingleArrayBlockWriter) GetShort(position int32, offset int32) int16 {
	return sr.AbstractSingleArrayBlock.GetShort(position, offset)
}

// @Override
func (sr *SingleArrayBlockWriter) GetInt(position int32, offset int32) int32 {
	return sr.AbstractSingleArrayBlock.GetInt(position, offset)
}

// @Override
func (sr *SingleArrayBlockWriter) GetLong(position int32, offset int32) int64 {
	return sr.AbstractSingleArrayBlock.GetLong(position, offset)
}

// @Override
func (sr *SingleArrayBlockWriter) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return sr.AbstractSingleArrayBlock.GetSlice(position, offset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) GetObject(position int32, clazz reflect.Type) basic.Object {
	return sr.AbstractSingleArrayBlock.GetObject(position, clazz)
}

// @Override
func (sr *SingleArrayBlockWriter) GetPositionsSizeInBytes(positions []bool) int64 {
	return sr.AbstractSingleArrayBlock.GetPositionsSizeInBytes(positions)
}

// @Override
func (sr *SingleArrayBlockWriter) GetRegion(positionOffset int32, length int32) Block {
	return sr.AbstractSingleArrayBlock.GetRegion(positionOffset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) GetRegionSizeInBytes(position int32, length int32) int64 {
	return sr.AbstractSingleArrayBlock.GetRegionSizeInBytes(position, length)
}

// @Override
func (sr *SingleArrayBlockWriter) GetSingleValueBlock(position int32) Block {
	return sr.AbstractSingleArrayBlock.GetSingleValueBlock(position)
}

// @Override
func (sr *SingleArrayBlockWriter) GetSliceLength(position int32) int32 {
	return sr.AbstractSingleArrayBlock.GetSliceLength(position)
}

// @Override
func (sr *SingleArrayBlockWriter) Hash(position int32, offset int32, length int32) int64 {
	return sr.AbstractSingleArrayBlock.Hash(position, offset, length)
}

// @Override
func (sr *SingleArrayBlockWriter) IsNull(position int32) bool {
	return sr.AbstractSingleArrayBlock.IsNull(position)
}

// @Override
func (sr *SingleArrayBlockWriter) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	sr.AbstractSingleArrayBlock.WriteBytesTo(position, offset, length, blockBuilder)
}
func (ak *SingleArrayBlockWriter) GetLoadedBlock() Block {
	return ak
}

func (ik *SingleArrayBlockWriter) IsLoaded() bool {
	return true
}
