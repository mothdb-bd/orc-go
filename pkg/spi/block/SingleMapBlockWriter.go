package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SingleMapBlockWriter struct {
	// 继承
	BlockBuilder
	// 继承
	AbstractSingleMapBlock

	offset                  int32
	keyBlockBuilder         BlockBuilder
	valueBlockBuilder       BlockBuilder
	setStrict               func() *MapBlockBuilder
	initialBlockBuilderSize int64
	positionsWritten        int32
	writeToValueNext        bool
}

// var singleMapBlockWriteriNSTANCE_SIZE int32 = ClassLayout.parseClass(SingleMapBlockWriter.class).instanceSize()

var SINGLE_MAP_INSTANCE_SIZE int32 = util.SizeOf(&SingleMapBlockWriter{})

func NewSingleMapBlockWriter(start int32, keyBlockBuilder BlockBuilder, valueBlockBuilder BlockBuilder, setStrict func() *MapBlockBuilder) *SingleMapBlockWriter {
	sr := new(SingleMapBlockWriter)
	sr.offset = start
	sr.keyBlockBuilder = keyBlockBuilder
	sr.valueBlockBuilder = valueBlockBuilder
	sr.setStrict = setStrict
	sr.initialBlockBuilderSize = keyBlockBuilder.GetSizeInBytes() + valueBlockBuilder.GetSizeInBytes()
	return sr
}

func (sr *SingleMapBlockWriter) Strict() *SingleMapBlockWriter {
	sr.setStrict()
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) getOffset() int32 {
	return sr.offset
}

// @Override
func (sr *SingleMapBlockWriter) getRawKeyBlock() Block {
	return sr.keyBlockBuilder
}

// @Override
func (sr *SingleMapBlockWriter) getRawValueBlock() Block {
	return sr.valueBlockBuilder
}

// @Override
func (sr *SingleMapBlockWriter) GetSizeInBytes() int64 {
	return sr.keyBlockBuilder.GetSizeInBytes() + sr.valueBlockBuilder.GetSizeInBytes() - sr.initialBlockBuilderSize
}

// @Override
func (sr *SingleMapBlockWriter) GetRetainedSizeInBytes() int64 {
	return int64(SINGLE_MAP_INSTANCE_SIZE) + sr.keyBlockBuilder.GetRetainedSizeInBytes() + sr.valueBlockBuilder.GetRetainedSizeInBytes()
}

// @Override
func (sr *SingleMapBlockWriter) WriteByte(value byte) BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.WriteByte(value)
	} else {
		sr.keyBlockBuilder.WriteByte(value)
	}
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) WriteShort(value int16) BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.WriteShort(value)
	} else {
		sr.keyBlockBuilder.WriteShort(value)
	}
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) WriteInt(value int32) BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.WriteInt(value)
	} else {
		sr.keyBlockBuilder.WriteInt(value)
	}
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) WriteLong(value int64) BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.WriteLong(value)
	} else {
		sr.keyBlockBuilder.WriteLong(value)
	}
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) WriteBytes(source *slice.Slice, sourceIndex int32, length int32) BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.WriteBytes(source, sourceIndex, length)
	} else {
		sr.keyBlockBuilder.WriteBytes(source, sourceIndex, length)
	}
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) BeginBlockEntry() BlockBuilder {
	var result BlockBuilder
	if sr.writeToValueNext {
		result = sr.valueBlockBuilder.BeginBlockEntry()
	} else {
		result = sr.keyBlockBuilder.BeginBlockEntry()
	}
	return result
}

// @Override
func (sr *SingleMapBlockWriter) AppendNull() BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.AppendNull()
	} else {
		sr.keyBlockBuilder.AppendNull()
	}
	sr.entryAdded()
	return sr
}

// @Override
func (sr *SingleMapBlockWriter) CloseEntry() BlockBuilder {
	if sr.writeToValueNext {
		sr.valueBlockBuilder.CloseEntry()
	} else {
		sr.keyBlockBuilder.CloseEntry()
	}
	sr.entryAdded()
	return sr
}

func (sr *SingleMapBlockWriter) entryAdded() {
	sr.writeToValueNext = !sr.writeToValueNext
	sr.positionsWritten++
}

// @Override
func (sr *SingleMapBlockWriter) GetPositionCount() int32 {
	return sr.positionsWritten
}

// @Override
func (sr *SingleMapBlockWriter) Build() Block {
	panic("Unsupported")
}

// @Override
func (sr *SingleMapBlockWriter) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	panic("Unsupported")
}

// @Override
func (sr *SingleMapBlockWriter) String() string {
	return fmt.Sprintf("SingleMapBlockWriter{positionCount=%d}", sr.GetPositionCount())
}

// / 继承类 Block
// @Override
func (sr *SingleMapBlockWriter) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return sr.AbstractSingleMapBlock.BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (sr *SingleMapBlockWriter) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return sr.AbstractSingleMapBlock.BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (sr *SingleMapBlockWriter) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return sr.AbstractSingleMapBlock.CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (sr *SingleMapBlockWriter) CopyPositions(positions []int32, offset int32, length int32) Block {
	return sr.AbstractSingleMapBlock.CopyPositions(positions, offset, length)
}

// @Override
func (sr *SingleMapBlockWriter) CopyRegion(position int32, length int32) Block {
	return sr.AbstractSingleMapBlock.CopyRegion(position, length)
}

// @Override
func (sr *SingleMapBlockWriter) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return sr.AbstractSingleMapBlock.Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (sr *SingleMapBlockWriter) GetChildren() *util.ArrayList[Block] {
	return sr.AbstractSingleMapBlock.GetChildren()
}

// @Override
func (sr *SingleMapBlockWriter) GetEstimatedDataSizeForStats(position int32) int64 {
	return sr.AbstractSingleMapBlock.GetEstimatedDataSizeForStats(position)
}

// @Override
func (sr *SingleMapBlockWriter) GetByte(position int32, offset int32) byte {
	return sr.AbstractSingleMapBlock.GetByte(position, offset)
}

// @Override
func (sr *SingleMapBlockWriter) GetShort(position int32, offset int32) int16 {
	return sr.AbstractSingleMapBlock.GetShort(position, offset)
}

// @Override
func (sr *SingleMapBlockWriter) GetInt(position int32, offset int32) int32 {
	return sr.AbstractSingleMapBlock.GetInt(position, offset)
}

// @Override
func (sr *SingleMapBlockWriter) GetLong(position int32, offset int32) int64 {
	return sr.AbstractSingleMapBlock.GetLong(position, offset)
}

// @Override
func (sr *SingleMapBlockWriter) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return sr.AbstractSingleMapBlock.GetSlice(position, offset, length)
}

// @Override
func (sr *SingleMapBlockWriter) GetObject(position int32, clazz reflect.Type) basic.Object {
	return sr.AbstractSingleMapBlock.GetObject(position, clazz)
}

// @Override
func (sr *SingleMapBlockWriter) GetPositionsSizeInBytes(positions []bool) int64 {
	return sr.AbstractSingleMapBlock.GetPositionsSizeInBytes(positions)
}

// @Override
func (sr *SingleMapBlockWriter) GetRegion(positionOffset int32, length int32) Block {
	return sr.AbstractSingleMapBlock.GetRegion(positionOffset, length)
}

// @Override
func (sr *SingleMapBlockWriter) GetRegionSizeInBytes(position int32, length int32) int64 {
	return sr.AbstractSingleMapBlock.GetRegionSizeInBytes(position, length)
}

// @Override
func (sr *SingleMapBlockWriter) GetSingleValueBlock(position int32) Block {
	return sr.AbstractSingleMapBlock.GetSingleValueBlock(position)
}

// @Override
func (sr *SingleMapBlockWriter) GetSliceLength(position int32) int32 {
	return sr.AbstractSingleMapBlock.GetSliceLength(position)
}

// @Override
func (sr *SingleMapBlockWriter) Hash(position int32, offset int32, length int32) int64 {
	return sr.AbstractSingleMapBlock.Hash(position, offset, length)
}

// @Override
func (sr *SingleMapBlockWriter) IsNull(position int32) bool {
	return sr.AbstractSingleMapBlock.IsNull(position)
}

// @Override
func (sr *SingleMapBlockWriter) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	sr.AbstractSingleMapBlock.WriteBytesTo(position, offset, length, blockBuilder)
}
func (ak *SingleMapBlockWriter) GetLoadedBlock() Block {
	return ak
}

func (ik *SingleMapBlockWriter) IsLoaded() bool {
	return true
}
