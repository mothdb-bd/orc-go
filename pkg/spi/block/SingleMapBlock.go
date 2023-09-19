package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var SINGLE_MAP_BLOCK_INSTANCE_SIZE int32 = util.SizeOf(&SingleMapBlock{})

type SingleMapBlock struct {
	// 继承
	AbstractSingleMapBlock
	// 继承
	BlockBuilder

	offset        int32
	positionCount int32
	mapBlock      *AbstractMapBlock
}

func NewSingleMapBlock(offset int32, positionCount int32, mapBlock *AbstractMapBlock) *SingleMapBlock {
	sk := new(SingleMapBlock)
	sk.offset = offset
	sk.positionCount = positionCount
	sk.mapBlock = mapBlock
	return sk
}

func (sk *SingleMapBlock) GetMapType() Type {
	return sk.mapBlock.getMapType()
}

// @Override
func (sk *SingleMapBlock) GetPositionCount() int32 {
	return sk.positionCount
}

// @Override
func (sk *SingleMapBlock) GetSizeInBytes() int64 {
	return sk.mapBlock.getRawKeyBlock().GetRegionSizeInBytes(sk.offset/2, sk.positionCount/2) + sk.mapBlock.getRawValueBlock().GetRegionSizeInBytes(sk.offset/2, sk.positionCount/2) + int64(util.BYTE_BYTES*(sk.positionCount/2*MHT_HASH_MULTIPLIER))
}

// @Override
func (sk *SingleMapBlock) GetRetainedSizeInBytes() int64 {
	return int64(SINGLE_MAP_BLOCK_INSTANCE_SIZE) + sk.mapBlock.GetRetainedSizeInBytes()
}

// @Override
func (sk *SingleMapBlock) GetOffset() int32 {
	return sk.offset
}

// @Override
func (sk *SingleMapBlock) getRawKeyBlock() Block {
	return sk.mapBlock.getRawKeyBlock()
}

// @Override
func (sk *SingleMapBlock) getRawValueBlock() Block {
	return sk.mapBlock.getRawValueBlock()
}

// @Override
func (sk *SingleMapBlock) ToString() string {
	return fmt.Sprintf("SingleMapBlock{positionCount=%d}", sk.GetPositionCount())
}

// @Override
func (sk *SingleMapBlock) IsLoaded() bool {
	return sk.mapBlock.getRawKeyBlock().IsLoaded() && sk.mapBlock.getRawValueBlock().IsLoaded()
}

// @Override
func (sk *SingleMapBlock) GetLoadedBlock() Block {
	if sk.mapBlock.getRawKeyBlock() != sk.mapBlock.getRawKeyBlock().GetLoadedBlock() {
		panic("Illegal")
	}
	loadedValueBlock := sk.mapBlock.getRawValueBlock().GetLoadedBlock()
	if loadedValueBlock == sk.mapBlock.getRawValueBlock() {
		return sk
	}
	return NewSingleMapBlock(sk.offset, sk.positionCount, sk.mapBlock)
}

func (sk *SingleMapBlock) TryGetHashTable() *optional.Optional[[]int32] {
	return sk.mapBlock.getHashTables().tryGet()
}

// / 继承类 Block
// @Override
func (sr *SingleMapBlock) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return sr.AbstractSingleMapBlock.BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (sr *SingleMapBlock) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return sr.AbstractSingleMapBlock.BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (sr *SingleMapBlock) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return sr.AbstractSingleMapBlock.CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (sr *SingleMapBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	return sr.AbstractSingleMapBlock.CopyPositions(positions, offset, length)
}

// @Override
func (sr *SingleMapBlock) CopyRegion(position int32, length int32) Block {
	return sr.AbstractSingleMapBlock.CopyRegion(position, length)
}

// @Override
func (sr *SingleMapBlock) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return sr.AbstractSingleMapBlock.Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (sr *SingleMapBlock) GetChildren() *util.ArrayList[Block] {
	return sr.AbstractSingleMapBlock.GetChildren()
}

// @Override
func (sr *SingleMapBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return sr.AbstractSingleMapBlock.GetEstimatedDataSizeForStats(position)
}

// @Override
func (sr *SingleMapBlock) GetByte(position int32, offset int32) byte {
	return sr.AbstractSingleMapBlock.GetByte(position, offset)
}

// @Override
func (sr *SingleMapBlock) GetShort(position int32, offset int32) int16 {
	return sr.AbstractSingleMapBlock.GetShort(position, offset)
}

// @Override
func (sr *SingleMapBlock) GetInt(position int32, offset int32) int32 {
	return sr.AbstractSingleMapBlock.GetInt(position, offset)
}

// @Override
func (sr *SingleMapBlock) GetLong(position int32, offset int32) int64 {
	return sr.AbstractSingleMapBlock.GetLong(position, offset)
}

// @Override
func (sr *SingleMapBlock) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return sr.AbstractSingleMapBlock.GetSlice(position, offset, length)
}

// @Override
func (sr *SingleMapBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	return sr.AbstractSingleMapBlock.GetObject(position, clazz)
}

// @Override
func (sr *SingleMapBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return sr.AbstractSingleMapBlock.GetPositionsSizeInBytes(positions)
}

// @Override
func (sr *SingleMapBlock) GetRegion(positionOffset int32, length int32) Block {
	return sr.AbstractSingleMapBlock.GetRegion(positionOffset, length)
}

// @Override
func (sr *SingleMapBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	return sr.AbstractSingleMapBlock.GetRegionSizeInBytes(position, length)
}

// @Override
func (sr *SingleMapBlock) GetSingleValueBlock(position int32) Block {
	return sr.AbstractSingleMapBlock.GetSingleValueBlock(position)
}

// @Override
func (sr *SingleMapBlock) GetSliceLength(position int32) int32 {
	return sr.AbstractSingleMapBlock.GetSliceLength(position)
}

// @Override
func (sr *SingleMapBlock) Hash(position int32, offset int32, length int32) int64 {
	return sr.AbstractSingleMapBlock.Hash(position, offset, length)
}

// @Override
func (sr *SingleMapBlock) IsNull(position int32) bool {
	return sr.AbstractSingleMapBlock.IsNull(position)
}

// @Override
func (sr *SingleMapBlock) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	sr.AbstractSingleMapBlock.WriteBytesTo(position, offset, length, blockBuilder)
}
