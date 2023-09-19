package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MapBlock struct {
	// 继承
	AbstractMapBlock

	startOffset         int32
	positionCount       int32
	mapIsNull           []bool
	offsets             []int32
	keyBlock            Block
	valueBlock          Block
	hashTables          *MapHashTables
	baseSizeInBytes     int64
	valueSizeInBytes    int64
	retainedSizeInBytes int64
}

// var mapBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(MapBlock.class).instanceSize()
var MAP_INSTANCE_SIZE = util.SizeOf(&MapBlock{})

func FromKeyValueBlock(mapIsNull *optional.Optional[[]bool], offsets []int32, keyBlock Block, valueBlock Block, mapType *MapType) *MapBlock {
	validateConstructorArguments(mapType, 0, util.Int32sLenInt32(offsets)-1, mapIsNull.OrElse(nil), offsets, keyBlock, valueBlock)
	mapCount := util.Int32sLenInt32(offsets) - 1
	return CreateMapBlockInternal2(mapType, 0, mapCount, mapIsNull, offsets, keyBlock, valueBlock, NewMapHashTables(mapType, optional.Empty[[]int32]()))
}

func CreateMapBlockInternal2(mapType *MapType, startOffset int32, positionCount int32, mapIsNull *optional.Optional[[]bool], offsets []int32, keyBlock Block, valueBlock Block, hashTables *MapHashTables) *MapBlock {
	validateConstructorArguments(mapType, startOffset, positionCount, mapIsNull.OrElse(nil), offsets, keyBlock, valueBlock)
	return NewMapBlock(mapType, startOffset, positionCount, mapIsNull.OrElse(nil), offsets, keyBlock, valueBlock, hashTables)
}

func validateConstructorArguments(mapType *MapType, startOffset int32, positionCount int32, mapIsNull []bool, offsets []int32, keyBlock Block, valueBlock Block) {
	if startOffset < 0 {
		panic("startOffset is negative")
	}
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	if mapIsNull != nil && util.BoolsLenInt32(mapIsNull)-startOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	if util.Int32sLenInt32(offsets)-startOffset < positionCount+1 {
		panic("offsets length is less than positionCount")
	}
	if keyBlock.GetPositionCount() != valueBlock.GetPositionCount() {
		panic(fmt.Sprintf("keyBlock and valueBlock has different size: %d %d", keyBlock.GetPositionCount(), valueBlock.GetPositionCount()))
	}
}
func NewMapBlock(mapType *MapType, startOffset int32, positionCount int32, mapIsNull []bool, offsets []int32, keyBlock Block, valueBlock Block, hashTables *MapHashTables) *MapBlock {
	mk := new(MapBlock)
	mk.mapType = mapType
	rawHashTables := hashTables.tryGet().OrElse(nil)
	if rawHashTables != nil && util.Int32sLenInt32(rawHashTables) < keyBlock.GetPositionCount()*MHT_HASH_MULTIPLIER {
		panic(fmt.Sprintf("keyBlock/valueBlock size does not match hash table size: %d %d", keyBlock.GetPositionCount(), util.Int32sLenInt32(rawHashTables)))
	}
	mk.startOffset = startOffset
	mk.positionCount = positionCount
	mk.mapIsNull = mapIsNull
	mk.offsets = offsets
	mk.keyBlock = keyBlock
	mk.valueBlock = valueBlock
	mk.hashTables = hashTables
	entryCount := offsets[startOffset+positionCount] - offsets[startOffset]
	mk.baseSizeInBytes = int64(util.INT32_BYTES*MHT_HASH_MULTIPLIER*entryCount+(util.INT32_BYTES+util.BYTE_BYTES)*positionCount) + mk.calculateSize(keyBlock)
	mk.retainedSizeInBytes = int64(MAP_INSTANCE_SIZE + util.SizeOf(offsets) + util.SizeOf(mapIsNull))
	return mk
}

// @Override
func (mk *MapBlock) getRawKeyBlock() Block {
	return mk.keyBlock
}

// @Override
func (mk *MapBlock) getRawValueBlock() Block {
	return mk.valueBlock
}

// @Override
func (mk *MapBlock) getHashTables() *MapHashTables {
	return mk.hashTables
}

// @Override
func (mk *MapBlock) getOffsets() []int32 {
	return mk.offsets
}

// @Override
func (mk *MapBlock) getOffsetBase() int32 {
	return mk.startOffset
}

// @Override
// @Nullable
func (mk *MapBlock) getMapIsNull() []bool {
	return mk.mapIsNull
}

// @Override
func (mk *MapBlock) GetPositionCount() int32 {
	return mk.positionCount
}

// @Override
func (mk *MapBlock) GetSizeInBytes() int64 {
	if mk.valueSizeInBytes < 0 {
		if !mk.valueBlock.IsLoaded() {
			return mk.baseSizeInBytes + mk.valueBlock.GetSizeInBytes()
		}
		mk.valueSizeInBytes = mk.calculateSize(mk.valueBlock)
	}
	return mk.baseSizeInBytes + mk.valueSizeInBytes
}

func (mk *MapBlock) calculateSize(block Block) int64 {
	entriesStart := mk.offsets[mk.startOffset]
	entriesEnd := mk.offsets[mk.startOffset+mk.positionCount]
	entryCount := entriesEnd - entriesStart
	return block.GetRegionSizeInBytes(entriesStart, entryCount)
}

// @Override
func (mk *MapBlock) GetRetainedSizeInBytes() int64 {
	return mk.retainedSizeInBytes + mk.keyBlock.GetRetainedSizeInBytes() + mk.valueBlock.GetRetainedSizeInBytes() + mk.hashTables.GetRetainedSizeInBytes()
}

// @Override
func (mk *MapBlock) IsLoaded() bool {
	return mk.keyBlock.IsLoaded() && mk.valueBlock.IsLoaded()
}

// @Override
func (mk *MapBlock) GetLoadedBlock() Block {
	if mk.keyBlock != mk.keyBlock.GetLoadedBlock() {
		panic("Block is not equal")
	}
	loadedValueBlock := mk.valueBlock.GetLoadedBlock()
	if loadedValueBlock == mk.valueBlock {
		return mk
	}
	return CreateMapBlockInternal2(mk.getMapType(), mk.startOffset, mk.positionCount, optional.Of(mk.mapIsNull), mk.offsets, mk.keyBlock, loadedValueBlock, mk.hashTables)
}

// @Override
func (mk *MapBlock) ensureHashTableLoaded() {
	mk.hashTables.buildAllHashTablesIfNecessary(mk.getRawKeyBlock(), mk.offsets, mk.mapIsNull)
}
func (ik *MapBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
