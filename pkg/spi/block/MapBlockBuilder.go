package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MapBlockBuilder struct { //@Nullable
	// 继承
	BlockBuilder
	// 继承
	AbstractMapBlock

	blockBuilderStatus *BlockBuilderStatus
	positionCount      int32
	offsets            []int32
	mapIsNull          []bool
	keyBlockBuilder    BlockBuilder
	valueBlockBuilder  BlockBuilder
	hashTables         *MapHashTables
	currentEntryOpened bool
	strict             bool
}

// var mapBlockBuilderiNSTANCE_SIZE int32 = ClassLayout.parseClass(MapBlockBuilder.class).instanceSize()

var (
	MAP_BB_INSTANCE_SIZE int32 = util.SizeOf(&MapBlockBuilder{})
)

func NewMapBlockBuilder(mapType *MapType, blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *MapBlockBuilder {
	kbb := mapType.GetKeyType().CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
	vbb := mapType.GetValueType().CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
	return NewMapBlockBuilder2(mapType, blockBuilderStatus, kbb, vbb, make([]int32, expectedEntries+1), make([]bool, expectedEntries))
}
func NewMapBlockBuilder2(mapType *MapType, blockBuilderStatus *BlockBuilderStatus, keyBlockBuilder BlockBuilder, valueBlockBuilder BlockBuilder, offsets []int32, mapIsNull []bool) *MapBlockBuilder {
	mr := new(MapBlockBuilder)
	// NewMapBlockBuilder(mapType)
	mr.mapType = mapType

	mr.blockBuilderStatus = blockBuilderStatus
	mr.positionCount = 0
	mr.offsets = offsets
	mr.mapIsNull = mapIsNull
	mr.keyBlockBuilder = keyBlockBuilder
	mr.valueBlockBuilder = valueBlockBuilder
	hashTable := make([]int32, util.BoolsLenInt32(mapIsNull)*MHT_HASH_MULTIPLIER)
	util.FillInt32s(hashTable, -1)
	mr.hashTables = NewMapHashTables(mapType, optional.Of(hashTable))

	return mr
}

func (mr *MapBlockBuilder) Strict() *MapBlockBuilder {
	mr.strict = true
	return mr
}

// @Override
func (mr *MapBlockBuilder) getRawKeyBlock() Block {
	return mr.keyBlockBuilder
}

// @Override
func (mr *MapBlockBuilder) getRawValueBlock() Block {
	return mr.valueBlockBuilder
}

// @Override
func (mr *MapBlockBuilder) getHashTables() *MapHashTables {
	return mr.hashTables
}

// @Override
func (mr *MapBlockBuilder) getOffsets() []int32 {
	return mr.offsets
}

// @Override
func (mr *MapBlockBuilder) getOffsetBase() int32 {
	return 0
}

// @Override
func (mr *MapBlockBuilder) getMapIsNull() []bool {
	return mr.mapIsNull
}

// @Override
func (mr *MapBlockBuilder) GetPositionCount() int32 {
	return mr.positionCount
}

// @Override
func (mr *MapBlockBuilder) GetSizeInBytes() int64 {
	return mr.keyBlockBuilder.GetSizeInBytes() + mr.valueBlockBuilder.GetSizeInBytes() + int64((util.INT32_BYTES+util.BYTE_BYTES)*mr.positionCount+util.INT32_BYTES*MHT_HASH_MULTIPLIER*mr.keyBlockBuilder.GetPositionCount())
}

// @Override
func (mr *MapBlockBuilder) GetRetainedSizeInBytes() int64 {
	size := int64(MAP_BB_INSTANCE_SIZE) + mr.keyBlockBuilder.GetRetainedSizeInBytes() + mr.valueBlockBuilder.GetRetainedSizeInBytes() + int64(util.SizeOf(mr.offsets)+util.SizeOf(mr.mapIsNull)) + mr.hashTables.GetRetainedSizeInBytes()
	if mr.blockBuilderStatus != nil {
		size += int64(BBSINSTANCE_SIZE)
	}
	return size
}

// @Override
func (mr *MapBlockBuilder) BeginBlockEntry() BlockBuilder {
	if mr.currentEntryOpened {
		panic("Expected current entry to be closed but was opened")
	}
	mr.currentEntryOpened = true
	return NewSingleMapBlockWriter(mr.keyBlockBuilder.GetPositionCount()*2, mr.keyBlockBuilder, mr.valueBlockBuilder, mr.Strict)
}

// @Override
func (mr *MapBlockBuilder) CloseEntry() BlockBuilder {
	if !mr.currentEntryOpened {
		panic("Expected entry to be opened but was closed")
	}
	mr.entryAdded(false)
	mr.currentEntryOpened = false
	mr.ensureHashTableSize()
	previousAggregatedEntryCount := mr.offsets[mr.positionCount-1]
	aggregatedEntryCount := mr.offsets[mr.positionCount]
	entryCount := aggregatedEntryCount - previousAggregatedEntryCount
	if mr.strict {
		mr.hashTables.buildHashTableStrict(mr.keyBlockBuilder, previousAggregatedEntryCount, entryCount)
	} else {
		mr.hashTables.buildHashTable(mr.keyBlockBuilder, previousAggregatedEntryCount, entryCount)
	}
	return mr
}

// @Deprecated
func (mr *MapBlockBuilder) CloseEntryStrict() {
	if !mr.currentEntryOpened {
		panic("Expected entry to be opened but was closed")
	}
	mr.entryAdded(false)
	mr.currentEntryOpened = false
	mr.ensureHashTableSize()
	previousAggregatedEntryCount := mr.offsets[mr.positionCount-1]
	aggregatedEntryCount := mr.offsets[mr.positionCount]
	entryCount := aggregatedEntryCount - previousAggregatedEntryCount
	mr.hashTables.buildHashTableStrict(mr.keyBlockBuilder, previousAggregatedEntryCount, entryCount)
}

// @Override
func (mr *MapBlockBuilder) AppendNull() BlockBuilder {
	if mr.currentEntryOpened {
		panic("Current entry must be closed before a null can be written")
	}
	mr.entryAdded(true)
	return mr
}

func (mr *MapBlockBuilder) entryAdded(isNull bool) {
	if mr.keyBlockBuilder.GetPositionCount() != mr.valueBlockBuilder.GetPositionCount() {
		panic(fmt.Sprintf("keyBlock and valueBlock has different size: %d %d", mr.keyBlockBuilder.GetPositionCount(), mr.valueBlockBuilder.GetPositionCount()))
	}
	if util.BoolsLenInt32(mr.mapIsNull) <= mr.positionCount {
		newSize := calculateNewArraySize(util.BoolsLenInt32(mr.mapIsNull))
		mr.mapIsNull = util.CopyOfBools(mr.mapIsNull, newSize)
		mr.offsets = util.CopyOfInt32(mr.offsets, newSize+1)
	}
	mr.offsets[mr.positionCount+1] = mr.keyBlockBuilder.GetPositionCount()
	mr.mapIsNull[mr.positionCount] = isNull
	mr.positionCount++
	if mr.blockBuilderStatus != nil {
		mr.blockBuilderStatus.AddBytes(util.INT32_BYTES + util.BYTE_BYTES)
		mr.blockBuilderStatus.AddBytes((mr.offsets[mr.positionCount] - mr.offsets[mr.positionCount-1]) * MHT_HASH_MULTIPLIER * util.INT32_BYTES)
	}
}

func (mr *MapBlockBuilder) ensureHashTableSize() {
	rawHashTables := mr.hashTables.get()
	if util.Int32sLenInt32(rawHashTables) < mr.offsets[mr.positionCount]*MHT_HASH_MULTIPLIER {
		newSize := calculateNewArraySize(mr.offsets[mr.positionCount] * MHT_HASH_MULTIPLIER)
		mr.hashTables.growHashTables(newSize)
	}
}

// @Override
func (mr *MapBlockBuilder) Build() Block {
	if mr.currentEntryOpened {
		panic("Current entry must be closed before the block can be built")
	}
	rawHashTables := mr.hashTables.get()
	hashTablesEntries := mr.offsets[mr.positionCount] * MHT_HASH_MULTIPLIER
	return CreateMapBlockInternal2(mr.getMapType(), 0, mr.positionCount, optional.Of(mr.mapIsNull), mr.offsets, mr.keyBlockBuilder.Build(), mr.valueBlockBuilder.Build(), NewMapHashTables(mr.getMapType(), optional.Of(util.CopyOfInt32(rawHashTables, hashTablesEntries))))
}

// @Override
func (mr *MapBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	newSize := calculateBlockResetSize(mr.GetPositionCount())
	return NewMapBlockBuilder2(mr.getMapType(), blockBuilderStatus, mr.keyBlockBuilder.NewBlockBuilderLike(blockBuilderStatus), mr.valueBlockBuilder.NewBlockBuilderLike(blockBuilderStatus), make([]int32, newSize+1), make([]bool, newSize))
}

// @Override
func (mr *MapBlockBuilder) ensureHashTableLoaded() {
}

// @Override
func (mr *MapBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	return mr.AbstractMapBlock.CopyPositions(positions, offset, length)
}

// @Override
func (mr *MapBlockBuilder) CopyRegion(position int32, length int32) Block {
	return mr.AbstractMapBlock.CopyRegion(position, length)
}

// @Override
func (mr *MapBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return mr.AbstractMapBlock.GetChildren()
}

// @Override
func (mr *MapBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return mr.AbstractMapBlock.GetEstimatedDataSizeForStats(position)
}

// @Override
func (mr *MapBlockBuilder) GetObject(position int32, clazz reflect.Type) basic.Object {
	return mr.AbstractMapBlock.GetObject(position, clazz)
}

// @Override
func (mr *MapBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	return mr.AbstractMapBlock.GetPositionsSizeInBytes(positions)
}

// @Override
func (mr *MapBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	return mr.AbstractMapBlock.GetRegion(positionOffset, length)
}

// @Override
func (mr *MapBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	return mr.AbstractMapBlock.GetRegionSizeInBytes(position, length)
}

// @Override
func (mr *MapBlockBuilder) GetSingleValueBlock(position int32) Block {
	return mr.AbstractMapBlock.GetSingleValueBlock(position)
}

// @Override
func (mr *MapBlockBuilder) IsNull(position int32) bool {
	return mr.AbstractMapBlock.IsNull(position)
}
func (ak *MapBlockBuilder) GetLoadedBlock() Block {
	return ak
}

func (ik *MapBlockBuilder) IsLoaded() bool {
	return true
}
