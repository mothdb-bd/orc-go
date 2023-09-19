package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractMapBlock struct {
	Block // 继承block

	mapType *MapType
}

func NewAbstractMapBlock(mapType *MapType) *AbstractMapBlock {
	ak := new(AbstractMapBlock)
	ak.mapType = mapType
	return ak
}

// protected abstract Block getRawKeyBlock();
func (b *AbstractMapBlock) getRawKeyBlock() Block {
	return nil
}

// protected abstract Block getRawValueBlock();
func (b *AbstractMapBlock) getRawValueBlock() Block {
	return nil
}

// protected abstract MapHashTables getHashTables();
func (b *AbstractMapBlock) getHashTables() *MapHashTables {
	return nil
}

/**
 * offset is entry-based, not position-based. In other words,
 * if offset[1] is 6, it means the first map has 6 key-value pairs,
 * not 6 key/values (which would be 3 pairs).
 */
// protected abstract int[] getOffsets();
func (b *AbstractMapBlock) getOffsets() []int32 {
	return nil
}

/**
 * offset is entry-based, not position-based. (see getOffsets)
 */
// protected abstract int getOffsetBase();
func (b *AbstractMapBlock) getOffsetBase() int32 {
	return 0
}

// @Nullable
// protected abstract boolean[] getMapIsNull();
func (b *AbstractMapBlock) getMapIsNull() []bool {
	return nil
}

// protected abstract void ensureHashTableLoaded();
func (b *AbstractMapBlock) ensureHashTableLoaded() {
}

// @Override
func (ak *AbstractMapBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawKeyBlock(), ak.getRawValueBlock())
}

func (ak *AbstractMapBlock) getMapType() *MapType {
	return ak.mapType
}

func (ak *AbstractMapBlock) getOffset(position int32) int32 {
	return ak.getOffsets()[position+ak.getOffsetBase()]
}

// @Override
func (ak *AbstractMapBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	newOffsets := make([]int32, length+1)
	newMapIsNull := make([]bool, length)
	entriesPositions := NewIntArrayList2()
	newPosition := 0
	for i := offset; i < offset+length; i++ {
		position := positions[i]
		if ak.IsNull(position) {
			newMapIsNull[newPosition] = true
			newOffsets[newPosition+1] = newOffsets[newPosition]
		} else {
			entriesStartOffset := ak.getOffset(position)
			entriesEndOffset := ak.getOffset(position + 1)
			entryCount := entriesEndOffset - entriesStartOffset
			newOffsets[newPosition+1] = newOffsets[newPosition] + entryCount
			for elementIndex := entriesStartOffset; elementIndex < entriesEndOffset; elementIndex++ {
				entriesPositions.Add(elementIndex)
			}
		}
		newPosition++
	}
	rawHashTables := ak.getHashTables().tryGet().OrElse(nil)
	var newRawHashTables []int32 = nil
	newHashTableEntries := newOffsets[len(newOffsets)-1] * MHT_HASH_MULTIPLIER
	if rawHashTables != nil {
		newRawHashTables = make([]int32, newHashTableEntries)
		newHashIndex := 0
		for i := offset; i < (offset + length); i++ {
			position := positions[i]
			entriesStartOffset := ak.getOffset(position)
			entriesEndOffset := ak.getOffset(position + 1)
			for hashIndex := entriesStartOffset * MHT_HASH_MULTIPLIER; hashIndex < entriesEndOffset*MHT_HASH_MULTIPLIER; hashIndex++ {
				newRawHashTables[newHashIndex] = rawHashTables[hashIndex]
				newHashIndex++
			}
		}
	}
	newKeys := ak.getRawKeyBlock().CopyPositions(entriesPositions.Elements(), 0, entriesPositions.Size())
	newValues := ak.getRawValueBlock().CopyPositions(entriesPositions.Elements(), 0, entriesPositions.Size())
	return CreateMapBlockInternal2(ak.mapType, 0, length, optional.Of(newMapIsNull), newOffsets, newKeys, newValues, NewMapHashTables(ak.mapType, optional.Of(newRawHashTables)))
}

// @Override
func (ak *AbstractMapBlock) GetRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	return CreateMapBlockInternal2(ak.mapType, position+ak.getOffsetBase(), length, optional.Of(ak.getMapIsNull()), ak.getOffsets(), ak.getRawKeyBlock(), ak.getRawValueBlock(), ak.getHashTables())
}

// @Override
func (ak *AbstractMapBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	entriesStart := ak.getOffsets()[ak.getOffsetBase()+position]
	entriesEnd := ak.getOffsets()[ak.getOffsetBase()+position+length]
	entryCount := entriesEnd - entriesStart
	return ak.getRawKeyBlock().GetRegionSizeInBytes(entriesStart, entryCount) + ak.getRawValueBlock().GetRegionSizeInBytes(entriesStart, entryCount) + int64(INT32_BYTE_BYTES*length+util.INT32_BYTES*MHT_HASH_MULTIPLIER*entryCount)
}

// @Override
func (ak *AbstractMapBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	positionCount := ak.GetPositionCount()
	checkValidPositions(positions, positionCount)
	entryPositions := make([]bool, ak.getRawKeyBlock().GetPositionCount())
	var usedEntryCount int32 = 0
	var usedPositionCount int32 = 0
	var i int32
	for i = 0; i < int32(len(positions)); i++ {
		if positions[i] {
			usedPositionCount++
			entriesStart := ak.getOffsets()[ak.getOffsetBase()+i]
			entriesEnd := ak.getOffsets()[ak.getOffsetBase()+i+1]
			for j := entriesStart; j < entriesEnd; j++ {
				entryPositions[j] = true
			}
			usedEntryCount += (entriesEnd - entriesStart)
		}
	}
	return ak.getRawKeyBlock().GetPositionsSizeInBytes(entryPositions) + ak.getRawValueBlock().GetPositionsSizeInBytes(entryPositions) + int64(INT32_BYTE_BYTES*usedPositionCount+util.INT32_BYTES*MHT_HASH_MULTIPLIER*usedEntryCount)
}

// @Override
func (ak *AbstractMapBlock) CopyRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + length)
	newKeys := ak.getRawKeyBlock().CopyRegion(startValueOffset, endValueOffset-startValueOffset)
	newValues := ak.getRawValueBlock().CopyRegion(startValueOffset, endValueOffset-startValueOffset)
	newOffsets := compactOffsets(ak.getOffsets(), position+ak.getOffsetBase(), length)
	mapIsNull := ak.getMapIsNull()

	var newMapIsNull []bool = nil
	if mapIsNull != nil {
		newMapIsNull = compactBoolArray(mapIsNull, position+ak.getOffsetBase(), length)
	}
	rawHashTables := ak.getHashTables().tryGet().OrElse(nil)
	var newRawHashTables []int32 = nil
	expectedNewHashTableEntries := (endValueOffset - startValueOffset) * MHT_HASH_MULTIPLIER
	if rawHashTables != nil {
		newRawHashTables = compactInt32Array(rawHashTables, startValueOffset*MHT_HASH_MULTIPLIER, expectedNewHashTableEntries)
	}
	if reflect.ValueOf(newKeys).UnsafeAddr() == reflect.ValueOf(ak.getRawKeyBlock()).UnsafeAddr() && reflect.ValueOf(newValues).UnsafeAddr() == reflect.ValueOf(ak.getRawValueBlock()).UnsafeAddr() && reflect.ValueOf(newOffsets).UnsafeAddr() == reflect.ValueOf(ak.getOffsets()).UnsafeAddr() && reflect.ValueOf(newMapIsNull).UnsafeAddr() == reflect.ValueOf(mapIsNull).UnsafeAddr() && reflect.ValueOf(newRawHashTables).UnsafeAddr() == reflect.ValueOf(rawHashTables).UnsafeAddr() {
		return ak
	}
	return CreateMapBlockInternal2(ak.mapType, 0, length, optional.Of(newMapIsNull), newOffsets, newKeys, newValues, NewMapHashTables(ak.mapType, optional.Of(newRawHashTables)))
}

// @Override
func (ak *AbstractMapBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	if clazz != BLOCK_TYPE {
		panic("clazz must be Block.class")
	}
	ak.checkReadablePosition(position)
	startEntryOffset := ak.getOffset(position)
	endEntryOffset := ak.getOffset(position + 1)
	return NewSingleMapBlock(startEntryOffset*2, (endEntryOffset-startEntryOffset)*2, ak)
}

// @Override
func (ak *AbstractMapBlock) GetSingleValueBlock(position int32) Block {
	ak.checkReadablePosition(position)
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	valueLength := endValueOffset - startValueOffset
	newKeys := ak.getRawKeyBlock().CopyRegion(startValueOffset, valueLength)
	newValues := ak.getRawValueBlock().CopyRegion(startValueOffset, valueLength)
	rawHashTables := ak.getHashTables().tryGet().OrElse(nil)
	var newRawHashTables []int32 = nil
	if rawHashTables != nil {
		newRawHashTables = util.CopyInt32sOfRange(rawHashTables, startValueOffset*MHT_HASH_MULTIPLIER, endValueOffset*MHT_HASH_MULTIPLIER)
	}
	return CreateMapBlockInternal2(ak.mapType, 0, 1, optional.Of([]bool{ak.IsNull(position)}), []int32{0, valueLength}, newKeys, newValues, NewMapHashTables(ak.mapType, optional.Of(newRawHashTables)))
}

// @Override
func (ak *AbstractMapBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	ak.checkReadablePosition(position)
	if ak.IsNull(position) {
		return 0
	}
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	size := int64(0)
	rawKeyBlock := ak.getRawKeyBlock()
	rawValueBlock := ak.getRawValueBlock()
	for i := startValueOffset; i < endValueOffset; i++ {
		size += rawKeyBlock.GetEstimatedDataSizeForStats(i)
		size += rawValueBlock.GetEstimatedDataSizeForStats(i)
	}
	return size
}

// @Override
func (ak *AbstractMapBlock) IsNull(position int32) bool {
	ak.checkReadablePosition(position)
	mapIsNull := ak.getMapIsNull()
	return mapIsNull != nil && mapIsNull[position+ak.getOffsetBase()]
}

func (ak *AbstractMapBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ak.GetPositionCount() {
		panic("position is not valid")
	}
}
