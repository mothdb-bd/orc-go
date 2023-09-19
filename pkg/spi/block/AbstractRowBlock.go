package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractRowBlock struct {
	Block // 继承block

	numFields int32
}

// abstract
func (r *AbstractRowBlock) getRawFieldBlocks() []Block {
	return nil
}

func (r *AbstractRowBlock) getFieldBlockOffsets() []int32 {
	return nil
}

func (r *AbstractRowBlock) getOffsetBase() int32 {
	return 0
}

/**
 * @return the underlying rowIsNull array, or null when all rows are guaranteed to be non-null
 */
func (r *AbstractRowBlock) getRowIsNull() []bool {
	return nil
}

// @Override
func (ak *AbstractRowBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawFieldBlocks()...)
}

func (ak *AbstractRowBlock) getFieldBlockOffset(position int32) int32 {
	offsets := ak.getFieldBlockOffsets()
	return util.If(offsets != nil, offsets[position+ak.getOffsetBase()], position+ak.getOffsetBase()).(int32)
}
func NewAbstractRowBlock(numFields int32) *AbstractRowBlock {
	ak := new(AbstractRowBlock)
	if numFields <= 0 {
		panic("Number of fields in RowBlock must be positive")
	}
	ak.numFields = numFields
	return ak
}

// @Override
func (ak *AbstractRowBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newOffsets []int32 = nil
	fieldBlockPositions := make([]int32, length)
	var fieldBlockPositionCount int32
	var newRowIsNull []bool
	if ak.getRowIsNull() == nil {
		newRowIsNull = nil
		var i int32
		for i = 0; i < int32(len(fieldBlockPositions)); i++ {
			position := positions[offset+i]
			ak.checkReadablePosition(position)
			fieldBlockPositions[i] = ak.getFieldBlockOffset(position)
		}
		fieldBlockPositionCount = int32(len(fieldBlockPositions))
	} else {
		newRowIsNull = make([]bool, length)
		newOffsets = make([]int32, length+1)
		fieldBlockPositionCount = 0
		var i int32
		for i = 0; i < length; i++ {
			newOffsets[i] = fieldBlockPositionCount
			position := positions[offset+i]
			if ak.IsNull(position) {
				newRowIsNull[i] = true
			} else {
				fieldBlockPositions[fieldBlockPositionCount] = ak.getFieldBlockOffset(position)
				fieldBlockPositionCount++
			}
		}
		newOffsets[length] = fieldBlockPositionCount
		if fieldBlockPositionCount == length {
			newRowIsNull = nil
			newOffsets = nil
		}
	}
	newBlocks := make([]Block, ak.numFields)
	rawBlocks := ak.getRawFieldBlocks()
	for i := 0; i < len(newBlocks); i++ {
		newBlocks[i] = rawBlocks[i].CopyPositions(fieldBlockPositions, 0, fieldBlockPositionCount)
	}
	return CreateRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks)
}

// @Override
func (ak *AbstractRowBlock) GetRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	return CreateRowBlockInternal(position+ak.getOffsetBase(), length, ak.getRowIsNull(), ak.getFieldBlockOffsets(), ak.getRawFieldBlocks())
}

// @Override
func (ak *AbstractRowBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	startFieldBlockOffset := ak.getFieldBlockOffset(position)
	endFieldBlockOffset := ak.getFieldBlockOffset(position + length)
	fieldBlockLength := endFieldBlockOffset - startFieldBlockOffset
	regionSizeInBytes := int64(INT32_BYTE_BYTES * length)
	var i int32
	for i = 0; i < ak.numFields; i++ {
		regionSizeInBytes += ak.getRawFieldBlocks()[i].GetRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength)
	}
	return regionSizeInBytes
}

// @Override
func (ak *AbstractRowBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	checkValidPositions(positions, ak.GetPositionCount())
	var usedPositionCount int32 = 0
	fieldPositions := make([]bool, ak.getRawFieldBlocks()[0].GetPositionCount())
	for i := 0; i < len(positions); i++ {
		if positions[i] {
			usedPositionCount++
			startFieldBlockOffset := ak.getFieldBlockOffset(int32(i))
			endFieldBlockOffset := ak.getFieldBlockOffset(int32(i + 1))
			for j := startFieldBlockOffset; j < endFieldBlockOffset; j++ {
				fieldPositions[j] = true
			}
		}
	}
	var sizeInBytes int64 = 0
	var j int32
	for j = 0; j < ak.numFields; j++ {
		sizeInBytes += ak.getRawFieldBlocks()[j].GetPositionsSizeInBytes(fieldPositions)
	}
	return sizeInBytes + int64(INT32_BYTE_BYTES*usedPositionCount)
}

// @Override
func (ak *AbstractRowBlock) CopyRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	startFieldBlockOffset := ak.getFieldBlockOffset(position)
	endFieldBlockOffset := ak.getFieldBlockOffset(position + length)
	fieldBlockLength := endFieldBlockOffset - startFieldBlockOffset
	newBlocks := make([]Block, ak.numFields)
	var i int32
	for i = 0; i < ak.numFields; i++ {
		newBlocks[i] = ak.getRawFieldBlocks()[i].CopyRegion(startFieldBlockOffset, fieldBlockLength)
	}
	fieldBlockOffsets := ak.getFieldBlockOffsets()
	newOffsets := util.If(fieldBlockOffsets == nil, nil, compactOffsets(fieldBlockOffsets, position+ak.getOffsetBase(), length)).([]int32)
	rowIsNull := ak.getRowIsNull()
	newRowIsNull := util.If(rowIsNull == nil, nil, compactBoolArray(rowIsNull, position+ak.getOffsetBase(), length)).([]bool)

	if blockArraySame(newBlocks, ak.getRawFieldBlocks()) && basic.ObjectEqual(newOffsets, fieldBlockOffsets) && basic.ObjectEqual(newRowIsNull, rowIsNull) {
		return ak
	}
	return CreateRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks)
}

// @Override
func (ak *AbstractRowBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	if clazz != BLOCK_TYPE {
		panic("clazz must be Block type")
	}
	ak.checkReadablePosition(position)
	return NewSingleRowBlock(ak.getFieldBlockOffset(position), ak.getRawFieldBlocks())
}

// @Override
func (ak *AbstractRowBlock) GetSingleValueBlock(position int32) Block {
	ak.checkReadablePosition(position)
	startFieldBlockOffset := ak.getFieldBlockOffset(position)
	endFieldBlockOffset := ak.getFieldBlockOffset(position + 1)
	fieldBlockLength := endFieldBlockOffset - startFieldBlockOffset
	newBlocks := make([]Block, ak.numFields)
	var i int32
	for i = 0; i < ak.numFields; i++ {
		newBlocks[i] = ak.getRawFieldBlocks()[i].CopyRegion(startFieldBlockOffset, fieldBlockLength)
	}
	newRowIsNull := util.If(ak.IsNull(position), []bool{true}, nil).([]bool)
	newOffsets := util.If(ak.IsNull(position), []int32{0, fieldBlockLength}, nil).([]int32)
	return CreateRowBlockInternal(0, 1, newRowIsNull, newOffsets, newBlocks)
}

// @Override
func (ak *AbstractRowBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	ak.checkReadablePosition(position)
	if ak.IsNull(position) {
		return 0
	}
	rawFieldBlocks := ak.getRawFieldBlocks()
	var size int64 = 0
	var i int32
	for i = 0; i < ak.numFields; i++ {
		size += rawFieldBlocks[i].GetEstimatedDataSizeForStats(ak.getFieldBlockOffset(position))
	}
	return size
}

// @Override
func (ak *AbstractRowBlock) IsNull(position int32) bool {
	ak.checkReadablePosition(position)
	rowIsNull := ak.getRowIsNull()
	return rowIsNull != nil && rowIsNull[position+ak.getOffsetBase()]
}

func (ak *AbstractRowBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ak.GetPositionCount() {
		panic("position is not valid")
	}
}
