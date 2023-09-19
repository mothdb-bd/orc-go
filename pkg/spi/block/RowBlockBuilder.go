package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type RowBlockBuilder struct { //@Nullable
	// 继承
	BlockBuilder
	// 继承
	AbstractRowBlock

	blockBuilderStatus   *BlockBuilderStatus
	positionCount        int32
	fieldBlockOffsets    []int32
	rowIsNull            []bool
	fieldBlockBuilders   []BlockBuilder
	singleRowBlockWriter *SingleRowBlockWriter
	currentEntryOpened   bool
	hasNullRow           bool
}

// var rowBlockBuilderROW_BB_INSTANCE_SIZE int32 = ClassLayout.parseClass(RowBlockBuilder.class).instanceSize()
var ROW_BB_INSTANCE_SIZE int32 = util.SizeOf(&RowBlockBuilder{})

func NewRowBlockBuilder(fieldTypes *util.ArrayList[Type], blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *RowBlockBuilder {
	return NewRowBlockBuilder2(blockBuilderStatus, createFieldBlockBuilders(fieldTypes, blockBuilderStatus, expectedEntries), make([]int32, expectedEntries+1), make([]bool, expectedEntries))
}
func NewRowBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, fieldBlockBuilders []BlockBuilder, fieldBlockOffsets []int32, rowIsNull []bool) *RowBlockBuilder {
	rr := new(RowBlockBuilder)

	numFields := int32(len(fieldBlockBuilders))
	if numFields <= 0 {
		panic("Number of fields in RowBlock must be positive")
	}
	rr.numFields = numFields
	rr.blockBuilderStatus = blockBuilderStatus
	rr.positionCount = 0
	rr.fieldBlockOffsets = fieldBlockOffsets
	rr.rowIsNull = rowIsNull
	rr.fieldBlockBuilders = fieldBlockBuilders
	rr.singleRowBlockWriter = NewSingleRowBlockWriter(fieldBlockBuilders)
	return rr
}

func createFieldBlockBuilders(fieldTypes *util.ArrayList[Type], blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) []BlockBuilder {
	fieldBlockBuilders := make([]BlockBuilder, fieldTypes.Size())
	for i := 0; i < fieldTypes.Size(); i++ {
		fieldBlockBuilders[i] = fieldTypes.Get(i).CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
	}
	return fieldBlockBuilders
}

// @Override
func (rr *RowBlockBuilder) getRawFieldBlocks() []Block {
	return corvertToBlocks(rr.fieldBlockBuilders)
}

// @Override
// @Nullable
func (rr *RowBlockBuilder) getFieldBlockOffsets() []int32 {
	if rr.hasNullRow {
		return rr.fieldBlockOffsets
	} else {
		return nil
	}
}

// @Override
func (rr *RowBlockBuilder) getOffsetBase() int32 {
	return 0
}

// @Nullable
// @Override
func (rr *RowBlockBuilder) getRowIsNull() []bool {
	if rr.hasNullRow {
		return rr.rowIsNull
	} else {
		return nil
	}
}

// @Override
func (rr *RowBlockBuilder) MayHaveNull() bool {
	return rr.hasNullRow
}

// @Override
func (rr *RowBlockBuilder) GetPositionCount() int32 {
	return rr.positionCount
}

// @Override
func (rr *RowBlockBuilder) GetSizeInBytes() int64 {
	sizeInBytes := int64((util.INT32_BYTES + util.BYTE_BYTES) * rr.positionCount)
	var i int32
	for i = 0; i < rr.numFields; i++ {
		sizeInBytes += rr.fieldBlockBuilders[i].GetSizeInBytes()
	}
	return sizeInBytes
}

// @Override
func (rr *RowBlockBuilder) GetRetainedSizeInBytes() int64 {
	size := int64(ROW_BB_INSTANCE_SIZE + util.SizeOf(rr.fieldBlockOffsets) + util.SizeOf(rr.rowIsNull))
	var i int32
	for i = 0; i < rr.numFields; i++ {
		size += rr.fieldBlockBuilders[i].GetRetainedSizeInBytes()
	}
	if rr.blockBuilderStatus != nil {
		size += int64(BBSINSTANCE_SIZE)
	}
	size += int64(SINGLEROW_INSTANCE_SIZE)
	return size
}

// @Override
func (rr *RowBlockBuilder) BeginBlockEntry() BlockBuilder {
	if rr.currentEntryOpened {
		panic("Expected current entry to be closed but was opened")
	}
	rr.currentEntryOpened = true
	rr.singleRowBlockWriter.setRowIndex(rr.fieldBlockBuilders[0].GetPositionCount())
	return rr.singleRowBlockWriter
}

// @Override
func (rr *RowBlockBuilder) CloseEntry() BlockBuilder {
	if !rr.currentEntryOpened {
		panic("Expected entry to be opened but was closed")
	}
	rr.entryAdded(false)
	rr.currentEntryOpened = false
	rr.singleRowBlockWriter.reset()
	return rr
}

// @Override
func (rr *RowBlockBuilder) AppendNull() BlockBuilder {
	if rr.currentEntryOpened {
		panic("Current entry must be closed before a null can be written")
	}
	rr.entryAdded(true)
	return rr
}

func (rr *RowBlockBuilder) entryAdded(isNull bool) {
	rinInt32 := util.BoolsLenInt32(rr.rowIsNull)
	if rinInt32 <= rr.positionCount {
		newSize := calculateNewArraySize(rinInt32)
		rr.rowIsNull = util.CopyOfBools(rr.rowIsNull, newSize)
		rr.fieldBlockOffsets = util.CopyOfInt32(rr.fieldBlockOffsets, newSize+1)
	}
	if isNull {
		rr.fieldBlockOffsets[rr.positionCount+1] = rr.fieldBlockOffsets[rr.positionCount]
	} else {
		rr.fieldBlockOffsets[rr.positionCount+1] = rr.fieldBlockOffsets[rr.positionCount] + 1
	}
	rr.rowIsNull[rr.positionCount] = isNull
	rr.hasNullRow = rr.hasNullRow || isNull
	rr.positionCount++

	var i int32
	for i = 0; i < rr.numFields; i++ {
		if rr.fieldBlockBuilders[i].GetPositionCount() != rr.fieldBlockOffsets[rr.positionCount] {
			panic(fmt.Sprintf("field %d has unexpected position count. Expected: %d, actual: %d", i, rr.fieldBlockOffsets[rr.positionCount], rr.fieldBlockBuilders[i].GetPositionCount()))
		}
	}
	if rr.blockBuilderStatus != nil {
		rr.blockBuilderStatus.AddBytes(util.INT32_BYTES + util.BYTE_BYTES)
	}
}

// @Override
func (rr *RowBlockBuilder) Build() Block {
	if rr.currentEntryOpened {
		panic("Current entry must be closed before the block can be built")
	}
	fieldBlocks := make([]Block, rr.numFields)
	var i int32
	for i = 0; i < rr.numFields; i++ {
		fieldBlocks[i] = rr.fieldBlockBuilders[i].Build()
	}
	if rr.hasNullRow {
		return CreateRowBlockInternal(0, rr.positionCount, rr.rowIsNull, rr.fieldBlockOffsets, fieldBlocks)
	} else {
		return CreateRowBlockInternal(0, rr.positionCount, nil, nil, fieldBlocks)
	}

}

// @Override
func (rr *RowBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	newSize := calculateBlockResetSize(rr.GetPositionCount())
	newBlockBuilders := make([]BlockBuilder, rr.numFields)
	var i int32
	for i = 0; i < rr.numFields; i++ {
		newBlockBuilders[i] = rr.fieldBlockBuilders[i].NewBlockBuilderLike(blockBuilderStatus)
	}
	return NewRowBlockBuilder2(blockBuilderStatus, newBlockBuilders, make([]int32, newSize+1), make([]bool, newSize))
}

// @Override
func (b *RowBlockBuilder) GetSliceLength(position int32) int32 {
	return b.AbstractRowBlock.GetSliceLength(position)
}

// @Override
func (b *RowBlockBuilder) GetByte(position int32, offset int32) byte {
	return b.AbstractRowBlock.GetByte(position, offset)
}

// @Override
func (b *RowBlockBuilder) GetShort(position int32, offset int32) int16 {
	return b.AbstractRowBlock.GetShort(position, offset)
}

// @Override
func (b *RowBlockBuilder) GetInt(position int32, offset int32) int32 {
	return b.AbstractRowBlock.GetInt(position, offset)
}

// @Override
func (b *RowBlockBuilder) GetLong(position int32, offset int32) int64 {
	return b.AbstractRowBlock.GetLong(position, offset)
}

// @Override
func (b *RowBlockBuilder) GetSlice(position int32, offset int32, length int32) *slice.Slice {
	return b.AbstractRowBlock.GetSlice(position, offset, length)
}

// @Override
func (b *RowBlockBuilder) IsLoaded() bool {
	return b.AbstractRowBlock.IsLoaded()
}

// @Override
func (b *RowBlockBuilder) GetLoadedBlock() Block {
	return b.AbstractRowBlock.GetLoadedBlock()
}

// @Override
func (b *RowBlockBuilder) GetLogicalSizeInBytes() int64 {
	return b.AbstractRowBlock.GetLogicalSizeInBytes()
}

// @Override
func (b *RowBlockBuilder) BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool {
	return b.AbstractRowBlock.BytesEqual(position, offset, otherSlice, otherOffset, length)
}

// @Override
func (b *RowBlockBuilder) BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32 {
	return b.AbstractRowBlock.BytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
}

// @Override
func (b *RowBlockBuilder) WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder) {
	b.AbstractRowBlock.WriteBytesTo(position, offset, length, blockBuilder)
}

// @Override
func (b *RowBlockBuilder) Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool {
	return b.AbstractRowBlock.Equals(position, offset, otherBlock, otherPosition, otherOffset, length)
}

// @Override
func (b *RowBlockBuilder) Hash(position int32, offset int32, length int32) int64 {
	return b.AbstractRowBlock.Hash(position, offset, length)
}

// @Override
func (b *RowBlockBuilder) CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32 {
	return b.AbstractRowBlock.CompareTo(leftPosition, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength)
}

// @Override
func (ak *RowBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawFieldBlocks()...)
}

func (ak *RowBlockBuilder) getFieldBlockOffset(position int32) int32 {
	offsets := ak.getFieldBlockOffsets()
	return util.If(offsets != nil, offsets[position+ak.getOffsetBase()], position+ak.getOffsetBase()).(int32)
}

// @Override
func (ak *RowBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
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
func (ak *RowBlockBuilder) GetRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	return CreateRowBlockInternal(position+ak.getOffsetBase(), length, ak.getRowIsNull(), ak.getFieldBlockOffsets(), ak.getRawFieldBlocks())
}

// @Override
func (ak *RowBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
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
func (ak *RowBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
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
func (ak *RowBlockBuilder) CopyRegion(position int32, length int32) Block {
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
func (ak *RowBlockBuilder) GetObject(position int32, clazz reflect.Type) basic.Object {
	if clazz != BLOCK_TYPE {
		panic("clazz must be Block type")
	}
	ak.checkReadablePosition(position)
	return NewSingleRowBlock(ak.getFieldBlockOffset(position), ak.getRawFieldBlocks())
}

// @Override
func (ak *RowBlockBuilder) GetSingleValueBlock(position int32) Block {
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
func (ak *RowBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
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
func (ak *RowBlockBuilder) IsNull(position int32) bool {
	ak.checkReadablePosition(position)
	rowIsNull := ak.getRowIsNull()
	return rowIsNull != nil && rowIsNull[position+ak.getOffsetBase()]
}

func (ak *RowBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= ak.GetPositionCount() {
		panic("position is not valid")
	}
}
