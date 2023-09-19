package block

import (
	"errors"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ArrayBlockBuilder struct {
	// 继承
	BlockBuilder

	// 继承
	AbstractArrayBlock

	// // 继承
	// aab *AbstractArrayBlock

	positionCount       int32 //@Nullable
	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	offsets             []int32
	valueIsNull         []bool
	hasNullValue        bool
	values              BlockBuilder
	currentEntryOpened  bool
	retainedSizeInBytes int64
}

// var ARRAY_BB_INSTANCE_SIZE int32 = ClassLayout.parseClass(ArrayBlockBuilder.class).instanceSize()
var ARRAY_BB_INSTANCE_SIZE int32 = util.SizeOf(&ArrayBlockBuilder{})

func NewArrayBlockBuilder(valuesBlock BlockBuilder, blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *ArrayBlockBuilder {
	return NewArrayBlockBuilder4(blockBuilderStatus, valuesBlock, expectedEntries)
}
func NewArrayBlockBuilder2(elementType Type, blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) *ArrayBlockBuilder {
	bb := elementType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
	return NewArrayBlockBuilder4(blockBuilderStatus, bb, expectedEntries)
}
func NewArrayBlockBuilder3(elementType Type, blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *ArrayBlockBuilder {
	bb := elementType.CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
	return NewArrayBlockBuilder4(blockBuilderStatus, bb, expectedEntries)
}
func NewArrayBlockBuilder4(blockBuilderStatus *BlockBuilderStatus, values BlockBuilder, expectedEntries int32) *ArrayBlockBuilder {
	ar := new(ArrayBlockBuilder)
	ar.offsets = make([]int32, 1)
	ar.valueIsNull = make([]bool, 0)
	ar.blockBuilderStatus = blockBuilderStatus
	ar.values = values
	ar.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	ar.updateDataSize()
	return ar
}

// @Override
func (ar *ArrayBlockBuilder) GetPositionCount() int32 {
	return ar.positionCount
}

// @Override
func (ar *ArrayBlockBuilder) GetSizeInBytes() int64 {
	// return ar.values.GetSizeInBytes() + ((Integer.BYTES + Byte.BYTES) * positionCount.(int64))
	return ar.values.GetSizeInBytes() + int64(int32(util.INT32_BYTES+util.BYTE_BYTES)*ar.positionCount)
}

// @Override
func (ar *ArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return ar.retainedSizeInBytes + ar.values.GetRetainedSizeInBytes()
}

// @Override
func (ar *ArrayBlockBuilder) GetOffsets() []int32 {
	return ar.offsets
}

// @Override
func (ar *ArrayBlockBuilder) GetOffsetBase() int32 {
	return 0
}

// @Nullable
// @Override
func (ar *ArrayBlockBuilder) GetValueIsNull() []bool {
	if ar.hasNullValue {
		return ar.valueIsNull
	} else {
		return nil
	}
}

// @Override
func (ar *ArrayBlockBuilder) MayHaveNull() bool {
	return ar.hasNullValue
}

// @Override
func (ar *ArrayBlockBuilder) BeginBlockEntry() BlockBuilder {
	if ar.currentEntryOpened {
		panic("Expected current entry to be closed but was opened")
	}
	ar.currentEntryOpened = true
	return NewSingleArrayBlockWriter(ar.values, ar.values.GetPositionCount())
}

// @Override
func (ar *ArrayBlockBuilder) CloseEntry() BlockBuilder {
	if !ar.currentEntryOpened {
		panic("Expected entry to be opened but was closed")
	}
	ar.entryAdded(false)
	ar.currentEntryOpened = false
	return ar
}

// @Override
func (ar *ArrayBlockBuilder) AppendNull() BlockBuilder {
	if ar.currentEntryOpened {
		panic("Current entry must be closed before a null can be written")
	}
	ar.entryAdded(true)
	return ar
}

func (ar *ArrayBlockBuilder) entryAdded(isNull bool) {
	if len(ar.valueIsNull) <= int(ar.positionCount) {
		ar.growCapacity()
	}
	ar.offsets[ar.positionCount+1] = ar.values.GetPositionCount()
	ar.valueIsNull[ar.positionCount] = isNull
	ar.hasNullValue = ar.hasNullValue || isNull
	ar.positionCount++
	if ar.blockBuilderStatus != nil {
		// ar.blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES)
		(*ar.blockBuilderStatus).AddBytes(util.INT32_BYTES + util.BYTE_BYTES)
	}
}

func (ar *ArrayBlockBuilder) growCapacity() {
	var newSize int32
	if ar.initialized {
		newSize = calculateNewArraySize(int32(len(ar.valueIsNull)))
	} else {
		newSize = ar.initialEntryCount
		ar.initialized = true
	}

	ar.valueIsNull = util.CopyOfBools(ar.valueIsNull, newSize)
	ar.offsets = util.CopyOfInt32(ar.offsets, newSize+1)
	ar.updateDataSize()
}

func (ar *ArrayBlockBuilder) updateDataSize() {
	ar.retainedSizeInBytes = int64(ARRAY_BB_INSTANCE_SIZE + util.SizeOf(ar.valueIsNull) + util.SizeOf(ar.offsets))
	if ar.blockBuilderStatus != nil {
		ar.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (ar *ArrayBlockBuilder) Build() Block {
	if ar.currentEntryOpened {
		panic("Current entry must be closed before the block can be built")
	}
	if ar.hasNullValue {
		return createArrayBlockInternal(0, ar.positionCount, ar.valueIsNull, ar.offsets, ar.values.Build())
	} else {
		return createArrayBlockInternal(0, ar.positionCount, nil, ar.offsets, ar.values.Build())
	}

}

// @Override
func (ar *ArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	newSize := calculateBlockResetSize(ar.GetPositionCount())
	return NewArrayBlockBuilder4(blockBuilderStatus, ar.values.NewBlockBuilderLike(blockBuilderStatus), newSize)
}

func (ak *ArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}

func (ik *ArrayBlockBuilder) IsLoaded() bool {
	return true
}

// protected abstract Block getRawElementBlock();
func (ak *ArrayBlockBuilder) getRawElementBlock() Block {
	return nil
}

// @Override
func (ak *ArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawElementBlock())
}

func (ak *ArrayBlockBuilder) getOffset(position int32) int32 {
	return ak.GetOffsets()[position+ak.GetOffsetBase()]
}

// @Override
func (ak *ArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	newOffsets := make([]int32, length+1)
	newOffsets[0] = 0

	var newValueIsNull []bool
	if ak.GetValueIsNull() == nil {
		newValueIsNull = nil
	} else {
		newValueIsNull = make([]bool, length)
	}
	valuesPositions := NewIntArrayList2()
	for i := 0; int32(i) < length; i++ {
		position := positions[offset+int32(i)]

		if newValueIsNull != nil && ak.IsNull(position) {
			newValueIsNull[i] = true
			newOffsets[i+1] = newOffsets[i]
		} else {
			valuesStartOffset := ak.getOffset(position)
			valuesEndOffset := ak.getOffset(position + 1)
			valuesLength := valuesEndOffset - valuesStartOffset
			newOffsets[i+1] = newOffsets[i] + valuesLength
			for elementIndex := valuesStartOffset; elementIndex < valuesEndOffset; elementIndex++ {
				valuesPositions.Add(elementIndex)
			}
		}
	}
	newValues := ak.getRawElementBlock().CopyPositions(valuesPositions.Elements(), 0, valuesPositions.Size())
	return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues)
}

// @Override
func (ak *ArrayBlockBuilder) GetRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	return createArrayBlockInternal(position+ak.GetOffsetBase(), length, ak.GetValueIsNull(), ak.GetOffsets(), ak.getRawElementBlock())
}

// @Override
func (ak *ArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	valueStart := ak.GetOffsets()[ak.GetOffsetBase()+position]
	valueEnd := ak.GetOffsets()[ak.GetOffsetBase()+position+length]

	// return ak.GetRawElementBlock().GetRegionSizeInBytes(valueStart, valueEnd-valueStart) + ((Integer.BYTES + Byte.BYTES) * int64(length))
	return ak.getRawElementBlock().GetRegionSizeInBytes(valueStart, valueEnd-valueStart) + ((4 + 1) * int64(length))
}

// @Override
func (ak *ArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	checkValidPositions(positions, ak.GetPositionCount())
	used := make([]bool, ak.getRawElementBlock().GetPositionCount())
	usedPositionCount := 0
	for i := 0; i < len(positions); i++ {
		if positions[i] {
			usedPositionCount++
			valueStart := ak.GetOffsets()[ak.GetOffsetBase()+int32(i)]
			valueEnd := ak.GetOffsets()[ak.GetOffsetBase()+int32(i)+1]
			for j := valueStart; j < valueEnd; j++ {
				used[j] = true
			}
		}
	}
	// return (*ak.GetRawElementBlock()).GetPositionsSizeInBytes(used) + ((Integer.BYTES + Byte.BYTES) * usedPositionCount.(int64))
	return ak.getRawElementBlock().GetPositionsSizeInBytes(used) + ((4 + 1) * int64(usedPositionCount))
}

// @Override
func (ak *ArrayBlockBuilder) CopyRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + length)
	newValues := ak.getRawElementBlock().CopyRegion(startValueOffset, endValueOffset-startValueOffset)
	newOffsets := compactOffsets(ak.GetOffsets(), position+ak.GetOffsetBase(), length)
	valueIsNull := ak.GetValueIsNull()

	// newValueIsNull := ternary(valueIsNull == nil, nil, compactArray(valueIsNull, position+getOffsetBase(), length))
	var newValueIsNull []bool
	if valueIsNull == nil {
		newValueIsNull = nil
	} else {
		newValueIsNull = compactBoolArray(valueIsNull, position+ak.GetOffsetBase(), length)
	}

	if newValues == ak.getRawElementBlock() && reflect.DeepEqual(newOffsets, ak.GetOffsets()) && reflect.DeepEqual(newValueIsNull, valueIsNull) {
		return ak
	}
	return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues)
}

// @Override
func (ak *ArrayBlockBuilder) GetObject(position int32, clazz reflect.Type) basic.Object {
	// if clazz != reflect.TypeOf(&Block{}) {
	// 	panic("type must be Block type")
	// }
	ak.checkReadablePosition(position)
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	return ak.getRawElementBlock().GetRegion(startValueOffset, endValueOffset-startValueOffset)
}

// @Override
func (ak *ArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	err := ak.checkReadablePosition(position)
	if err != nil {
		return nil
	}
	startValueOffset := ak.getOffset(position)
	valueLength := ak.getOffset(position+1) - startValueOffset
	newBlock := ak.getRawElementBlock()
	newValues := newBlock.CopyRegion(startValueOffset, valueLength)

	offsets := []int32{0, valueLength}
	if ak.IsNull(position) {
		valueIsNull := []bool{true}
		return createArrayBlockInternal(0, 1, valueIsNull, offsets, newValues)
	} else {
		return createArrayBlockInternal(0, 1, nil, offsets, newValues)
	}

}

// @Override
func (ak *ArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	err := ak.checkReadablePosition(position)
	if err != nil {
		panic(err.Error())
	}
	if ak.IsNull(position) {
		return 0
	}
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	rawElementBlock := ak.getRawElementBlock()
	size := int64(0)
	for i := startValueOffset; i < endValueOffset; i++ {
		s := rawElementBlock.GetEstimatedDataSizeForStats(i)
		size += s
	}
	return int64(size)
}

// @Override
func (ak *ArrayBlockBuilder) IsNull(position int32) bool {
	err := ak.checkReadablePosition(position)
	if err != nil {
		panic(err.Error())
	}
	valueIsNull := ak.GetValueIsNull()
	return valueIsNull != nil && valueIsNull[position+ak.GetOffsetBase()]
}

func (ak *ArrayBlockBuilder) Apply(function ArrayBlockFunction, position int32) (*interface{}, error) {
	err := ak.checkReadablePosition(position)
	if err != nil {
		return nil, err
	}
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	return function.Apply(ak.getRawElementBlock(), startValueOffset, endValueOffset-startValueOffset), nil
}

func (ak *ArrayBlockBuilder) checkReadablePosition(position int32) error {
	if position < 0 || position >= ak.GetPositionCount() {
		return errors.New("position is not valid")
	}
	return nil
}
