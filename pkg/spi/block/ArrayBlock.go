package block

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ArrayBlock struct {
	AbstractArrayBlock // 继承 AbstractArrayBlock

	arrayOffset         int32
	positionCount       int32
	valueIsNull         []bool
	values              Block
	offsets             []int32
	sizeInBytes         int64
	retainedSizeInBytes int64
}

// var arrayBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(ArrayBlock.class).instanceSize()
var ARRAY_INSTANCE_SIZE int32 = util.SizeOf(&ArrayBlock{})

func FromElementBlock(positionCount int32, valueIsNullOptional *optional.Optional[[]bool], arrayOffset []int32, values Block) Block {
	valueIsNull := (valueIsNullOptional.OrElse(nil))
	arrayValidate(0, positionCount, valueIsNull, arrayOffset, values)
	for i := 0; i < int(positionCount); i++ {
		offset := arrayOffset[i]
		length := arrayOffset[i+1] - offset
		if length < 0 {
			panic(fmt.Sprintf("Offset is not monotonically ascending. offsets[%d]=%d, offsets[%d]=%d", i, arrayOffset[i], i+1, arrayOffset[i+1]))
		}
		if valueIsNull != nil && valueIsNull[i] && length != 0 {
			panic("A null array must have zero entries")
		}
	}
	return NewArrayBlock(0, positionCount, valueIsNull, arrayOffset, values)
}

func createArrayBlockInternal(arrayOffset int32, positionCount int32, valueIsNull []bool, offsets []int32, values Block) *ArrayBlock {
	arrayValidate(arrayOffset, positionCount, valueIsNull, offsets, values)
	return NewArrayBlock(arrayOffset, positionCount, valueIsNull, offsets, values)
}

func arrayValidate(arrayOffset int32, positionCount int32, valueIsNull []bool, offsets []int32, values Block) {
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	if valueIsNull != nil && (int32(len(valueIsNull))-arrayOffset) < positionCount {
		panic("isNull length is less than positionCount")
	}
	if offsets == nil {
		panic("offsets is null")
	}
	if int32(len(offsets))-arrayOffset < (positionCount + 1) {
		panic("offsets length is less than positionCount")
	}
	if values == nil {
		panic("values is null")
	}
}
func NewArrayBlock(arrayOffset int32, positionCount int32, valueIsNull []bool, offsets []int32, values Block) *ArrayBlock {
	ak := new(ArrayBlock)
	ak.arrayOffset = arrayOffset
	ak.positionCount = positionCount
	ak.valueIsNull = valueIsNull
	ak.offsets = offsets
	ak.values = values
	ak.sizeInBytes = -1
	ak.retainedSizeInBytes = int64(ARRAY_INSTANCE_SIZE + util.SizeOf(offsets) + util.SizeOf(valueIsNull))
	return ak
}

// @Override
func (ak *ArrayBlock) GetPositionCount() int32 {
	return ak.positionCount
}

// @Override
func (ak *ArrayBlock) GetSizeInBytes() int64 {
	if ak.sizeInBytes < 0 {
		if !ak.values.IsLoaded() {
			return ak.getBaseSizeInBytes()
		}
		ak.calculateSize()
	}
	return ak.sizeInBytes
}

func (ak *ArrayBlock) calculateSize() {
	valueStart := ak.offsets[ak.arrayOffset]
	valueEnd := ak.offsets[ak.arrayOffset+ak.positionCount]
	ak.sizeInBytes = ak.values.GetRegionSizeInBytes(valueStart, valueEnd-valueStart) + ak.getBaseSizeInBytes()
}

func (ak *ArrayBlock) getBaseSizeInBytes() int64 {
	// return (Integer.BYTES + Byte.BYTES) * (long) this.positionCount;
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * ak.positionCount)
}

// @Override
func (ak *ArrayBlock) GetRetainedSizeInBytes() int64 {
	return ak.retainedSizeInBytes + ak.values.GetRetainedSizeInBytes()
}

// @Override
func (ak *ArrayBlock) getRawElementBlock() Block {
	return ak.values
}

// @Override
func (ak *ArrayBlock) GetOffsets() []int32 {
	return ak.offsets
}

// @Override
func (ak *ArrayBlock) GetOffsetBase() int32 {
	return ak.arrayOffset
}

// @Override
// @Nullable
func (ak *ArrayBlock) GetValueIsNull() []bool {
	return ak.valueIsNull
}

// @Override
func (ak *ArrayBlock) MayHaveNull() bool {
	return ak.valueIsNull != nil
}

// @Override
func (ak *ArrayBlock) IsLoaded() bool {
	return ak.values.IsLoaded()
}

// @Override
func (ak *ArrayBlock) GetLoadedBlock() Block {
	loadedValuesBlock := ak.values.GetLoadedBlock()
	if loadedValuesBlock == ak.values {
		return ak
	}
	return createArrayBlockInternal(ak.arrayOffset, ak.positionCount, ak.valueIsNull, ak.offsets, loadedValuesBlock)
}

// @Override
func (ak *ArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.NewArrayList(ak.getRawElementBlock())
}

func (ak *ArrayBlock) getOffset(position int32) int32 {
	return ak.GetOffsets()[position+ak.GetOffsetBase()]
}

// @Override
func (ak *ArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
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
func (ak *ArrayBlock) GetRegion(position int32, length int32) Block {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	return createArrayBlockInternal(position+ak.GetOffsetBase(), length, ak.GetValueIsNull(), ak.GetOffsets(), ak.getRawElementBlock())
}

// @Override
func (ak *ArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	positionCount := ak.GetPositionCount()
	checkValidRegion(positionCount, position, length)
	valueStart := ak.GetOffsets()[ak.GetOffsetBase()+position]
	valueEnd := ak.GetOffsets()[ak.GetOffsetBase()+position+length]

	// return ak.GetRawElementBlock().GetRegionSizeInBytes(valueStart, valueEnd-valueStart) + ((Integer.BYTES + Byte.BYTES) * int64(length))
	return ak.getRawElementBlock().GetRegionSizeInBytes(valueStart, valueEnd-valueStart) + ((4 + 1) * int64(length))
}

// @Override
func (ak *ArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
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
func (ak *ArrayBlock) CopyRegion(position int32, length int32) Block {
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
func (ak *ArrayBlock) GetObject(position int32, clazz reflect.Type) basic.Object {
	// if clazz != reflect.TypeOf(&Block{}) {
	// 	panic("type must be Block type")
	// }
	ak.checkReadablePosition(position)
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	return ak.getRawElementBlock().GetRegion(startValueOffset, endValueOffset-startValueOffset)
}

// @Override
func (ak *ArrayBlock) GetSingleValueBlock(position int32) Block {
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
func (ak *ArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
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
func (ak *ArrayBlock) IsNull(position int32) bool {
	err := ak.checkReadablePosition(position)
	if err != nil {
		panic(err.Error())
	}
	valueIsNull := ak.GetValueIsNull()
	return valueIsNull != nil && valueIsNull[position+ak.GetOffsetBase()]
}

func (ak *ArrayBlock) Apply(function ArrayBlockFunction, position int32) (*interface{}, error) {
	err := ak.checkReadablePosition(position)
	if err != nil {
		return nil, err
	}
	startValueOffset := ak.getOffset(position)
	endValueOffset := ak.getOffset(position + 1)
	return function.Apply(ak.getRawElementBlock(), startValueOffset, endValueOffset-startValueOffset), nil
}

func (ak *ArrayBlock) checkReadablePosition(position int32) error {
	if position < 0 || position >= ak.GetPositionCount() {
		return errors.New("position is not valid")
	}
	return nil
}
