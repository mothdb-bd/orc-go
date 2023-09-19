package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// var intArrayBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(IntArrayBlock.class).instanceSize()

type IntArrayBlock struct {
	// 继承
	Block

	arrayOffset         int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	values              []int32
	sizeInBytes         int64
	retainedSizeInBytes int64
}

// var intArrayBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(IntArrayBlock.class).instanceSize()
var INT_INSTANCE_SIZE int32 = util.SizeOf(&IntArrayBlock{})

func NewIntArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], values []int32) *IntArrayBlock {
	return NewIntArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), values)
}
func NewIntArrayBlock2(arrayOffset int32, positionCount int32, valueIsNull []bool, values []int32) *IntArrayBlock {
	ik := new(IntArrayBlock)
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	ik.arrayOffset = arrayOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	ik.positionCount = positionCount
	if int32(len(values))-arrayOffset < positionCount {
		panic("values length is less than positionCount")
	}
	ik.values = values
	if valueIsNull != nil && int32(len(valueIsNull))-arrayOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	ik.valueIsNull = valueIsNull
	// ik.sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount.(int64)
	ik.sizeInBytes = int64((util.INT32_BYTES + util.BYTE_BYTES) * positionCount)
	// retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values)
	ik.retainedSizeInBytes = int64(INT_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(values))
	return ik
}

// @Override
func (ik *IntArrayBlock) GetSizeInBytes() int64 {
	return ik.sizeInBytes
}

// @Override
func (ik *IntArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Integer.BYTES + Byte.BYTES) * length.(int64)
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ik *IntArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return ik.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (ik *IntArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	// return (Integer.BYTES + Byte.BYTES).(int64) * selectedPositionsCount
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * selectedPositionsCount)
}

// @Override
func (ik *IntArrayBlock) GetRetainedSizeInBytes() int64 {
	return ik.retainedSizeInBytes
}

// @Override
func (ik *IntArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ik.IsNull(position), 0, int64(util.INT32_BYTES))
}

// @Override
func (ik *IntArrayBlock) GetPositionCount() int32 {
	return ik.positionCount
}

// @Override
func (ik *IntArrayBlock) GetInt(position int32, offset int32) int32 {
	ik.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return ik.values[position+ik.arrayOffset]
}

// @Override
func (ik *IntArrayBlock) MayHaveNull() bool {
	return ik.valueIsNull != nil
}

// @Override
func (ik *IntArrayBlock) IsNull(position int32) bool {
	ik.checkReadablePosition(position)
	return ik.valueIsNull != nil && ik.valueIsNull[position+ik.arrayOffset]
}

// @Override
func (ik *IntArrayBlock) GetSingleValueBlock(position int32) Block {
	ik.checkReadablePosition(position)
	if ik.IsNull(position) {
		return NewIntArrayBlock2(0, 1, []bool{true}, []int32{ik.values[position+ik.arrayOffset]})
	} else {
		return NewIntArrayBlock2(0, 1, nil, []int32{ik.values[position+ik.arrayOffset]})
	}
}

// @Override
func (ik *IntArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int32, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		ik.checkReadablePosition(position)
		if ik.valueIsNull != nil {
			newValueIsNull[i] = ik.valueIsNull[position+ik.arrayOffset]
		}
		newValues[i] = ik.values[position+ik.arrayOffset]
	}
	return NewIntArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (ik *IntArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	return NewIntArrayBlock2(positionOffset+ik.arrayOffset, length, ik.valueIsNull, ik.values)
}

// @Override
func (ik *IntArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	positionOffset += ik.arrayOffset
	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = compactBoolArray(ik.valueIsNull, positionOffset, length)
	}
	newValues := compactInt32Array(ik.values, positionOffset, length)
	if reflect.DeepEqual(newValueIsNull, ik.valueIsNull) && reflect.DeepEqual(newValues, ik.values) {
		return ik
	}
	return NewIntArrayBlock2(0, length, newValueIsNull, newValues)
}

func (ik *IntArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ik.GetPositionCount() {
		panic("position is not valid")
	}
}

func (ik *IntArrayBlock) IsLoaded() bool {
	return true
}

func (ik *IntArrayBlock) GetLoadedBlock() Block {
	return ik
}

func (ik *IntArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
