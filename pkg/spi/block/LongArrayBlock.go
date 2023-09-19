package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongArrayBlock struct {
	// 继承
	Block

	arrayOffset         int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	values              []int64
	sizeInBytes         int64
	retainedSizeInBytes int64
}

// var longArrayBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(LongArrayBlock.class).instanceSize()

var LONG_ARRAY_BLOCK_INSTANCE_SIZE int32 = util.SizeOf(&LongArrayBlock{})

func NewLongArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], values []int64) *LongArrayBlock {
	return NewLongArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), values)
}
func NewLongArrayBlock2(arrayOffset int32, positionCount int32, valueIsNull []bool, values []int64) *LongArrayBlock {
	lk := new(LongArrayBlock)
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	lk.arrayOffset = arrayOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	lk.positionCount = positionCount

	if util.Int64sLenInt32(values)-arrayOffset < positionCount {
		panic("values length is less than positionCount")
	}
	lk.values = values

	if valueIsNull != nil && util.BoolsLenInt32(valueIsNull)-arrayOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	lk.valueIsNull = valueIsNull

	lk.sizeInBytes = int64(BYTE_LONG_BYTES * positionCount)
	lk.retainedSizeInBytes = int64(LONG_ARRAY_BLOCK_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(values))
	return lk
}

// @Override
func (lk *LongArrayBlock) GetSizeInBytes() int64 {
	return lk.sizeInBytes
}

// @Override
func (lk *LongArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Long.BYTES + Byte.BYTES) * length.(int64)
	return int64(BYTE_LONG_BYTES * length)
}

// @Override
func (lk *LongArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return lk.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (lk *LongArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	// return (Long.BYTES + Byte.BYTES).(int64) * selectedPositionsCount
	return int64(BYTE_LONG_BYTES * selectedPositionsCount)
}

// @Override
func (lk *LongArrayBlock) GetRetainedSizeInBytes() int64 {
	return lk.retainedSizeInBytes
}

// @Override
func (lk *LongArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	if lk.IsNull(position) {
		return 0
	} else {
		return util.INT64_BYTES
	}
}

// @Override
func (lk *LongArrayBlock) GetPositionCount() int32 {
	return lk.positionCount
}

// @Override
func (lk *LongArrayBlock) GetLong(position int32, offset int32) int64 {
	lk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return lk.values[position+lk.arrayOffset]
}

// @Override
// @Deprecated
func (lk *LongArrayBlock) GetInt(position int32, offset int32) int32 {
	lk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	num, _ := util.ToInt32Exact(lk.values[position+lk.arrayOffset])
	return num
}

// @Override
// @Deprecated
func (lk *LongArrayBlock) GetShort(position int32, offset int32) int16 {
	lk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	num, _ := util.ToInt16Exact(lk.values[position+lk.arrayOffset])
	return num
}

// @Override
// @Deprecated
func (lk *LongArrayBlock) GetByte(position int32, offset int32) byte {
	lk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	num, _ := util.ToByteExact(lk.values[position+lk.arrayOffset])
	return num
}

// @Override
func (lk *LongArrayBlock) MayHaveNull() bool {
	return lk.valueIsNull != nil
}

// @Override
func (lk *LongArrayBlock) IsNull(position int32) bool {
	lk.checkReadablePosition(position)
	return lk.valueIsNull != nil && lk.valueIsNull[position+lk.arrayOffset]
}

// @Override
func (lk *LongArrayBlock) GetSingleValueBlock(position int32) Block {
	lk.checkReadablePosition(position)
	if lk.IsNull(position) {
		return NewLongArrayBlock2(0, 1, []bool{true}, []int64{lk.values[position+lk.arrayOffset]})
	} else {
		return NewLongArrayBlock2(0, 1, nil, []int64{lk.values[position+lk.arrayOffset]})
	}
}

// @Override
func (lk *LongArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if lk.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int64, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		lk.checkReadablePosition(position)
		if lk.valueIsNull != nil {
			newValueIsNull[i] = lk.valueIsNull[position+lk.arrayOffset]
		}
		newValues[i] = lk.values[position+lk.arrayOffset]
	}
	return NewLongArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (lk *LongArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(lk.GetPositionCount(), positionOffset, length)
	return NewLongArrayBlock2(positionOffset+lk.arrayOffset, length, lk.valueIsNull, lk.values)
}

// @Override
func (lk *LongArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(lk.GetPositionCount(), positionOffset, length)
	positionOffset += lk.arrayOffset
	var newValueIsNull []bool = nil
	if lk.valueIsNull == nil {
		newValueIsNull = compactBoolArray(lk.valueIsNull, positionOffset, length)
	}
	newValues := compactInt64Array(lk.values, positionOffset, length)
	if reflect.DeepEqual(newValueIsNull, lk.valueIsNull) && reflect.DeepEqual(newValues, lk.values) {
		return lk
	}
	return NewLongArrayBlock2(0, length, newValueIsNull, newValues)
}

func (lk *LongArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= lk.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *LongArrayBlock) GetLoadedBlock() Block {
	return ak
}

func (ik *LongArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
func (ik *LongArrayBlock) IsLoaded() bool {
	return true
}
