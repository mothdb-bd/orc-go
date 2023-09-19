package block

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortArrayBlock struct {
	// 继承
	Block

	arrayOffset         int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	values              []int16
	sizeInBytes         int64
	retainedSizeInBytes int64
}

var SHORT_ARRAY_BLOCK_INSTANCE_SIZE int32 = util.SizeOf(&ShortArrayBlock{})

func NewShortArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], values []int16) *ShortArrayBlock {
	return NewShortArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), values)
}
func NewShortArrayBlock2(arrayOffset int32, positionCount int32, valueIsNull []bool, values []int16) *ShortArrayBlock {
	sk := new(ShortArrayBlock)
	if arrayOffset < 0 {
		panic("arrayOffset is negative")
	}
	sk.arrayOffset = arrayOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	sk.positionCount = positionCount
	if int32(len(values))-arrayOffset < positionCount {
		panic("values length is less than positionCount")
	}
	sk.values = values
	if valueIsNull != nil && int32(len(valueIsNull))-arrayOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	sk.valueIsNull = valueIsNull
	sk.sizeInBytes = int64(BYTE_SHORT_BYTES * positionCount)
	sk.retainedSizeInBytes = int64(SHORT_ARRAY_BLOCK_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(values))
	return sk
}

// @Override
func (sk *ShortArrayBlock) GetSizeInBytes() int64 {
	return sk.sizeInBytes
}

// @Override
func (sk *ShortArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64(BYTE_SHORT_BYTES * length)
}

// @Override
func (sk *ShortArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return sk.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (sk *ShortArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	return int64(BYTE_SHORT_BYTES * selectedPositionsCount)
}

// @Override
func (sk *ShortArrayBlock) GetRetainedSizeInBytes() int64 {
	return sk.retainedSizeInBytes
}

// @Override
func (sk *ShortArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(sk.IsNull(position), util.INT64_ZERO, util.INT16_BYTES)
}

// @Override
func (sk *ShortArrayBlock) GetPositionCount() int32 {
	return sk.positionCount
}

// @Override
func (sk *ShortArrayBlock) GetShort(position int32, offset int32) int16 {
	sk.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return sk.values[position+sk.arrayOffset]
}

// @Override
func (sk *ShortArrayBlock) MayHaveNull() bool {
	return sk.valueIsNull != nil
}

// @Override
func (sk *ShortArrayBlock) IsNull(position int32) bool {
	sk.checkReadablePosition(position)
	return sk.valueIsNull != nil && sk.valueIsNull[position+sk.arrayOffset]
}

// @Override
func (sk *ShortArrayBlock) GetSingleValueBlock(position int32) Block {
	sk.checkReadablePosition(position)
	if sk.IsNull(position) {
		return NewShortArrayBlock2(0, 1, []bool{true}, []int16{sk.values[position+sk.arrayOffset]})
	} else {
		return NewShortArrayBlock2(0, 1, nil, []int16{sk.values[position+sk.arrayOffset]})
	}

}

// @Override
func (sk *ShortArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if sk.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int16, length)
	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		sk.checkReadablePosition(position)
		if sk.valueIsNull != nil {
			newValueIsNull[i] = sk.valueIsNull[position+sk.arrayOffset]
		}
		newValues[i] = sk.values[position+sk.arrayOffset]
	}
	return NewShortArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (sk *ShortArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(sk.GetPositionCount(), positionOffset, length)
	return NewShortArrayBlock2(positionOffset+sk.arrayOffset, length, sk.valueIsNull, sk.values)
}

// @Override
func (sk *ShortArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(sk.GetPositionCount(), positionOffset, length)
	positionOffset += sk.arrayOffset

	var newValueIsNull []bool = nil
	if sk.valueIsNull != nil {
		newValueIsNull = compactBoolArray(sk.valueIsNull, positionOffset, length)
	}
	newValues := compactInt16Array(sk.values, positionOffset, length)
	if basic.ObjectEqual(newValueIsNull, sk.valueIsNull) && basic.ObjectEqual(newValues, sk.values) {
		return sk
	}
	return NewShortArrayBlock2(0, length, newValueIsNull, newValues)
}

func (sk *ShortArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= sk.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *ShortArrayBlock) GetLoadedBlock() Block {
	return ak
}
func (ik *ShortArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
func (ik *ShortArrayBlock) IsLoaded() bool {
	return true
}
