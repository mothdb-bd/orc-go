package block

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Int96ArrayBlock struct {
	// 继承
	Block

	positionOffset      int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	high                []int64
	low                 []int32
	sizeInBytes         int64
	retainedSizeInBytes int64
}

var (
	INT96_INSTANCE_SIZE int32 = util.SizeOf(&Int96ArrayBlock{})
	INT96_BYTES         int32 = util.INT64_BYTES + util.INT32_BYTES
)

func NewInt96ArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], high []int64, low []int32) *Int96ArrayBlock {
	return NewInt96ArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), high, low)
}
func NewInt96ArrayBlock2(positionOffset int32, positionCount int32, valueIsNull []bool, high []int64, low []int32) *Int96ArrayBlock {
	ik := new(Int96ArrayBlock)
	if positionOffset < 0 {
		panic("positionOffset is negative")
	}
	ik.positionOffset = positionOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	ik.positionCount = positionCount
	if util.Int64sLenInt32(high)-positionOffset < positionCount {
		panic("high length is less than positionCount")
	}
	ik.high = high
	if util.Int32sLenInt32(low)-positionOffset < positionCount {
		panic("low length is less than positionCount")
	}
	ik.low = low
	if valueIsNull != nil && util.BoolsLenInt32(valueIsNull)-positionOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	ik.valueIsNull = valueIsNull
	ik.sizeInBytes = int64((INT96_BYTES + util.BYTE_BYTES) * positionCount)
	ik.retainedSizeInBytes = int64(INT96_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(high) + util.SizeOf(low))
	return ik
}

// @Override
func (ik *Int96ArrayBlock) GetSizeInBytes() int64 {
	return ik.sizeInBytes
}

// @Override
func (ik *Int96ArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64((INT96_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ik *Int96ArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return ik.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (ik *Int96ArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	return int64((INT96_BYTES + util.BYTE_BYTES) * selectedPositionsCount)
}

// @Override
func (ik *Int96ArrayBlock) GetRetainedSizeInBytes() int64 {
	return ik.retainedSizeInBytes
}

// @Override
func (ik *Int96ArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ik.IsNull(position), 0, int64(INT96_BYTES))
}

// @Override
func (ik *Int96ArrayBlock) GetPositionCount() int32 {
	return ik.positionCount
}

// @Override
func (ik *Int96ArrayBlock) GetLong(position int32, offset int32) int64 {
	ik.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be 0")
	}
	return ik.high[ik.positionOffset+position]
}

// @Override
func (ik *Int96ArrayBlock) GetInt(position int32, offset int32) int32 {
	ik.checkReadablePosition(position)
	if offset != 8 {
		panic("offset must be 8")
	}
	return ik.low[ik.positionOffset+position]
}

// @Override
func (ik *Int96ArrayBlock) MayHaveNull() bool {
	return ik.valueIsNull != nil
}

// @Override
func (ik *Int96ArrayBlock) IsNull(position int32) bool {
	ik.checkReadablePosition(position)
	return ik.valueIsNull != nil && ik.valueIsNull[position+ik.positionOffset]
}

// @Override
func (ik *Int96ArrayBlock) GetSingleValueBlock(position int32) Block {
	ik.checkReadablePosition(position)
	if ik.IsNull(position) {
		return NewInt96ArrayBlock2(0, 1, []bool{true}, []int64{ik.high[position+ik.positionOffset]}, []int32{ik.low[position+ik.positionOffset]})
	} else {
		return NewInt96ArrayBlock2(0, 1, nil, []int64{ik.high[position+ik.positionOffset]}, []int32{ik.low[position+ik.positionOffset]})
	}
}

// @Override
func (ik *Int96ArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newHigh := make([]int64, length)
	newLow := make([]int32, length)
	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		ik.checkReadablePosition(position)
		if ik.valueIsNull != nil {
			newValueIsNull[i] = ik.valueIsNull[position+ik.positionOffset]
		}
		newHigh[i] = ik.high[position+ik.positionOffset]
		newLow[i] = ik.low[position+ik.positionOffset]
	}
	return NewInt96ArrayBlock2(0, length, newValueIsNull, newHigh, newLow)
}

// @Override
func (ik *Int96ArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	return NewInt96ArrayBlock2(positionOffset+ik.positionOffset, length, ik.valueIsNull, ik.high, ik.low)
}

// @Override
func (ik *Int96ArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	positionOffset += ik.positionOffset
	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = compactBoolArray(ik.valueIsNull, positionOffset, length)
	}
	newHigh := compactInt64Array(ik.high, positionOffset, length)
	newLow := compactInt32Array(ik.low, positionOffset, length)
	if basic.ObjectEqual(newValueIsNull, ik.valueIsNull) && basic.ObjectEqual(newHigh, ik.high) && basic.ObjectEqual(newLow, ik.low) {
		return ik
	}
	return NewInt96ArrayBlock2(0, length, newValueIsNull, newHigh, newLow)
}

func (ik *Int96ArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ik.GetPositionCount() {
		panic("position is not valid")
	}
}

func (ak *Int96ArrayBlock) GetLoadedBlock() Block {
	return ak
}
func (ik *Int96ArrayBlock) IsLoaded() bool {
	return true
}

func (ik *Int96ArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
