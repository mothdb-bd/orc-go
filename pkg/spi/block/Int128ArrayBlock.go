package block

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Int128ArrayBlock struct {
	// 继承
	Block

	positionOffset      int32
	positionCount       int32 //@Nullable
	valueIsNull         []bool
	values              []int64
	sizeInBytes         int64
	retainedSizeInBytes int64
}

var (
	INT128_INSTANCE_SIZE int32 = util.SizeOf(&Int128ArrayBlock{})
	INT128_BYTES         int32 = util.INT64_BYTES + util.INT64_BYTES
)

func NewInt128ArrayBlock(positionCount int32, valueIsNull *optional.Optional[[]bool], values []int64) *Int128ArrayBlock {
	return NewInt128ArrayBlock2(0, positionCount, valueIsNull.OrElse(nil), values)
}
func NewInt128ArrayBlock2(positionOffset int32, positionCount int32, valueIsNull []bool, values []int64) *Int128ArrayBlock {
	ik := new(Int128ArrayBlock)
	if positionOffset < 0 {
		panic("positionOffset is negative")
	}
	ik.positionOffset = positionOffset
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	ik.positionCount = positionCount
	if util.Int64sLenInt32(values)-(positionOffset*2) < positionCount*2 {
		panic("values length is less than positionCount")
	}
	ik.values = values
	if valueIsNull != nil && util.BoolsLenInt32(valueIsNull)-positionOffset < positionCount {
		panic("isNull length is less than positionCount")
	}
	ik.valueIsNull = valueIsNull
	ik.sizeInBytes = int64((INT128_BYTES + util.BYTE_BYTES) * positionCount)
	ik.retainedSizeInBytes = int64(INT128_INSTANCE_SIZE + util.SizeOf(valueIsNull) + util.SizeOf(values))
	return ik
}

// @Override
func (ik *Int128ArrayBlock) GetSizeInBytes() int64 {
	return ik.sizeInBytes
}

// @Override
func (ik *Int128ArrayBlock) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64((INT128_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ik *Int128ArrayBlock) GetPositionsSizeInBytes(positions []bool) int64 {
	return ik.GetPositionsSizeInBytes2(positions, countUsedPositions(positions))
}

// @Override
func (ik *Int128ArrayBlock) GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64 {
	return int64((INT128_BYTES + util.BYTE_BYTES) * selectedPositionsCount)
}

// @Override
func (ik *Int128ArrayBlock) GetRetainedSizeInBytes() int64 {
	return ik.retainedSizeInBytes
}

// @Override
func (ik *Int128ArrayBlock) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ik.IsNull(position), util.INT64_ZERO, int64(INT128_BYTES))
}

// @Override
func (ik *Int128ArrayBlock) GetPositionCount() int32 {
	return ik.positionCount
}

// @Override
func (ik *Int128ArrayBlock) GetLong(position int32, offset int32) int64 {
	ik.checkReadablePosition(position)
	if offset == 0 {
		return ik.values[(position+ik.positionOffset)*2]
	}
	if offset == 8 {
		return ik.values[((position+ik.positionOffset)*2)+1]
	}
	panic("offset must be 0 or 8")
}

// @Override
func (ik *Int128ArrayBlock) MayHaveNull() bool {
	return ik.valueIsNull != nil
}

// @Override
func (ik *Int128ArrayBlock) IsNull(position int32) bool {
	ik.checkReadablePosition(position)
	return ik.valueIsNull != nil && ik.valueIsNull[position+ik.positionOffset]
}

// @Override
func (ik *Int128ArrayBlock) GetSingleValueBlock(position int32) Block {
	ik.checkReadablePosition(position)
	if ik.IsNull(position) {
		return NewInt128ArrayBlock2(0, 1, []bool{true}, []int64{ik.values[(position+ik.positionOffset)*2], ik.values[((position+ik.positionOffset)*2)+1]})
	} else {
		return NewInt128ArrayBlock2(0, 1, nil, []int64{ik.values[(position+ik.positionOffset)*2], ik.values[((position+ik.positionOffset)*2)+1]})
	}

}

// @Override
func (ik *Int128ArrayBlock) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int64, length*2)
	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		ik.checkReadablePosition(position)
		if ik.valueIsNull != nil {
			newValueIsNull[i] = ik.valueIsNull[position+ik.positionOffset]
		}
		newValues[i*2] = ik.values[(position+ik.positionOffset)*2]
		newValues[(i*2)+1] = ik.values[((position+ik.positionOffset)*2)+1]
	}
	return NewInt128ArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (ik *Int128ArrayBlock) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	return NewInt128ArrayBlock2(positionOffset+ik.positionOffset, length, ik.valueIsNull, ik.values)
}

// @Override
func (ik *Int128ArrayBlock) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ik.GetPositionCount(), positionOffset, length)
	positionOffset += ik.positionOffset

	var newValueIsNull []bool = nil
	if ik.valueIsNull != nil {
		newValueIsNull = compactBoolArray(ik.valueIsNull, positionOffset, length)
	}
	newValues := compactInt64Array(ik.values, positionOffset*2, length*2)
	if basic.ObjectEqual(newValueIsNull, ik.valueIsNull) && basic.ObjectEqual(newValues, ik.values) {
		return ik
	}
	return NewInt128ArrayBlock2(0, length, newValueIsNull, newValues)
}

func (ik *Int128ArrayBlock) checkReadablePosition(position int32) {
	if position < 0 || position >= ik.GetPositionCount() {
		panic("position is not valid")
	}
}

func (ik *Int128ArrayBlock) IsLoaded() bool {
	return true
}

func (ik *Int128ArrayBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}

func (ik *Int128ArrayBlock) GetLoadedBlock() Block {
	return ik
}
