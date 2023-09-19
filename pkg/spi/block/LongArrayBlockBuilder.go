package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongArrayBlockBuilder struct { //@Nullable
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	values              []int64
	retainedSizeInBytes int64
}

// var (
// 	longArrayBlockBuilderiNSTANCE_SIZE    int32  = ClassLayout.parseClass(LongArrayBlockBuilder.class).instanceSize()
// 	longArrayBlockBuildernULL_VALUE_BLOCK Block = NewLongArrayBlock(0, 1, make([]bool), make([]int64, 1))
// )

var (
	LONGARRAYBB_INSTANCE_SIZE    int32 = util.SizeOf(&LongArrayBlockBuilder{})
	LONGARRAYBB_NULL_VALUE_BLOCK Block = NewLongArrayBlock2(0, 1, []bool{true}, []int64{1})
)

func NewLongArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *LongArrayBlockBuilder {
	lr := new(LongArrayBlockBuilder)
	lr.valueIsNull = make([]bool, 0)
	lr.values = make([]int64, 0)

	lr.blockBuilderStatus = blockBuilderStatus
	lr.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	lr.updateDataSize()
	return lr
}

// @Override
func (lr *LongArrayBlockBuilder) WriteLong(value int64) BlockBuilder {
	if int32(len(lr.values)) <= lr.positionCount {
		lr.growCapacity()
	}
	lr.values[lr.positionCount] = value
	lr.hasNonNullValue = true
	lr.positionCount++
	if lr.blockBuilderStatus != nil {
		lr.blockBuilderStatus.AddBytes(BYTE_LONG_BYTES)
	}
	return lr
}

// @Override
func (lr *LongArrayBlockBuilder) CloseEntry() BlockBuilder {
	return lr
}

// @Override
func (lr *LongArrayBlockBuilder) AppendNull() BlockBuilder {
	if util.Int64sLenInt32(lr.values) <= lr.positionCount {
		lr.growCapacity()
	}
	lr.valueIsNull[lr.positionCount] = true
	lr.hasNullValue = true
	lr.positionCount++
	if lr.blockBuilderStatus != nil {
		lr.blockBuilderStatus.AddBytes(BYTE_LONG_BYTES)
	}
	return lr
}

// @Override
func (lr *LongArrayBlockBuilder) Build() Block {
	if !lr.hasNonNullValue {
		return NewRunLengthEncodedBlock(LONGARRAYBB_NULL_VALUE_BLOCK, lr.positionCount)
	}
	if lr.hasNullValue {
		return NewLongArrayBlock2(0, lr.positionCount, lr.valueIsNull, lr.values)
	} else {
		return NewLongArrayBlock2(0, lr.positionCount, nil, lr.values)
	}
}

// @Override
func (lr *LongArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewLongArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(lr.positionCount))
}

func (lr *LongArrayBlockBuilder) growCapacity() {
	var newSize int32
	if lr.initialized {
		newSize = calculateNewArraySize(util.Int64sLenInt32(lr.values))
	} else {
		newSize = lr.initialEntryCount
		lr.initialized = true
	}
	lr.valueIsNull = util.CopyOfBools(lr.valueIsNull, newSize)
	lr.values = util.CopyOfInt64s(lr.values, newSize)
	lr.updateDataSize()
}

func (lr *LongArrayBlockBuilder) updateDataSize() {
	lr.retainedSizeInBytes = int64(LONGARRAYBB_INSTANCE_SIZE + util.SizeOf(lr.valueIsNull) + util.SizeOf(lr.values))
	if lr.blockBuilderStatus != nil {
		lr.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (lr *LongArrayBlockBuilder) GetSizeInBytes() int64 {
	return int64(BYTE_LONG_BYTES * lr.positionCount)
}

// @Override
func (lr *LongArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Long.BYTES + Byte.BYTES) * length.(int64)
	return int64(BYTE_LONG_BYTES * length)
}

// @Override
func (lr *LongArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	// return (Long.BYTES + Byte.BYTES) * countUsedPositions(positions).(int64)
	return int64(BYTE_LONG_BYTES * countUsedPositions(positions))
}

// @Override
func (lr *LongArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return lr.retainedSizeInBytes
}

// @Override
func (lr *LongArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(lr.IsNull(position), util.INT64_ZERO, util.INT64_BYTES)
}

// @Override
func (lr *LongArrayBlockBuilder) GetPositionCount() int32 {
	return lr.positionCount
}

// @Override
func (lr *LongArrayBlockBuilder) GetLong(position int32, offset int32) int64 {
	lr.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return lr.values[position]
}

// @Override
// @Deprecated
func (lr *LongArrayBlockBuilder) GetInt(position int32, offset int32) int32 {
	lr.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	// return toIntExact(values[position])
	num, _ := util.ToInt32Exact(lr.values[position])
	return num
}

// @Override
// @Deprecated
func (lr *LongArrayBlockBuilder) GetShort(position int32, offset int32) int16 {
	lr.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	num, _ := util.ToInt16Exact(lr.values[position])
	return num
}

// @Override
// @Deprecated
func (lr *LongArrayBlockBuilder) GetByte(position int32, offset int32) byte {
	lr.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	num, _ := util.ToByteExact(lr.values[position])
	return num
}

// @Override
func (lr *LongArrayBlockBuilder) MayHaveNull() bool {
	return lr.hasNullValue
}

// @Override
func (lr *LongArrayBlockBuilder) IsNull(position int32) bool {
	lr.checkReadablePosition(position)
	return lr.valueIsNull[position]
}

// @Override
func (lr *LongArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	lr.checkReadablePosition(position)
	if lr.valueIsNull[position] {
		return NewLongArrayBlock2(0, 1, []bool{true}, []int64{lr.values[position]})
	} else {
		return NewLongArrayBlock2(0, 1, nil, []int64{lr.values[position]})
	}
}

// @Override
func (lr *LongArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !lr.hasNonNullValue {
		return NewRunLengthEncodedBlock(LONGARRAYBB_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if lr.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int64, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		lr.checkReadablePosition(position)
		if lr.hasNullValue {
			newValueIsNull[i] = lr.valueIsNull[position]
		}
		newValues[i] = lr.values[position]
	}
	return NewLongArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (lr *LongArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(lr.GetPositionCount(), positionOffset, length)
	if !lr.hasNonNullValue {
		return NewRunLengthEncodedBlock(LONGARRAYBB_NULL_VALUE_BLOCK, length)
	}
	if lr.hasNullValue {
		return NewLongArrayBlock2(positionOffset, length, lr.valueIsNull, lr.values)
	} else {
		return NewLongArrayBlock2(positionOffset, length, nil, lr.values)
	}
}

// @Override
func (lr *LongArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(lr.GetPositionCount(), positionOffset, length)
	if !lr.hasNonNullValue {
		return NewRunLengthEncodedBlock(LONGARRAYBB_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = make([]bool, length)
	if lr.hasNullValue {
		// newValueIsNull = Arrays.copyOfRange(valueIsNull, positionOffset, positionOffset+length)
		util.CopyBools(lr.valueIsNull, positionOffset, newValueIsNull, 0, length)
	}

	var newValues []int64 = make([]int64, length)
	util.CopyInt64s(lr.values, positionOffset, newValues, 0, length)
	return NewLongArrayBlock2(0, length, newValueIsNull, newValues)
}

func (lr *LongArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= lr.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *LongArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}
func (ik *LongArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
func (ik *LongArrayBlockBuilder) IsLoaded() bool {
	return true
}
