package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortArrayBlockBuilder struct { //@Nullable
	// 继承
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	values              []int16
	retainedSizeInBytes int64
}

// var (
// 	shortArrayBlockBuilderiNSTANCE_SIZE    int32 = ClassLayout.parseClass(ShortArrayBlockBuilder.class).instanceSize()
// 	shortArrayBlockBuildernULL_VALUE_BLOCK Block = NewShortArrayBlock(0, 1, make([]bool), make([]int16, 1))
// )

var (
	SHORT_ARRAY_INSTANCE_SIZE    int32 = util.SizeOf(&ShortArrayBlockBuilder{})
	SHORT_ARRAY_NULL_VALUE_BLOCK Block = NewShortArrayBlock2(0, 1, []bool{true}, make([]int16, 1))
)

func NewShortArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *ShortArrayBlockBuilder {
	sr := new(ShortArrayBlockBuilder)

	sr.valueIsNull = make([]bool, 0)
	sr.values = make([]int16, 0)

	sr.blockBuilderStatus = blockBuilderStatus
	sr.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	sr.updateDataSize()
	return sr
}

// @Override
func (sr *ShortArrayBlockBuilder) WriteShort(value int16) BlockBuilder {
	if util.Int16sLenInt32(sr.values) <= sr.positionCount {
		sr.growCapacity()
	}
	sr.values[sr.positionCount] = value
	sr.hasNonNullValue = true
	sr.positionCount++
	if sr.blockBuilderStatus != nil {
		sr.blockBuilderStatus.AddBytes(BYTE_SHORT_BYTES)
	}
	return sr
}

// @Override
func (sr *ShortArrayBlockBuilder) CloseEntry() BlockBuilder {
	return sr
}

// @Override
func (sr *ShortArrayBlockBuilder) AppendNull() BlockBuilder {
	if util.Int16sLenInt32(sr.values) <= sr.positionCount {
		sr.growCapacity()
	}
	sr.valueIsNull[sr.positionCount] = true
	sr.hasNullValue = true
	sr.positionCount++
	if sr.blockBuilderStatus != nil {
		sr.blockBuilderStatus.AddBytes(BYTE_SHORT_BYTES)
	}
	return sr
}

// @Override
func (sr *ShortArrayBlockBuilder) Build() Block {
	if !sr.hasNonNullValue {
		return NewRunLengthEncodedBlock(SHORT_ARRAY_NULL_VALUE_BLOCK, sr.positionCount)
	}
	if sr.hasNullValue {
		return NewShortArrayBlock2(0, sr.positionCount, sr.valueIsNull, sr.values)
	} else {
		return NewShortArrayBlock2(0, sr.positionCount, nil, sr.values)
	}
}

// @Override
func (sr *ShortArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewShortArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(sr.positionCount))
}

func (sr *ShortArrayBlockBuilder) growCapacity() {
	var newSize int32
	if sr.initialized {
		newSize = calculateNewArraySize(util.Int16sLenInt32(sr.values))
	} else {
		newSize = sr.initialEntryCount
		sr.initialized = true
	}
	sr.valueIsNull = util.CopyOfBools(sr.valueIsNull, newSize)
	sr.values = util.CopyOfInt16s(sr.values, newSize)
	sr.updateDataSize()
}

func (sr *ShortArrayBlockBuilder) updateDataSize() {
	sr.retainedSizeInBytes = int64(SHORT_ARRAY_INSTANCE_SIZE + util.SizeOf(sr.valueIsNull) + util.SizeOf(sr.values))
	if sr.blockBuilderStatus != nil {
		sr.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (sr *ShortArrayBlockBuilder) GetSizeInBytes() int64 {
	return int64(BYTE_SHORT_BYTES * sr.positionCount)
}

// @Override
func (sr *ShortArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64(BYTE_SHORT_BYTES * length)
}

// @Override
func (sr *ShortArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	return int64(BYTE_SHORT_BYTES * countUsedPositions(positions))
}

// @Override
func (sr *ShortArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return sr.retainedSizeInBytes
}

// @Override
func (sr *ShortArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(sr.IsNull(position), util.INT64_ZERO, util.INT16_BYTES)
}

// @Override
func (sr *ShortArrayBlockBuilder) GetPositionCount() int32 {
	return sr.positionCount
}

// @Override
func (sr *ShortArrayBlockBuilder) GetShort(position int32, offset int32) int16 {
	sr.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return sr.values[position]
}

// @Override
func (sr *ShortArrayBlockBuilder) MayHaveNull() bool {
	return sr.hasNullValue
}

// @Override
func (sr *ShortArrayBlockBuilder) IsNull(position int32) bool {
	sr.checkReadablePosition(position)
	return sr.valueIsNull[position]
}

// @Override
func (sr *ShortArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	sr.checkReadablePosition(position)
	if sr.valueIsNull[position] {
		return NewShortArrayBlock2(0, 1, []bool{true}, []int16{sr.values[position]})
	} else {
		return NewShortArrayBlock2(0, 1, nil, []int16{sr.values[position]})
	}
}

// @Override
func (sr *ShortArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !sr.hasNonNullValue {
		return NewRunLengthEncodedBlock(SHORT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if sr.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int16, length)
	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		sr.checkReadablePosition(position)
		if sr.hasNullValue {
			newValueIsNull[i] = sr.valueIsNull[position]
		}
		newValues[i] = sr.values[position]
	}
	return NewShortArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (sr *ShortArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(sr.GetPositionCount(), positionOffset, length)
	if !sr.hasNonNullValue {
		return NewRunLengthEncodedBlock(SHORT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	if sr.hasNullValue {
		return NewShortArrayBlock2(positionOffset, length, sr.valueIsNull, sr.values)
	} else {
		return NewShortArrayBlock2(positionOffset, length, nil, sr.values)
	}
}

// @Override
func (sr *ShortArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(sr.GetPositionCount(), positionOffset, length)
	if !sr.hasNonNullValue {
		return NewRunLengthEncodedBlock(SHORT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if sr.hasNullValue {

		newValueIsNull = util.CopyBoolsOfRange(sr.valueIsNull, positionOffset, positionOffset+length)
	}
	newValues := util.CopyInt16sOfRange(sr.values, positionOffset, positionOffset+length)
	return NewShortArrayBlock2(0, length, newValueIsNull, newValues)
}

func (sr *ShortArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= sr.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *ShortArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}
func (ik *ShortArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
func (ik *ShortArrayBlockBuilder) IsLoaded() bool {
	return true
}
