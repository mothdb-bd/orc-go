package block

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type IntArrayBlockBuilder struct { //@Nullable
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	values              []int32
	retainedSizeInBytes int64
}

// var (
// 	intArrayBlockBuilderiNSTANCE_SIZE    int32  = ClassLayout.parseClass(IntArrayBlockBuilder.class).instanceSize()
// 	intArrayBlockBuildernULL_VALUE_BLOCK Block = NewIntArrayBlock(0, 1, make([]bool), make([]int32, 1))
// )

var (
	INT_ARRAY_INSTANCE_SIZE    = util.SizeOf(&IntArrayBlockBuilder{})
	INT_ARRAY_NULL_VALUE_BLOCK = NewIntArrayBlock2(0, 1, []bool{true}, make([]int32, 1))
)

func NewIntArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *IntArrayBlockBuilder {
	ir := new(IntArrayBlockBuilder)
	ir.valueIsNull = make([]bool, 0)
	ir.values = make([]int32, 0)

	ir.blockBuilderStatus = blockBuilderStatus
	ir.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	ir.UpdateDataSize()
	return ir
}

// @Override
func (ir *IntArrayBlockBuilder) WriteInt(value int32) BlockBuilder {
	if len(ir.values) <= int(ir.positionCount) {
		ir.growCapacity()
	}
	ir.values[ir.positionCount] = value
	ir.hasNonNullValue = true
	ir.positionCount++
	if ir.blockBuilderStatus != nil {
		// ir.blockBuilderStatus.AddBytes(Byte.BYTES + Integer.BYTES)
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + util.INT32_BYTES)
	}
	return ir
}

// @Override
func (ir *IntArrayBlockBuilder) CloseEntry() BlockBuilder {
	return ir
}

// @Override
func (ir *IntArrayBlockBuilder) AppendNull() BlockBuilder {
	if len(ir.values) <= int(ir.positionCount) {
		ir.growCapacity()
	}
	ir.valueIsNull[ir.positionCount] = true
	ir.hasNullValue = true
	ir.positionCount++
	if ir.blockBuilderStatus != nil {
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + util.INT32_BYTES)
	}
	return ir
}

// @Override
func (ir *IntArrayBlockBuilder) Build() Block {
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT_ARRAY_NULL_VALUE_BLOCK, ir.positionCount)
	}
	if ir.hasNullValue {
		return NewIntArrayBlock2(0, ir.positionCount, ir.valueIsNull, ir.values)
	} else {
		return NewIntArrayBlock2(0, ir.positionCount, nil, ir.values)
	}
}

// @Override
func (ir *IntArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewIntArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(ir.positionCount))
}

func (ir *IntArrayBlockBuilder) growCapacity() {
	var newSize int32
	if ir.initialized {
		newSize = calculateNewArraySize(int32(len(ir.values)))
	} else {
		newSize = ir.initialEntryCount
		ir.initialized = true
	}
	// ir.valueIsNull = Arrays.copyOf(valueIsNull, newSize)
	// values = Arrays.copyOf(values, newSize)
	ir.valueIsNull = util.CopyOfBools(ir.valueIsNull, newSize)
	ir.values = util.CopyOfInt32(ir.values, newSize)
	ir.UpdateDataSize()
}

func (ir *IntArrayBlockBuilder) UpdateDataSize() {
	// retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values)
	ir.retainedSizeInBytes = int64(INT_ARRAY_INSTANCE_SIZE + util.SizeOf(ir.valueIsNull) + util.SizeOf(ir.values))
	if ir.blockBuilderStatus != nil {
		ir.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (ir *IntArrayBlockBuilder) GetSizeInBytes() int64 {
	// return (Integer.BYTES + Byte.BYTES) * positionCount.(int64)
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * ir.positionCount)
}

// @Override
func (ir *IntArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	// return (Integer.BYTES + Byte.BYTES) * length.(int64)
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ir *IntArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	// return (Integer.BYTES + Byte.BYTES) * countUsedPositions(positions).(int64)
	return int64((util.INT32_BYTES + util.BYTE_BYTES) * countUsedPositions(positions))
}

// @Override
func (ir *IntArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return ir.retainedSizeInBytes
}

// @Override
func (ir *IntArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ir.IsNull(position), 0, int64(util.INT64_BYTES))
}

// @Override
func (ir *IntArrayBlockBuilder) GetPositionCount() int32 {
	return ir.positionCount
}

// @Override
func (ir *IntArrayBlockBuilder) GetInt(position int32, offset int32) int32 {
	ir.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be zero")
	}
	return ir.values[position]
}

// @Override
func (ir *IntArrayBlockBuilder) MayHaveNull() bool {
	return ir.hasNullValue
}

// @Override
func (ir *IntArrayBlockBuilder) IsNull(position int32) bool {
	ir.checkReadablePosition(position)
	return ir.valueIsNull[position]
}

// @Override
func (ir *IntArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	ir.checkReadablePosition(position)
	if ir.valueIsNull[position] {
		return NewIntArrayBlock2(0, 1, []bool{true}, []int32{ir.values[position]})
	} else {
		return NewIntArrayBlock2(0, 1, nil, []int32{ir.values[position]})
	}
}

// @Override
func (ir *IntArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if ir.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newValues := make([]int32, length)
	for i := 0; i < int(length); i++ {
		position := positions[int(offset)+i]
		ir.checkReadablePosition(position)
		if ir.hasNullValue {
			newValueIsNull[i] = ir.valueIsNull[position]
		}
		newValues[i] = ir.values[position]
	}
	return NewIntArrayBlock2(0, length, newValueIsNull, newValues)
}

// @Override
func (ir *IntArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	if ir.hasNullValue {
		return NewIntArrayBlock2(positionOffset, length, ir.valueIsNull, ir.values)
	} else {
		return NewIntArrayBlock2(positionOffset, length, nil, ir.values)
	}
}

// @Override
func (ir *IntArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT_ARRAY_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = make([]bool, length)
	if ir.hasNullValue {
		util.CopyBools(ir.valueIsNull, positionOffset, newValueIsNull, 0, length)
	}
	var newValues []int32 = make([]int32, length)
	util.CopyInt32s(ir.values, positionOffset, newValues, 0, length)
	return NewIntArrayBlock2(0, length, newValueIsNull, newValues)
}

func (ir *IntArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= ir.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *IntArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}
func (ik *IntArrayBlockBuilder) IsLoaded() bool {
	return true
}

func (ik *IntArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
