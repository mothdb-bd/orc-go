package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Int96ArrayBlockBuilder struct { //@Nullable
	//	继承
	BlockBuilder

	blockBuilderStatus  *BlockBuilderStatus
	initialized         bool
	initialEntryCount   int32
	positionCount       int32
	hasNullValue        bool
	hasNonNullValue     bool
	valueIsNull         []bool
	high                []int64
	low                 []int32
	retainedSizeInBytes int64
	entryPositionCount  int32
}

var (
	INT96_BB_INSTANCE_SIZE int32 = util.SizeOf(&Int96ArrayBlockBuilder{})
	INT96_NULL_VALUE_BLOCK Block = NewInt96ArrayBlock2(0, 1, []bool{true}, make([]int64, 1), make([]int32, 1))
)

func NewInt96ArrayBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) *Int96ArrayBlockBuilder {
	ir := new(Int96ArrayBlockBuilder)
	ir.blockBuilderStatus = blockBuilderStatus
	ir.initialEntryCount = maths.MaxInt32(expectedEntries, 1)
	ir.valueIsNull = make([]bool, 0)
	ir.high = make([]int64, 0)
	ir.low = make([]int32, 0)
	ir.updateDataSize()
	return ir
}

// @Override
func (ir *Int96ArrayBlockBuilder) WriteLong(high int64) BlockBuilder {
	if ir.entryPositionCount != 0 {
		panic("long can only be written at the beginning of the entry")
	}
	if util.BoolsLenInt32(ir.valueIsNull) <= ir.positionCount {
		ir.growCapacity()
	}
	ir.high[ir.positionCount] = high
	ir.hasNonNullValue = true
	ir.entryPositionCount++
	return ir
}

// @Override
func (ir *Int96ArrayBlockBuilder) WriteInt(low int32) BlockBuilder {
	if ir.entryPositionCount != 1 {
		panic("int can only be written at the end of the entry")
	}
	if util.BoolsLenInt32(ir.valueIsNull) <= ir.positionCount {
		ir.growCapacity()
	}
	ir.low[ir.positionCount] = low
	ir.hasNonNullValue = true
	ir.entryPositionCount++
	return ir
}

// @Override
func (ir *Int96ArrayBlockBuilder) CloseEntry() BlockBuilder {
	if ir.entryPositionCount != 2 {
		panic(fmt.Sprintf("Expected entry size to be exactly %d bytes but was %d", INT96_BYTES, (ir.entryPositionCount * util.INT64_BYTES)))
	}
	ir.positionCount++
	ir.entryPositionCount = 0
	if ir.blockBuilderStatus != nil {
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + INT96_BYTES)
	}
	return ir
}

// @Override
func (ir *Int96ArrayBlockBuilder) AppendNull() BlockBuilder {
	if ir.entryPositionCount != 0 {
		panic("Current entry must be closed before a null can be written")
	}
	if util.BoolsLenInt32(ir.valueIsNull) <= ir.positionCount {
		ir.growCapacity()
	}
	ir.valueIsNull[ir.positionCount] = true
	ir.hasNullValue = true
	ir.positionCount++
	if ir.blockBuilderStatus != nil {
		ir.blockBuilderStatus.AddBytes(util.BYTE_BYTES + INT96_BYTES)
	}
	return ir
}

// @Override
func (ir *Int96ArrayBlockBuilder) Build() Block {
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT96_NULL_VALUE_BLOCK, ir.positionCount)
	}
	if ir.hasNullValue {
		return NewInt96ArrayBlock2(0, ir.positionCount, ir.valueIsNull, ir.high, ir.low)
	} else {
		return NewInt96ArrayBlock2(0, ir.positionCount, nil, ir.high, ir.low)
	}

}

// @Override
func (ir *Int96ArrayBlockBuilder) NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder {
	return NewInt96ArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(ir.positionCount))
}

func (ir *Int96ArrayBlockBuilder) growCapacity() {
	var newSize int32
	if ir.initialized {
		newSize = calculateNewArraySize(util.BoolsLenInt32(ir.valueIsNull))
	} else {
		newSize = ir.initialEntryCount
		ir.initialized = true
	}
	ir.valueIsNull = util.CopyOfBools(ir.valueIsNull, newSize)
	ir.high = util.CopyOfInt64s(ir.high, newSize)
	ir.low = util.CopyOfInt32(ir.low, newSize)
	ir.updateDataSize()
}

func (ir *Int96ArrayBlockBuilder) updateDataSize() {
	ir.retainedSizeInBytes = int64(INT96_BB_INSTANCE_SIZE + util.SizeOf(ir.valueIsNull) + util.SizeOf(ir.high) + util.SizeOf(ir.low))
	if ir.blockBuilderStatus != nil {
		ir.retainedSizeInBytes += int64(BBSINSTANCE_SIZE)
	}
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetSizeInBytes() int64 {
	return int64((INT96_BYTES + util.BYTE_BYTES) * ir.positionCount)
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetRegionSizeInBytes(position int32, length int32) int64 {
	return int64((INT96_BYTES + util.BYTE_BYTES) * length)
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetPositionsSizeInBytes(positions []bool) int64 {
	return int64((INT96_BYTES + util.BYTE_BYTES) * countUsedPositions(positions))
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetRetainedSizeInBytes() int64 {
	return ir.retainedSizeInBytes
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetEstimatedDataSizeForStats(position int32) int64 {
	return util.Ternary(ir.IsNull(position), 0, int64(INT96_BYTES))
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetPositionCount() int32 {
	return ir.positionCount
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetLong(position int32, offset int32) int64 {
	ir.checkReadablePosition(position)
	if offset != 0 {
		panic("offset must be 0")
	}
	return ir.high[position]
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetInt(position int32, offset int32) int32 {
	ir.checkReadablePosition(position)
	if offset != 8 {
		panic("offset must be 8")
	}
	return ir.low[position]
}

// @Override
func (ir *Int96ArrayBlockBuilder) MayHaveNull() bool {
	return ir.hasNullValue
}

// @Override
func (ir *Int96ArrayBlockBuilder) IsNull(position int32) bool {
	ir.checkReadablePosition(position)
	return ir.valueIsNull[position]
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetSingleValueBlock(position int32) Block {
	ir.checkReadablePosition(position)
	if ir.valueIsNull[position] {
		return NewInt96ArrayBlock2(0, 1, []bool{true}, []int64{ir.high[position]}, []int32{ir.low[position]})
	} else {
		return NewInt96ArrayBlock2(0, 1, nil, []int64{ir.high[position]}, []int32{ir.low[position]})
	}

}

// @Override
func (ir *Int96ArrayBlockBuilder) CopyPositions(positions []int32, offset int32, length int32) Block {
	checkArrayRange(positions, offset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT96_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if ir.hasNullValue {
		newValueIsNull = make([]bool, length)
	}
	newHigh := make([]int64, length)
	newLow := make([]int32, length)

	var i int32
	for i = 0; i < length; i++ {
		position := positions[offset+i]
		ir.checkReadablePosition(position)
		if ir.hasNullValue {
			newValueIsNull[i] = ir.valueIsNull[position]
		}
		newHigh[i] = ir.high[position]
		newLow[i] = ir.low[position]
	}
	return NewInt96ArrayBlock2(0, length, newValueIsNull, newHigh, newLow)
}

// @Override
func (ir *Int96ArrayBlockBuilder) GetRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT96_NULL_VALUE_BLOCK, length)
	}
	if ir.hasNullValue {
		return NewInt96ArrayBlock2(positionOffset, length, ir.valueIsNull, ir.high, ir.low)
	} else {
		return NewInt96ArrayBlock2(positionOffset, length, nil, ir.high, ir.low)
	}

}

// @Override
func (ir *Int96ArrayBlockBuilder) CopyRegion(positionOffset int32, length int32) Block {
	checkValidRegion(ir.GetPositionCount(), positionOffset, length)
	if !ir.hasNonNullValue {
		return NewRunLengthEncodedBlock(INT96_NULL_VALUE_BLOCK, length)
	}
	var newValueIsNull []bool = nil
	if ir.hasNullValue {
		newValueIsNull = compactBoolArray(ir.valueIsNull, positionOffset, length)
	}
	newHigh := compactInt64Array(ir.high, positionOffset, length)
	newLow := compactInt32Array(ir.low, positionOffset, length)
	return NewInt96ArrayBlock2(0, length, newValueIsNull, newHigh, newLow)
}

func (ir *Int96ArrayBlockBuilder) checkReadablePosition(position int32) {
	if position < 0 || position >= ir.GetPositionCount() {
		panic("position is not valid")
	}
}
func (ak *Int96ArrayBlockBuilder) GetLoadedBlock() Block {
	return ak
}
func (ik *Int96ArrayBlockBuilder) IsLoaded() bool {
	return true
}

func (ik *Int96ArrayBlockBuilder) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
