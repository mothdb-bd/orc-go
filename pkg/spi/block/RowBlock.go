package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/hashcode"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type RowBlock struct {
	// 继承
	AbstractRowBlock

	startOffset         int32
	positionCount       int32
	rowIsNull           []bool
	fieldBlockOffsets   []int32
	fieldBlocks         []Block
	sizeInBytes         int64
	retainedSizeInBytes int64
}

// var rowBlockiNSTANCE_SIZE int32 = ClassLayout.parseClass(RowBlock.class).instanceSize()
var ROW_BLOCK_iNSTANCE_SIZE int32 = util.SizeOf(&RowBlock{})

func FromFieldBlocks(positionCount int32, rowIsNullOptional *optional.Optional[[]bool], fieldBlocks []Block) Block {
	rowIsNull := rowIsNullOptional.OrElse(nil)
	var fieldBlockOffsets []int32 = nil
	if rowIsNull != nil {
		fieldBlockOffsets = make([]int32, positionCount+1)
		fieldBlockOffsets[0] = 0
		var position int32
		for position = 0; position < positionCount; position++ {
			fieldBlockOffsets[position+1] = fieldBlockOffsets[position] + (util.Ternary(rowIsNull[position], util.INT32_ZERO, 1))
		}
		if fieldBlockOffsets[positionCount] == positionCount {
			rowIsNull = nil
			fieldBlockOffsets = nil
		}
	}
	rowValidateConstructorArguments(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks)
	return NewRowBlock(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks)
}

func CreateRowBlockInternal(startOffset int32, positionCount int32, rowIsNull []bool, fieldBlockOffsets []int32, fieldBlocks []Block) *RowBlock {
	rowValidateConstructorArguments(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks)
	return NewRowBlock(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks)
}

func rowValidateConstructorArguments(startOffset int32, positionCount int32, rowIsNull []bool, fieldBlockOffsets []int32, fieldBlocks []Block) {
	if startOffset < 0 {
		panic("arrayOffset is negative")
	}
	if positionCount < 0 {
		panic("positionCount is negative")
	}
	if rowIsNull != nil && int32(len(rowIsNull))-startOffset < positionCount {
		panic("rowIsNull length is less than positionCount")
	}
	if (rowIsNull == nil) != (fieldBlockOffsets == nil) {
		panic("When rowIsNull is (non) null then fieldBlockOffsets should be (non) null as well")
	}
	if fieldBlockOffsets != nil && int32(len(fieldBlockOffsets))-startOffset < positionCount+1 {
		panic("fieldBlockOffsets length is less than positionCount")
	}
	if len(fieldBlocks) <= 0 {
		panic("Number of fields in RowBlock must be positive")
	}
	firstFieldBlockPositionCount := fieldBlocks[0].GetPositionCount()
	for i := 1; i < len(fieldBlocks); i++ {
		if firstFieldBlockPositionCount != fieldBlocks[i].GetPositionCount() {
			panic(fmt.Sprintf("length of field blocks differ: field 0: %d, block %d: %d", firstFieldBlockPositionCount, i, fieldBlocks[i].GetPositionCount()))
		}
	}
}

func NewRowBlock(startOffset int32, positionCount int32, rowIsNull []bool, fieldBlockOffsets []int32, fieldBlocks []Block) *RowBlock {
	rk := new(RowBlock)
	rk.numFields = int32(len(fieldBlocks))
	rk.startOffset = startOffset
	rk.positionCount = positionCount
	rk.rowIsNull = rowIsNull
	rk.fieldBlockOffsets = fieldBlockOffsets
	rk.fieldBlocks = fieldBlocks
	rk.retainedSizeInBytes = int64(ROW_BLOCK_iNSTANCE_SIZE + util.SizeOf(fieldBlockOffsets) + util.SizeOf(rowIsNull))
	return rk
}

// @Override
func (rk *RowBlock) getRawFieldBlocks() []Block {
	return rk.fieldBlocks
}

// @Override
// @Nullable
func (rk *RowBlock) getFieldBlockOffsets() []int32 {
	return rk.fieldBlockOffsets
}

// @Override
func (rk *RowBlock) getOffsetBase() int32 {
	return rk.startOffset
}

// @Override
// @Nullable
func (rk *RowBlock) getRowIsNull() []bool {
	return rk.rowIsNull
}

// @Override
func (rk *RowBlock) MayHaveNull() bool {
	return rk.rowIsNull != nil
}

// @Override
func (rk *RowBlock) GetPositionCount() int32 {
	return rk.positionCount
}

// @Override
func (rk *RowBlock) GetSizeInBytes() int64 {
	if rk.sizeInBytes >= 0 {
		return rk.sizeInBytes
	}
	sizeInBytes := rk.getBaseSizeInBytes()
	hasUnloadedBlocks := false
	startFieldBlockOffset := util.If(rk.fieldBlockOffsets != nil, rk.fieldBlockOffsets[rk.startOffset], rk.startOffset).(int32)
	endFieldBlockOffset := util.If(rk.fieldBlockOffsets != nil, rk.fieldBlockOffsets[rk.startOffset+rk.positionCount], rk.startOffset+rk.positionCount).(int32)
	fieldBlockLength := endFieldBlockOffset - startFieldBlockOffset
	for _, fieldBlock := range rk.fieldBlocks {
		sizeInBytes += fieldBlock.GetRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength)
		hasUnloadedBlocks = hasUnloadedBlocks || !fieldBlock.IsLoaded()
	}
	if !hasUnloadedBlocks {
		rk.sizeInBytes = sizeInBytes
	}
	return sizeInBytes
}

func (rk *RowBlock) getBaseSizeInBytes() int64 {
	return int64(INT32_BYTE_BYTES * rk.positionCount)
}

// @Override
func (rk *RowBlock) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := rk.retainedSizeInBytes
	for _, fieldBlock := range rk.fieldBlocks {
		retainedSizeInBytes += fieldBlock.GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}

// @Override
func (rk *RowBlock) IsLoaded() bool {
	for _, fieldBlock := range rk.fieldBlocks {
		if !fieldBlock.IsLoaded() {
			return false
		}
	}
	return true
}

// @Override
func (rk *RowBlock) GetLoadedBlock() Block {
	loadedFieldBlocks := ensureBlocksAreLoaded(rk.fieldBlocks)
	if hashcode.PointerEqual(loadedFieldBlocks, rk.fieldBlocks) {
		return rk
	}
	return CreateRowBlockInternal(rk.startOffset, rk.positionCount, rk.rowIsNull, rk.fieldBlockOffsets, loadedFieldBlocks)
}
func (ik *RowBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
