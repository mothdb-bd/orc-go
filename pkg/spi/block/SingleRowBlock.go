package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SingleRowBlock struct {
	// 继承
	AbstractSingleRowBlock

	fieldBlocks []Block
	rowIndex    int32
}

var SINGLE_INSTANCE_SIZE int32 = util.SizeOf(&SingleRowBlock{})

func NewSingleRowBlock(rowIndex int32, fieldBlocks []Block) *SingleRowBlock {
	sk := new(SingleRowBlock)
	sk.rowIndex = rowIndex
	sk.fieldBlocks = fieldBlocks
	return sk
}

// @Override
func (sk *SingleRowBlock) getRawFieldBlocks() []Block {
	return sk.fieldBlocks
}

// @Override
func (sk *SingleRowBlock) getRawFieldBlock(fieldIndex int32) Block {
	return sk.fieldBlocks[fieldIndex]
}

// @Override
func (sk *SingleRowBlock) GetPositionCount() int32 {
	return int32(len(sk.fieldBlocks))
}

// @Override
func (sk *SingleRowBlock) GetSizeInBytes() int64 {
	sizeInBytes := util.INT64_ZERO
	for i := util.INT32_ZERO; i < int32(len(sk.fieldBlocks)); i++ {
		sizeInBytes += sk.getRawFieldBlock(i).GetRegionSizeInBytes(sk.getRowIndex(), 1)
	}
	return sizeInBytes
}

// @Override
func (sk *SingleRowBlock) GetRetainedSizeInBytes() int64 {
	retainedSizeInBytes := int64(SINGLE_INSTANCE_SIZE)
	for i := util.INT32_ZERO; i < int32(len(sk.fieldBlocks)); i++ {
		retainedSizeInBytes += sk.getRawFieldBlock(i).GetRetainedSizeInBytes()
	}
	return retainedSizeInBytes
}

// @Override
func (sk *SingleRowBlock) GetRowIndex() int32 {
	return sk.rowIndex
}

// @Override
func (sk *SingleRowBlock) ToString() string {
	return fmt.Sprintf("SingleRowBlock{numFields=%d}", len(sk.fieldBlocks))
}

// @Override
func (sk *SingleRowBlock) IsLoaded() bool {
	for _, fieldBlock := range sk.fieldBlocks {
		if !fieldBlock.IsLoaded() {
			return false
		}
	}
	return true
}

// @Override
func (sk *SingleRowBlock) GetLoadedBlock() Block {
	loadedFieldBlocks := ensureBlocksAreLoaded(sk.fieldBlocks)
	if basic.ObjectEqual(loadedFieldBlocks, sk.fieldBlocks) {
		return sk
	}
	return NewSingleRowBlock(sk.getRowIndex(), loadedFieldBlocks)
}
func (ik *SingleRowBlock) GetChildren() *util.ArrayList[Block] {
	return util.EMPTY_LIST[Block]()
}
