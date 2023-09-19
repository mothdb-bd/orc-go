package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractLongType struct {
	// 继承 FixedWidthType
	FixedWidthType

	// 继承 AbstractType
	AbstractType
}

func NewAbstractLongType(signature *TypeSignature) *AbstractLongType {
	ae := new(AbstractLongType)
	ae.signature = signature
	ae.goKind = reflect.Int64
	return ae
}

// @Override
func (ae *AbstractLongType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (ae *AbstractLongType) IsComparable() bool {
	return true
}

// @Override
func (ae *AbstractLongType) IsOrderable() bool {
	return true
}

// @Override
func (ae *AbstractLongType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (ae *AbstractLongType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, ae.GetFixedSize())
}

// @Override
func (ae *AbstractLongType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (ae *AbstractLongType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ae *AbstractLongType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}

	// return NewLongArrayBlockBuilder(blockBuilderStatus, Math.min(expectedEntries, maxBlockSizeInBytes/Long.BYTES))
	newBB := NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT64_BYTES))
	return newBB
}

// @Override
func (ae *AbstractLongType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	// return createBlockBuilder(blockBuilderStatus, expectedEntries, Long.BYTES)
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT32_BYTES)
}

// @Override
func (ae *AbstractLongType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}
