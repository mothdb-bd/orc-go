package block

import (
	"math"
	"reflect"
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/errors"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractIntType struct {
	// 继承
	AbstractType
	// 继承 FixedWidthType
	FixedWidthType
}

func NewAbstractIntType(signature *TypeSignature) *AbstractIntType {
	ae := new(AbstractIntType)
	ae.signature = signature
	ae.goKind = reflect.Int64
	return ae
}

// @Override
func (ae *AbstractIntType) GetFixedSize() int32 {
	return util.INT32_BYTES
}

// @Override
func (ae *AbstractIntType) IsComparable() bool {
	return true
}

// @Override
func (ae *AbstractIntType) IsOrderable() bool {
	return true
}

// @Override
func (ae *AbstractIntType) GetLong(block Block, position int32) int64 {
	return int64(block.GetInt(position, 0))
}

// @Override
func (ae *AbstractIntType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, ae.GetFixedSize())
}

// @Override
func (ae *AbstractIntType) WriteLong(blockBuilder BlockBuilder, value int64) error {
	if value > math.MaxInt32 {
		return errors.NewStandardError(errors.GENERIC_INTERNAL_ERROR, "Value "+strconv.FormatInt(int64(value), 10)+" exceeds MAX_INT")
	}
	if value < math.MinInt32 {
		return errors.NewStandardError(errors.GENERIC_INTERNAL_ERROR, "Value "+strconv.FormatInt(int64(value), 10)+" is less than MIN_INT")
	}
	blockBuilder.WriteInt(int32(value))
	blockBuilder.CloseEntry()
	return nil
}

// @Override
func (ae *AbstractIntType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteInt(block.GetInt(position, 0))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ae *AbstractIntType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}

	// return block.NewIntArrayBlockBuilder(blockBuilderStatus, Math.min(expectedEntries, maxBlockSizeInBytes/Integer.BYTES))
	newBB := NewIntArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT32_BYTES))
	return newBB
}

// @Override
func (ae *AbstractIntType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	// return createBlockBuilder(blockBuilderStatus, expectedEntries, Integer.BYTES)
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, 4)
}

// @Override
func (ae *AbstractIntType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewIntArrayBlockBuilder(nil, positionCount)
}

// 继承Type
// @Override
func (te *AbstractIntType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *AbstractIntType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *AbstractIntType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *AbstractIntType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *AbstractIntType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *AbstractIntType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *AbstractIntType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *AbstractIntType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *AbstractIntType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *AbstractIntType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *AbstractIntType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *AbstractIntType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *AbstractIntType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *AbstractIntType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}
