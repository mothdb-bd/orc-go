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

var DATE *DateType = NewDateType()

type DateType struct {
	// 继承
	AbstractIntType
}

func NewDateType() *DateType {
	de := new(DateType)
	de.signature = NewTypeSignature(ST_DATE)
	de.goKind = reflect.Int64

	de.AbstractType = *NewAbstractType(de.signature, de.goKind)
	return de
}

// @Override
// @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
func (de *DateType) Equals(other Type) bool {
	return basic.ObjectEqual(de, other)
}

// 继承Type

// @Override
func (ae *DateType) GetFixedSize() int32 {
	return util.INT32_BYTES
}

// @Override
func (ae *DateType) IsComparable() bool {
	return true
}

// @Override
func (ae *DateType) IsOrderable() bool {
	return true
}

// @Override
func (ae *DateType) GetLong(block Block, position int32) int64 {
	return int64(block.GetInt(position, 0))
}

// @Override
func (ae *DateType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, ae.GetFixedSize())
}

// @Override
func (ae *DateType) WriteLong(blockBuilder BlockBuilder, value int64) {
	if value > math.MaxInt32 {
		panic(errors.NewStandardError(errors.GENERIC_INTERNAL_ERROR, "Value "+strconv.FormatInt(int64(value), 10)+" exceeds MAX_INT"))
	}
	if value < math.MinInt32 {
		panic(errors.NewStandardError(errors.GENERIC_INTERNAL_ERROR, "Value "+strconv.FormatInt(int64(value), 10)+" is less than MIN_INT"))
	}
	blockBuilder.WriteInt(int32(value))
	blockBuilder.CloseEntry()
}

// @Override
func (ae *DateType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteInt(block.GetInt(position, 0))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ae *DateType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
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
func (ae *DateType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	// return createBlockBuilder(blockBuilderStatus, expectedEntries, Integer.BYTES)
	return ae.CreateBlockBuilder(blockBuilderStatus, expectedEntries, 4)
}

// @Override
func (ae *DateType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewIntArrayBlockBuilder(nil, positionCount)
}

// 继承Type
// @Override
func (te *DateType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *DateType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *DateType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *DateType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *DateType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *DateType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *DateType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *DateType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *DateType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *DateType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *DateType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *DateType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *DateType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *DateType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}
