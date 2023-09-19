package block

import (
	"fmt"
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SmallintType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType
}

var SMALLINT *SmallintType = NewSmallintType()

func NewSmallintType() *SmallintType {
	se := new(SmallintType)
	se.signature = NewTypeSignature(ST_SMALLINT)
	se.goKind = reflect.Int64
	se.AbstractType = *NewAbstractType(se.signature, se.goKind)
	return se
}

// @Override
func (se *SmallintType) GetFixedSize() int32 {
	return util.INT16_BYTES
}

// @Override
func (se *SmallintType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewShortArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/util.INT16_BYTES))
}

// @Override
func (se *SmallintType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return se.CreateBlockBuilder(blockBuilderStatus, expectedEntries, util.INT16_BYTES)
}

// @Override
func (se *SmallintType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewShortArrayBlockBuilder(nil, positionCount)
}

// @Override
func (se *SmallintType) IsComparable() bool {
	return true
}

// @Override
func (se *SmallintType) IsOrderable() bool {
	return true
}

// @Override
func (se *SmallintType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteShort(block.GetShort(position, 0)).CloseEntry()
	}
}

// @Override
func (se *SmallintType) GetLong(block Block, position int32) int64 {
	return int64(block.GetShort(position, 0))
}

// @Override
func (se *SmallintType) WriteLong(blockBuilder BlockBuilder, value int64) {
	if value > math.MaxInt16 {
		panic(fmt.Sprintf("Value %d exceeds MAX_SHORT", value))
	}
	if value < math.MinInt16 {
		panic(fmt.Sprintf("Value %d is less than MIN_SHORT", value))
	}
	blockBuilder.WriteShort(int16(value)).CloseEntry()
}

// @Override
// @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
func (se *SmallintType) Equals(other Type) bool {
	return basic.ObjectEqual(other, SMALLINT)
}

// 继承Type
// @Override
func (te *SmallintType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *SmallintType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *SmallintType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *SmallintType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *SmallintType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *SmallintType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *SmallintType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *SmallintType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *SmallintType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *SmallintType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *SmallintType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *SmallintType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *SmallintType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *SmallintType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

/**
 * Writes the Slice value into the {@code BlockBuilder}.
 */
// @Override
func (te *SmallintType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}
