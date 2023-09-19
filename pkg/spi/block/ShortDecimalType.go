package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ShortDecimalType struct {
	// 继承
	DecimalType
}

func NewShortDecimalType(precision int32, scale int32) *ShortDecimalType {
	se := new(ShortDecimalType)
	se.signature = NewTypeSignature2(ST_DECIMAL, buildTypeParameters(precision, scale))
	se.goKind = reflect.Int64
	se.precision = precision
	se.scale = scale
	se.AbstractType = *NewAbstractType(se.signature, se.goKind)
	return se
}

// @Override
func (se *ShortDecimalType) GetFixedSize() int32 {
	return util.INT64_BYTES
}

// @Override
func (se *ShortDecimalType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewLongArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/se.GetFixedSize()))
}

// @Override
func (se *ShortDecimalType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return se.CreateBlockBuilder(blockBuilderStatus, expectedEntries, se.GetFixedSize())
}

// @Override
func (se *ShortDecimalType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewLongArrayBlockBuilder(nil, positionCount)
}

// @Override
func (se *ShortDecimalType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0)).CloseEntry()
	}
}

// @Override
func (se *ShortDecimalType) GetLong(block Block, position int32) int64 {
	return block.GetLong(position, 0)
}

// @Override
func (se *ShortDecimalType) WriteLong(blockBuilder BlockBuilder, value int64) {
	blockBuilder.WriteLong(value).CloseEntry()
}

// @Override
func (se *ShortDecimalType) GetBaseName() string {
	return se.AbstractType.GetBaseName()
}

// @Override
func (se *ShortDecimalType) GetBoolean(block Block, position int32) bool {
	return false
}

// @Override
func (se *ShortDecimalType) GetDisplayName() string {
	return se.signature.ToString()
}

// @Override
func (se *ShortDecimalType) GetDouble(block Block, position int32) float64 {
	return 0
}

// @Override
func (se *ShortDecimalType) GetGoKind() reflect.Kind {
	return se.goKind
}

// @Override
func (se *ShortDecimalType) WriteDouble(blockBuilder BlockBuilder, value float64) {
}

// @Override
func (se *ShortDecimalType) GetSlice(block Block, position int32) *slice.Slice {
	return nil
}

// @Override
func (se *ShortDecimalType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
}

// @Override
func (se *ShortDecimalType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
}

// @Override
func (se *ShortDecimalType) GetTypeId() *TypeId {
	return OfTypeId(se.GetTypeSignature().ToString())
}

// @Override
func (se *ShortDecimalType) GetTypeSignature() *TypeSignature {
	return se.signature
}

// @Override
func (se *ShortDecimalType) GetTypeParameters() *util.ArrayList[Type] {
	return se.AbstractType.GetTypeParameters()
}

// @Override
func (se *ShortDecimalType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
}

// @Override
func (se *ShortDecimalType) GetObject(block Block, position int32) basic.Object {
	return nil
}

// @Override
func (se *ShortDecimalType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
}
