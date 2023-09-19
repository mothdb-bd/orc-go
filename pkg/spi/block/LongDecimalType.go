package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongDecimalType struct {
	// 继承
	DecimalType
}

func NewLongDecimalType(precision int32, scale int32) *LongDecimalType {
	le := new(LongDecimalType)
	// NewlongDecimalType(precision, scale, Int128.class)
	le.signature = NewTypeSignature2(ST_DECIMAL, buildTypeParameters(precision, scale))
	le.goKind = reflect.TypeOf(&Int128{}).Kind()
	le.precision = precision
	le.scale = scale
	le.AbstractType = *NewAbstractType(le.signature, le.goKind)
	return le
}

// @Override
func (le *LongDecimalType) GetFixedSize() int32 {
	return util.INT128_BYTES
}

// @Override
func (le *LongDecimalType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	var maxBlockSizeInBytes int32
	if blockBuilderStatus == nil {
		maxBlockSizeInBytes = DEFAULT_MAX_PAGE_SIZE_IN_BYTES
	} else {
		maxBlockSizeInBytes = blockBuilderStatus.GetMaxPageSizeInBytes()
	}
	return NewInt128ArrayBlockBuilder(blockBuilderStatus, maths.MinInt32(expectedEntries, maxBlockSizeInBytes/le.GetFixedSize()))
}

// @Override
func (le *LongDecimalType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return le.CreateBlockBuilder(blockBuilderStatus, expectedEntries, le.GetFixedSize())
}

// @Override
func (le *LongDecimalType) CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder {
	return NewInt128ArrayBlockBuilder(nil, positionCount)
}

// @Override
func (le *LongDecimalType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		blockBuilder.WriteLong(block.GetLong(position, 0))
		blockBuilder.WriteLong(block.GetLong(position, util.INT64_BYTES))
		blockBuilder.CloseEntry()
	}
}

// @Override
func (le *LongDecimalType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	decimal := value.(Int128)
	blockBuilder.WriteLong(int64(decimal.hi))
	blockBuilder.WriteLong(int64(decimal.lo))
	blockBuilder.CloseEntry()
}

// @Override
func (le *LongDecimalType) GetObject(block Block, position int32) basic.Object {
	return &Int128{
		hi: uint64(block.GetLong(position, 0)),
		lo: uint64(block.GetLong(position, util.INT64_BYTES)),
	}
}

// @Override
func (le *LongDecimalType) GetBaseName() string {
	return le.AbstractType.GetBaseName()
}

// @Override
func (le *LongDecimalType) GetBoolean(block Block, position int32) bool {
	return false
}

// @Override
func (le *LongDecimalType) GetDisplayName() string {
	return le.signature.ToString()
}

// @Override
func (le *LongDecimalType) GetDouble(block Block, position int32) float64 {
	return 0
}

// @Override
func (le *LongDecimalType) GetGoKind() reflect.Kind {
	return le.goKind
}

// @Override
func (le *LongDecimalType) GetLong(block Block, position int32) int64 {
	return 0
}

// @Override
func (le *LongDecimalType) WriteLong(blockBuilder BlockBuilder, value int64) {
}

// @Override
func (le *LongDecimalType) WriteDouble(blockBuilder BlockBuilder, value float64) {
}

// @Override
func (le *LongDecimalType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	le.WriteSlice2(blockBuilder, value, 0, value.Length())
}

// @Override
func (le *LongDecimalType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	if length != INT128_BYTES {
		panic(fmt.Sprintf("Expected entry size to be exactly %d but was %d", INT128_BYTES, length))
	}
	hi, _ := value.GetInt64LE(int(offset))
	lo, _ := value.GetInt64LE(int(offset + util.INT64_BYTES))
	blockBuilder.WriteLong(hi)
	blockBuilder.WriteLong(lo)
	blockBuilder.CloseEntry()
}

// @Override
func (le *LongDecimalType) GetSlice(block Block, position int32) *slice.Slice {
	s := slice.NewWithSize(2 * util.INT64_BYTES)
	s.WriteInt64LE(block.GetLong(position, 0))
	s.WriteInt64LE(block.GetLong(position, util.INT64_BYTES))
	return s
	// return Slices.wrappedLongArray(
	// 	block.getLong(position, 0),
	// 	block.getLong(position, SIZE_OF_LONG))
}

// @Override
func (le *LongDecimalType) GetTypeId() *TypeId {
	return OfTypeId(le.GetTypeSignature().ToString())
}

// @Override
func (le *LongDecimalType) GetTypeSignature() *TypeSignature {
	return le.signature
}

// @Override
func (le *LongDecimalType) GetTypeParameters() *util.ArrayList[Type] {
	return le.AbstractType.GetTypeParameters()
}

// @Override
func (le *LongDecimalType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
}
