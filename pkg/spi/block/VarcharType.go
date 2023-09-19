package block

import (
	"fmt"
	"math"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type VarcharType struct {
	// 继承
	AbstractVariableWidthType

	length int32
}

// var (
// 	VarcharTypeUNBOUNDED_LENGTH	int32		= Integer.MAX_VALUE
// 	VarcharTypeMAX_LENGTH		int32		= Integer.MAX_VALUE - 1
// 	VarcharTypeVARCHAR		*VarcharType	= NewVarcharType(UNBOUNDED_LENGTH)
// )

var (
	VARCHAR_UNBOUNDED_LENGTH int32        = math.MaxInt32
	VARCHAR_MAX_LENGTH       int32        = math.MaxInt32 - 1
	VARCHAR                  *VarcharType = NewVarcharType(VARCHAR_UNBOUNDED_LENGTH)
)

func CreateUnboundedVarcharType() *VarcharType {
	return VARCHAR
}

func CreateVarcharType(length int32) *VarcharType {
	if length > VARCHAR_MAX_LENGTH || length < 0 {
		panic(fmt.Sprintf("Invalid VARCHAR length %d", length))
	}
	return NewVarcharType(length)
}

func GetParametrizedVarcharSignature(param string) *TypeSignature {
	return NewTypeSignature(ST_VARCHAR, TypeVariable(param))
}

func NewVarcharType(length int32) *VarcharType {
	ve := new(VarcharType)
	ve.signature = NewTypeSignature(ST_VARCHAR, NumericParameter(int64(length)))
	ve.goKind = slice.SLICE_KIND
	// NewVarcharType(NewTypeSignature(StandardTypes.VARCHAR, singletonList(TypeSignatureParameter.numericParameter(length.(int64)))), Slice.class)
	if length < 0 {
		panic(fmt.Sprintf("Invalid VARCHAR length %d", length))
	}
	ve.length = length
	ve.AbstractType = *NewAbstractType(ve.signature, ve.goKind)
	return ve
}

func (ve *VarcharType) GetLength() *optional.Optional[int32] {
	if ve.IsUnbounded() {
		return optional.Empty[int32]()
	}
	return optional.Of(ve.length)
}

func (ve *VarcharType) GetBoundedLength() int32 {
	if ve.IsUnbounded() {
		panic("Cannot get size of unbounded VARCHAR.")
	}
	return ve.length
}

func (ve *VarcharType) IsUnbounded() bool {
	return ve.length == VARCHAR_UNBOUNDED_LENGTH
}

// @Override
func (ve *VarcharType) IsComparable() bool {
	return true
}

// @Override
func (ve *VarcharType) IsOrderable() bool {
	return true
}

// @Override
func (ve *VarcharType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	var nLen int32 = 0
	length := ve.GetLength()
	if length.IsPresent() {
		nLen = maths.MinInt32(length.Get(), EXPECTED_BYTES_PER_ENTRY)
	} else {
		nLen = EXPECTED_BYTES_PER_ENTRY
	}
	return ve.AbstractVariableWidthType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, nLen)
}

// @Override
func (ve *VarcharType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {

	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		block.WriteBytesTo(position, 0, block.GetSliceLength(position), blockBuilder)
		blockBuilder.CloseEntry()
	}
}

// @Override
func (ve *VarcharType) GetSlice(block Block, position int32) *slice.Slice {
	// return block.getSlice(position, 0, block.getSliceLength(position))
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

func (ve *VarcharType) WriteString(blockBuilder BlockBuilder, value string) {
	s := slice.NewWithString(value)
	ve.WriteSlice(blockBuilder, s)
}

// @Override
func (ve *VarcharType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	// writeSlice(blockBuilder, value, 0, value.length())
	ve.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (ve *VarcharType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	// blockBuilder.writeBytes(value, offset, length).closeEntry()
	b, _ := value.GetByte(int(offset+length) - 1)
	if length > 0 && b == ' ' {
		panic("Slice representing Char should not have trailing spaces")
	}
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}

// 继承Type
// @Override
func (te *VarcharType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *VarcharType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *VarcharType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *VarcharType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *VarcharType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *VarcharType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *VarcharType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *VarcharType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *VarcharType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *VarcharType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *VarcharType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *VarcharType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *VarcharType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *VarcharType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *VarcharType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *VarcharType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
