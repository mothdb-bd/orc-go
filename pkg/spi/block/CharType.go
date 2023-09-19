package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	CHAR_MAX_LENGTH int32 = 65_536
)

type CharType struct {
	// 继承
	AbstractVariableWidthType

	length int32
}

func CreateCharType(length int64) *CharType {
	return NewCharType(length)
}

func NewCharType(length int64) *CharType {
	ce := new(CharType)
	// NewCharType(NewTypeSignature(StandardTypes.CHAR, singletonList(TypeSignatureParameter.numericParameter(length))), Slice.class)
	ce.signature = NewTypeSignature(ST_CHAR, NumericParameter(length))
	ce.goKind = reflect.TypeOf(&slice.Slice{}).Kind()

	if length < 0 || length > int64(CHAR_MAX_LENGTH) {
		panic(fmt.Sprintf("CHAR length must be in range [0, %d], got %d", CHAR_MAX_LENGTH, length))
	}
	ce.length = int32(length)

	ce.AbstractType = *NewAbstractType(ce.signature, ce.goKind)
	return ce
}

func (ce *CharType) GetLength() int32 {
	return ce.length
}

// @Override
func (ce *CharType) IsComparable() bool {
	return true
}

// @Override
func (ce *CharType) IsOrderable() bool {
	return true
}

// @Override
func (ce *CharType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	bb := ce.AbstractVariableWidthType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, maths.MinInt32(ce.length, EXPECTED_BYTES_PER_ENTRY))
	return bb
}

// @Override
func (ce *CharType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {

	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		block.WriteBytesTo(position, 0, block.GetSliceLength(position), blockBuilder)
		blockBuilder.CloseEntry()
	}

}

// @Override
func (ce *CharType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

func (ce *CharType) WriteString(blockBuilder BlockBuilder, value string) {
	s := slice.NewWithString(value)
	ce.WriteSlice(blockBuilder, s)
}

// @Override
func (ce *CharType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	ce.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (ce *CharType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	b, _ := value.GetByte(int(offset+length) - 1)
	if length > 0 && b == ' ' {
		panic("Slice representing Char should not have trailing spaces")
	}
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}

// 继承Type
// @Override
func (te *CharType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *CharType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *CharType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *CharType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *CharType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *CharType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *CharType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *CharType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *CharType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *CharType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *CharType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *CharType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *CharType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *CharType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *CharType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *CharType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
