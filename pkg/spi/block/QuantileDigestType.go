package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type QuantileDigestType struct {
	// 继承
	AbstractVariableWidthType

	valueType Type
}

func NewQuantileDigestType(valueType Type) *QuantileDigestType {
	qe := new(QuantileDigestType)
	qe.signature = NewTypeSignature(ST_QDIGEST, TSP_TypeParameter(valueType.GetTypeSignature()))
	qe.goKind = slice.SLICE_KIND
	qe.valueType = valueType
	qe.AbstractType = *NewAbstractType(qe.signature, qe.goKind)
	return qe
}

// @Override
func (qe *QuantileDigestType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		block.WriteBytesTo(position, 0, block.GetSliceLength(position), blockBuilder)
		blockBuilder.CloseEntry()
	}
}

// @Override
func (qe *QuantileDigestType) GetSlice(block Block, position int32) *slice.Slice {
	return block.GetSlice(position, 0, block.GetSliceLength(position))
}

// @Override
func (qe *QuantileDigestType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	qe.WriteSlice2(blockBuilder, value, 0, int32(value.Size()))
}

// @Override
func (qe *QuantileDigestType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	blockBuilder.WriteBytes(value, offset, length).CloseEntry()
}

func (qe *QuantileDigestType) GetValueType() Type {
	return qe.valueType
}

// @Override
func (qe *QuantileDigestType) GetTypeParameters() *util.ArrayList[Type] {
	return util.NewArrayList(qe.valueType)
}

// @Override
func (qe *QuantileDigestType) GetBaseName() string {
	return qe.AbstractType.GetBaseName()
}

// @Override
func (qe *QuantileDigestType) GetBoolean(block Block, position int32) bool {
	return qe.AbstractType.GetBoolean(block, position)
}

// @Override
func (qe *QuantileDigestType) GetDisplayName() string {
	return qe.AbstractType.GetDisplayName()
}

// @Override
func (qe *QuantileDigestType) GetLong(block Block, position int32) int64 {
	return qe.AbstractType.GetLong(block, position)
}

// @Override
func (qe *QuantileDigestType) GetDouble(block Block, position int32) float64 {
	return qe.AbstractType.GetDouble(block, position)
}

// @Override
func (qe *QuantileDigestType) GetObject(block Block, position int32) basic.Object {
	return qe.AbstractType.GetObject(block, position)
}

// @Override
func (qe *QuantileDigestType) GetGoKind() reflect.Kind {
	return qe.AbstractType.GetGoKind()
}

// @Override
func (qe *QuantileDigestType) GetTypeSignature() *TypeSignature {
	return qe.AbstractType.GetTypeSignature()
}

// @Override
func (qe *QuantileDigestType) GetTypeId() *TypeId {
	return qe.AbstractType.GetTypeId()
}

// @Override
func (qe *QuantileDigestType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	qe.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (qe *QuantileDigestType) WriteLong(blockBuilder BlockBuilder, value int64) {
	qe.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (qe *QuantileDigestType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	qe.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (qe *QuantileDigestType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	qe.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (qe *QuantileDigestType) IsComparable() bool {
	return qe.AbstractType.IsComparable()
}

// @Override
func (qe *QuantileDigestType) IsOrderable() bool {
	return qe.AbstractType.IsOrderable()
}
