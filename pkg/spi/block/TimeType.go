package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TimeType struct {
	// 继承
	AbstractLongType

	precision int32
}

// var (
// 	TimeTypeMAX_PRECISION     int32       = 12
// 	TimeTypeDEFAULT_PRECISION int32       = 3
// 	timeTypetYPES             []*TimeType = make([]*TimeType, MAX_PRECISION+1)
// 	TimeTypeTIME_SECONDS      *TimeType   = createTimeType(0)
// 	TimeTypeTIME_MILLIS       *TimeType   = createTimeType(3)
// 	TimeTypeTIME_MICROS       *TimeType   = createTimeType(6)
// 	TimeTypeTIME_NANOS        *TimeType   = createTimeType(9)
// 	TimeTypeTIME_PICOS        *TimeType   = createTimeType(12) //@Deprecated
// 	TimeTypeTIME              *TimeType   = NewTimeType(DEFAULT_PRECISION)
// )

var (
	TIME_MAX_PRECISION     int32       = 12
	TIME_DEFAULT_PRECISION int32       = 3
	TIME_TYPES             []*TimeType = make([]*TimeType, TIME_MAX_PRECISION+1)
	TIME_SECONDS           *TimeType   = CreateTimeType(0)
	TIME_MILLIS            *TimeType   = CreateTimeType(3)
	TIME_MICROS            *TimeType   = CreateTimeType(6)
	TIME_NANOS             *TimeType   = CreateTimeType(9)
	TIME_PICOS             *TimeType   = CreateTimeType(12)

	//@Deprecated
	TIME *TimeType = NewTimeType(TIME_DEFAULT_PRECISION)
)

func init() {
	for precision := int32(0); precision <= TIME_MAX_PRECISION; precision++ {
		TIME_TYPES[precision] = NewTimeType(precision)
	}
}
func NewTimeType(precision int32) *TimeType {
	te := new(TimeType)
	// te.AbstractLongType = (NewTypeSignature(StandardTypes.TIME, TypeSignatureParameter.numericParameter(precision)))
	te.signature = NewTypeSignature(ST_TIME, NumericParameter(int64(precision)))
	te.goKind = reflect.Int64
	te.precision = precision
	te.AbstractType = *NewAbstractType(te.signature, te.goKind)
	return te
}

func CreateTimeType(precision int32) *TimeType {
	if precision < 0 || precision > TIME_MAX_PRECISION {
		panic(fmt.Sprintf("TIME precision must be in range [0, %d]: %d", TIME_MAX_PRECISION, precision))
	}
	return TIME_TYPES[precision]
}

func (te *TimeType) GetPrecision() int32 {
	return te.precision
}

// 继承Type
// @Override
func (te *TimeType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *TimeType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *TimeType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *TimeType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *TimeType) IsComparable() bool {
	return te.AbstractType.IsComparable()
}

// @Override
func (te *TimeType) IsOrderable() bool {
	return te.AbstractType.IsOrderable()
}

// @Override
func (te *TimeType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *TimeType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *TimeType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *TimeType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
}

// @Override
func (te *TimeType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *TimeType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *TimeType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *TimeType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *TimeType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *TimeType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *TimeType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *TimeType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *TimeType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *TimeType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *TimeType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *TimeType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	te.AbstractType.AppendTo(block, position, blockBuilder)
}

// @Override
func (te *TimeType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
