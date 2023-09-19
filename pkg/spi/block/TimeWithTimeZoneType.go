package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	TTZ_MAX_PRECISION       int32 = 12
	TTZ_MAX_SHORT_PRECISION int32 = 9
	TTZ_DEFAULT_PRECISION   int32 = 3 //@Deprecated

	t                       Type                  = NewShortTimeWithTimeZoneType(TTZ_DEFAULT_PRECISION)
	TTZ_TIME_WITH_TIME_ZONE ITimeWithTimeZoneType = t.(ITimeWithTimeZoneType)
)

type ITimeWithTimeZoneType interface {
	//继承Type
	Type
	// 继承
	FixedWidthType

	GetPrecision() int32

	IsShort() bool
}

type TimeWithTimeZoneType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType

	precision int32
}

func CreateTimeWithTimeZoneType(precision int32) ITimeWithTimeZoneType {
	if precision == TTZ_DEFAULT_PRECISION {
		return TTZ_TIME_WITH_TIME_ZONE
	}
	if precision < 0 || precision > TTZ_MAX_PRECISION {
		panic(fmt.Sprintf("TIME WITH TIME ZONE precision must be in range [0, %d]: %d", TTZ_MAX_PRECISION, precision))
	}

	if precision <= TTZ_MAX_SHORT_PRECISION {
		return NewShortTimeWithTimeZoneType(precision)
	}
	return NewLongTimeWithTimeZoneType(precision)
}

func NewTimeWithTimeZoneType(precision int32, goKind reflect.Kind) *TimeWithTimeZoneType {
	te := new(TimeWithTimeZoneType)

	te.signature = NewTypeSignature(ST_TIME_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	te.goKind = goKind
	te.precision = precision
	te.AbstractType = *NewAbstractType(te.signature, te.goKind)
	return te
}

func (te *TimeWithTimeZoneType) GetPrecision() int32 {
	return te.precision
}

func (te *TimeWithTimeZoneType) IsShort() bool {
	return te.precision <= TTZ_MAX_SHORT_PRECISION
}

// @Override
func (te *TimeWithTimeZoneType) IsComparable() bool {
	return true
}

// @Override
func (te *TimeWithTimeZoneType) IsOrderable() bool {
	return true
}

// 继承Type
// @Override
func (te *TimeWithTimeZoneType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *TimeWithTimeZoneType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *TimeWithTimeZoneType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *TimeWithTimeZoneType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *TimeWithTimeZoneType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *TimeWithTimeZoneType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *TimeWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *TimeWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
}

// @Override
func (te *TimeWithTimeZoneType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *TimeWithTimeZoneType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *TimeWithTimeZoneType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *TimeWithTimeZoneType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *TimeWithTimeZoneType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *TimeWithTimeZoneType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *TimeWithTimeZoneType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *TimeWithTimeZoneType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *TimeWithTimeZoneType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *TimeWithTimeZoneType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *TimeWithTimeZoneType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *TimeWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	te.AbstractType.AppendTo(block, position, blockBuilder)
}

// @Override
func (te *TimeWithTimeZoneType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
