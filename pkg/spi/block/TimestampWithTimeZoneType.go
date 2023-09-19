package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	TIMESTAMP_WITHTIMEZONE_MAX_PRECISION       int32                        = 12
	TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION int32                        = 3
	TIMESTAMP_WITHTIMEZONE_DEFAULT_PRECISION   int32                        = 3
	TIMESTAMP_WITHTIMEZONE_TYPES               []ITimestampWithTimeZoneType = make([]ITimestampWithTimeZoneType, TIMESTAMP_WITHTIMEZONE_MAX_PRECISION+1)

	TZ_INIT              bool                       = tz_initValues()
	TIMESTAMP_TZ_SECONDS ITimestampWithTimeZoneType = CreateTimestampWithTimeZoneType(0)
	TIMESTAMP_TZ_MILLIS  ITimestampWithTimeZoneType = CreateTimestampWithTimeZoneType(3)
	TIMESTAMP_TZ_MICROS  ITimestampWithTimeZoneType = CreateTimestampWithTimeZoneType(6)
	TIMESTAMP_TZ_NANOS   ITimestampWithTimeZoneType = CreateTimestampWithTimeZoneType(9)
	TIMESTAMP_TZ_PICOS   ITimestampWithTimeZoneType = CreateTimestampWithTimeZoneType(12)

	//@Deprecated
	TIMESTAMP_WITH_TIME_ZONE ITimestampWithTimeZoneType = TIMESTAMP_TZ_MILLIS
)

type ITimestampWithTimeZoneType interface {
	// 继承
	Type
	// 继承
	FixedWidthType

	GetPrecision() int32
}

type TimestampWithTimeZoneType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType

	precision int32
}

func tz_initValues() bool {
	var precision int32
	for precision = 0; precision <= TIMESTAMP_WITHTIMEZONE_MAX_PRECISION; precision++ {
		if precision <= TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION {
			TIMESTAMP_WITHTIMEZONE_TYPES[precision] = NewShortTimestampWithTimeZoneType(precision)
		} else {
			TIMESTAMP_WITHTIMEZONE_TYPES[precision] = NewLongTimestampWithTimeZoneType(precision)
		}
		// TIMESTAMP_WITHTIMEZONE_TYPES[precision] = util.If((precision <= TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION), NewShortTimestampWithTimeZoneType(precision), NewLongTimestampWithTimeZoneType(precision)).(*TimestampWithTimeZoneType)
	}
	return true
}

func CreateTimestampWithTimeZoneType(precision int32) ITimestampWithTimeZoneType {
	if precision < 0 || precision > TIMESTAMP_WITHTIMEZONE_MAX_PRECISION {
		panic(fmt.Sprintf("TIMESTAMP WITH TIME ZONE precision must be in range [0, %d]: %d", TIMESTAMP_WITHTIMEZONE_MAX_PRECISION, precision))
	}
	return TIMESTAMP_WITHTIMEZONE_TYPES[precision]
}
func NewTimestampWithTimeZoneType(precision int32, goKind reflect.Kind) *TimestampWithTimeZoneType {
	te := new(TimestampWithTimeZoneType)

	te.signature = NewTypeSignature(ST_TIMESTAMP_WITH_TIME_ZONE, NumericParameter(int64(precision)))
	te.goKind = goKind
	if precision < 0 || precision > TIMESTAMP_WITHTIMEZONE_MAX_PRECISION {
		panic(fmt.Sprintf("Precision must be in the range [0, %d]", TIMESTAMP_WITHTIMEZONE_MAX_PRECISION))
	}
	te.precision = precision

	te.AbstractType = *NewAbstractType(te.signature, te.goKind)
	return te
}

func (te *TimestampWithTimeZoneType) GetPrecision() int32 {
	return te.precision
}

func (te *TimestampWithTimeZoneType) IsShort() bool {
	return te.precision <= TIMESTAMP_WITHTIMEZONE_MAX_SHORT_PRECISION
}

// @Override
func (te *TimestampWithTimeZoneType) IsComparable() bool {
	return true
}

// @Override
func (te *TimestampWithTimeZoneType) IsOrderable() bool {
	return true
}

// 继承Type
// @Override
func (te *TimestampWithTimeZoneType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *TimestampWithTimeZoneType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *TimestampWithTimeZoneType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *TimestampWithTimeZoneType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *TimestampWithTimeZoneType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *TimestampWithTimeZoneType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *TimestampWithTimeZoneType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *TimestampWithTimeZoneType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
}

// @Override
func (te *TimestampWithTimeZoneType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *TimestampWithTimeZoneType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *TimestampWithTimeZoneType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *TimestampWithTimeZoneType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *TimestampWithTimeZoneType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *TimestampWithTimeZoneType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *TimestampWithTimeZoneType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	te.AbstractType.AppendTo(block, position, blockBuilder)
}

// @Override
func (te *TimestampWithTimeZoneType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
