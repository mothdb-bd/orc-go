package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	TIMESTAMP_MAX_PRECISION       int32            = 12
	TIMESTAMP_MAX_SHORT_PRECISION int32            = 6
	TIMESTAMP_DEFAULT_PRECISION   int32            = 3
	TIMESTAMP_TYPES               []ITimestampType = make([]ITimestampType, TIMESTAMP_MAX_PRECISION+1)
	INIT                          bool             = initValues()

	TIMESTAMP_SECONDS ITimestampType = CreateTimestampType(0)
	TIMESTAMP_MILLIS  ITimestampType = CreateTimestampType(3)
	TIMESTAMP_MICROS  ITimestampType = CreateTimestampType(6)
	TIMESTAMP_NANOS   ITimestampType = CreateTimestampType(9)
	TIMESTAMP_PICOS   ITimestampType = CreateTimestampType(12)

	//@Deprecated
	TIMESTAMP ITimestampType = TIMESTAMP_MILLIS
)

type ITimestampType interface {
	// 继承
	Type
	// 继承
	FixedWidthType

	GetPrecision() int32
}

type TimestampType struct {
	// 继承
	AbstractType

	// 继承
	FixedWidthType

	precision int32
}

func initValues() bool {
	var precision int32
	for precision = 0; precision <= TIMESTAMP_MAX_PRECISION; precision++ {
		if precision <= TIMESTAMP_MAX_SHORT_PRECISION {
			TIMESTAMP_TYPES[precision] = NewShortTimestampType(precision)
		} else {
			TIMESTAMP_TYPES[precision] = NewLongTimestampType(precision)
		}
		// TIMESTAMP_TYPES[precision] = util.If((precision <= TIMESTAMP_MAX_SHORT_PRECISION), NewShortTimestampType(precision), NewLongTimestampType(precision)).(*TimestampType)
	}
	return true
}

func CreateTimestampType(precision int32) ITimestampType {
	if precision < 0 || precision > TIMESTAMP_MAX_PRECISION {
		panic(fmt.Sprintf("TIMESTAMP precision must be in range [0, %d]: %d", TIMESTAMP_MAX_PRECISION, precision))
	}
	return TIMESTAMP_TYPES[precision]
}

func NewTimestampType(precision int32, goKind reflect.Kind) *TimestampType {
	te := new(TimestampType)
	te.signature = NewTypeSignature(ST_TIMESTAMP, NumericParameter(int64(precision)))
	te.goKind = goKind
	te.precision = precision
	te.AbstractType = *NewAbstractType(te.signature, te.goKind)
	return te
}

func (te *TimestampType) GetPrecision() int32 {
	return te.precision
}

func (te *TimestampType) IsShort() bool {
	return te.precision <= TIMESTAMP_MAX_SHORT_PRECISION
}

// @Override
func (te *TimestampType) IsComparable() bool {
	return true
}

// @Override
func (te *TimestampType) IsOrderable() bool {
	return true
}

// 继承Type
// @Override
func (te *TimestampType) GetTypeSignature() *TypeSignature {
	return te.AbstractType.GetTypeSignature()
}

// @Override
func (te *TimestampType) GetTypeId() *TypeId {
	return te.AbstractType.GetTypeId()
}

// @Override
func (te *TimestampType) GetBaseName() string {
	return te.AbstractType.GetBaseName()
}

// @Override
func (te *TimestampType) GetDisplayName() string {
	return te.AbstractType.GetDisplayName()
}

// @Override
func (te *TimestampType) GetGoKind() reflect.Kind {
	return te.AbstractType.GetGoKind()
}

// @Override
func (te *TimestampType) GetTypeParameters() *util.ArrayList[Type] {
	return te.AbstractType.GetTypeParameters()
}

// @Override
func (te *TimestampType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry)
}

// @Override
func (te *TimestampType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return te.AbstractType.CreateBlockBuilder2(blockBuilderStatus, expectedEntries)
}

// @Override
func (te *TimestampType) GetBoolean(block Block, position int32) bool {
	return te.AbstractType.GetBoolean(block, position)
}

// @Override
func (te *TimestampType) GetLong(block Block, position int32) int64 {
	return te.AbstractType.GetLong(block, position)
}

// @Override
func (te *TimestampType) GetDouble(block Block, position int32) float64 {
	return te.AbstractType.GetDouble(block, position)
}

// @Override
func (te *TimestampType) GetSlice(block Block, position int32) *slice.Slice {
	return te.AbstractType.GetSlice(block, position)
}

// @Override
func (te *TimestampType) GetObject(block Block, position int32) basic.Object {
	return te.AbstractType.GetObject(block, position)
}

// @Override
func (te *TimestampType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
	te.AbstractType.WriteBoolean(blockBuilder, value)
}

// @Override
func (te *TimestampType) WriteLong(blockBuilder BlockBuilder, value int64) {
	te.AbstractType.WriteLong(blockBuilder, value)
}

// @Override
func (te *TimestampType) WriteDouble(blockBuilder BlockBuilder, value float64) {
	te.AbstractType.WriteDouble(blockBuilder, value)
}

// @Override
func (te *TimestampType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
	te.AbstractType.WriteSlice(blockBuilder, value)
}

// @Override
func (te *TimestampType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
	te.AbstractType.WriteSlice2(blockBuilder, value, offset, length)
}

// @Override
func (te *TimestampType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	te.AbstractType.WriteObject(blockBuilder, value)
}

// @Override
func (te *TimestampType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	te.AbstractType.AppendTo(block, position, blockBuilder)
}

// @Override
func (te *TimestampType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}
