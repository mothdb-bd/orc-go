package block

import (
	"fmt"
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type IDecimalType interface {
	// 继承
	Type

	// 继承
	FixedWidthType

	IsShort() bool

	GetScale() int32
}

type DecimalType struct {
	// 继承
	AbstractType
	// 继承
	FixedWidthType

	precision int32
	scale     int32
}

var (
	DECIMAL_DEFAULT_SCALE     int32 = 0
	DECIMAL_DEFAULT_PRECISION int32 = DECIMAL_MAX_PRECISION
)

func CreateDecimalType(precision int32, scale int32) IDecimalType {
	if precision <= 0 || precision > DECIMAL_MAX_PRECISION {
		panic(fmt.Sprintf("DECIMAL precision must be in range [1, %d]: %d", DECIMAL_MAX_PRECISION, precision))
	}
	if scale < 0 || scale > precision {
		panic(fmt.Sprintf("DECIMAL scale must be in range [0, precision (%d)]: %d", precision, scale))
	}
	if precision <= DECIMAL_MAX_SHORT_PRECISION {
		return NewShortDecimalType(precision, scale)
	} else {
		return NewLongDecimalType(precision, scale)
	}
}

func CreateDecimalType2(precision int32) IDecimalType {
	return CreateDecimalType(precision, DECIMAL_DEFAULT_SCALE)
}

func CreateDecimalType3() IDecimalType {
	return CreateDecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE)
}

func NewDecimalType(precision int32, scale int32, goKind reflect.Kind) *DecimalType {
	de := new(DecimalType)
	de.signature = NewTypeSignature2(ST_DECIMAL, buildTypeParameters(precision, scale))
	de.goKind = goKind
	de.precision = precision
	de.scale = scale
	de.AbstractType = *NewAbstractType(de.signature, de.goKind)
	return de
}

// @Override
func (de *DecimalType) IsComparable() bool {
	return true
}

// @Override
func (de *DecimalType) IsOrderable() bool {
	return true
}

func (de *DecimalType) GetPrecision() int32 {
	return de.precision
}

func (de *DecimalType) GetScale() int32 {
	return de.scale
}

func (de *DecimalType) IsShort() bool {
	return de.precision <= DECIMAL_MAX_SHORT_PRECISION
}

func buildTypeParameters(precision int32, scale int32) *util.ArrayList[*TypeSignatureParameter] {
	return util.NewArrayList(NumericParameter(int64(precision)), NumericParameter(int64(scale)))
}

func DecimalCheckArgument(condition bool, format string, args ...basic.Object) {
	if !condition {
		panic(fmt.Sprintf(format, args))
	}
}

// @Override
func (de *DecimalType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return nil
}

// @Override
func (de *DecimalType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return nil
}

// @Override
func (de *DecimalType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {

}

// @Override
func (de *DecimalType) GetObject(block Block, position int32) basic.Object {
	return nil
}

// @Override
func (de *DecimalType) GetBaseName() string {
	return de.AbstractType.GetBaseName()
}

// @Override
func (de *DecimalType) GetBoolean(block Block, position int32) bool {
	return false
}

// @Override
func (de *DecimalType) GetDisplayName() string {
	return de.signature.ToString()
}

// @Override
func (de *DecimalType) GetDouble(block Block, position int32) float64 {
	return 0
}

// @Override
func (de *DecimalType) GetGoKind() reflect.Kind {
	return de.goKind
}

// @Override
func (de *DecimalType) GetLong(block Block, position int32) int64 {
	return 0
}

// @Override
func (de *DecimalType) WriteLong(blockBuilder BlockBuilder, value int64) {
}

// @Override
func (de *DecimalType) WriteDouble(blockBuilder BlockBuilder, value float64) {
}

// @Override
func (de *DecimalType) GetSlice(block Block, position int32) *slice.Slice {
	return nil
}

// @Override
func (de *DecimalType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
}

// @Override
func (de *DecimalType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
}

// @Override
func (de *DecimalType) GetTypeId() *TypeId {
	return OfTypeId(de.GetTypeSignature().ToString())
}

// @Override
func (de *DecimalType) GetTypeSignature() *TypeSignature {
	return de.signature
}

// @Override
func (de *DecimalType) GetTypeParameters() *util.ArrayList[Type] {
	return de.AbstractType.GetTypeParameters()
}

// @Override
func (de *DecimalType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
}
