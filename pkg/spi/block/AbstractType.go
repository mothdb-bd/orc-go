package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/errors"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type AbstractType struct {
	//继承Type
	Type

	signature *TypeSignature
	goKind    reflect.Kind
}

func NewAbstractType(signature *TypeSignature, goKind reflect.Kind) *AbstractType {
	ae := new(AbstractType)
	ae.signature = signature
	ae.goKind = goKind
	return ae
}

// @Override
func (ae *AbstractType) GetTypeSignature() *TypeSignature {
	return ae.signature
}

// @Override
func (ae *AbstractType) GetTypeId() *TypeId {
	return OfTypeId(ae.GetTypeSignature().ToString())
}

// @Override
func (ae *AbstractType) GetBaseName() string {
	return ae.GetTypeSignature().GetBase()
}

// @Override
func (ae *AbstractType) GetDisplayName() string {
	return ae.signature.ToString()
}

// @Override
func (ae *AbstractType) GetGoKind() reflect.Kind {
	return ae.goKind
}

// @Override
func (ae *AbstractType) GetTypeParameters() *util.ArrayList[Type] {
	return util.NewArrayList[Type]()
}

// @Override
func (ae *AbstractType) IsComparable() bool {
	return false
}

// @Override
func (ae *AbstractType) IsOrderable() bool {
	return false
}

func (ae *AbstractType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return nil
}

func CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return nil
}

// @Override
func (ae *AbstractType) GetBoolean(block Block, position int32) bool {
	return false
}

// @Override
func (ae *AbstractType) WriteBoolean(blockBuilder BlockBuilder, value bool) {
}

// @Override
func (ae *AbstractType) GetLong(block Block, position int32) int64 {
	return 0
}

// @Override
func (ae *AbstractType) WriteLong(blockBuilder BlockBuilder, value int64) {
}

// @Override
func (ae *AbstractType) GetDouble(block Block, position int32) float64 {
	return 0
}

// @Override
func (ae *AbstractType) WriteDouble(blockBuilder BlockBuilder, value float64) {
}

// @Override
func (ae *AbstractType) GetSlice(block Block, position int32) *slice.Slice {
	return nil
}

// @Override
func (ae *AbstractType) WriteSlice(blockBuilder BlockBuilder, value *slice.Slice) {
}

// @Override
func (ae *AbstractType) WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32) {
}

// @Override
func (ae *AbstractType) GetObject(block Block, position int32) basic.Object {
	return nil
}

// @Override
func (ae *AbstractType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
}

func AppendTo(block Block, position int32, blockBuilder BlockBuilder) error {
	return errors.NewUnsupported("AppendTo")
}
