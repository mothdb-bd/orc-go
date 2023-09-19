package block

import (
	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var rowTypemEGAMORPHIC_FIELD_COUNT int32 = 64

type RowType struct {
	// 继承
	AbstractType

	// private final List<Field> fields;
	// private final List<Type> fieldTypes;
	fields     *util.ArrayList[*Field]
	fieldTypes *util.ArrayList[Type]
	comparable bool
	orderable  bool
}

func NewRowType(typeSignature *TypeSignature, fields *util.ArrayList[*Field]) *RowType {
	re := new(RowType)
	re.signature = typeSignature
	re.goKind = BLOCK_KIND

	re.AbstractType = *NewAbstractType(re.signature, re.goKind)

	re.fields = fields

	// re.fieldTypes = fields.stream().map(Field.getType).collect(Lists.toUnmodifiableList())
	fieldTypes := make([]Type, fields.Size())
	// re.comparable = fields.stream().allMatch(func(field interface{}) {
	// 	field.getType().isComparable()
	// })
	var comparable bool = true

	// re.orderable = fields.stream().allMatch(func(field interface{}) {
	// 	field.getType().isOrderable()
	// })
	var orderable bool = true
	for i := 0; i < fields.Size(); i++ {
		f := fields.Get(i)
		fieldTypes[i] = f.GetType()
		if !f.GetType().IsComparable() {
			comparable = false
		}
		if !f.GetType().IsOrderable() {
			orderable = false
		}

	}
	re.fieldTypes = util.NewArrayList(fieldTypes...)

	re.comparable = comparable
	re.orderable = orderable
	return re
}

func From(fields *util.ArrayList[*Field]) *RowType {
	return NewRowType(makeSignature(fields), fields)
}

func Anonymous(types *util.ArrayList[Type]) *RowType {
	// fields := types.stream().map(func(type interface{}) {
	// 	NewField(Optional.empty(), type)
	// }).collect(Lists.toUnmodifiableList())
	// return NewRowType(makeSignature(fields), fields)
	fields := util.NewArrayList[*Field]()
	for i := 0; i < types.Size(); i++ {
		t := types.Get(i)
		fields.Add(NewField(optional.Empty[string](), t))
	}
	return NewRowType(makeSignature(fields), fields)
}

func CreateRowType(field ...*Field) *RowType {
	return From(util.NewArrayList(field...))
}

func AnonymousRow(types ...Type) *RowType {
	return Anonymous(util.NewArrayList(types...))
}

func CreateWithTypeSignature(typeSignature *TypeSignature, fields *util.ArrayList[*Field]) *RowType {
	return NewRowType(typeSignature, fields)
}

func CreateField(name string, kind Type) *Field {
	return NewField(optional.Of(name), kind)
}

func CreateField2(kind Type) *Field {
	return NewField(optional.Empty[string](), kind)
}

func makeSignature(fields *util.ArrayList[*Field]) *TypeSignature {
	size := fields.Size()
	if size == 0 {
		panic("Row type must have at least 1 field")
	}

	// parameters := fields.stream().map(func(field interface{}) {
	// 	NewNamedTypeSignature(field.getName().map(RowFieldName.new), field.getType().getTypeSignature())
	// }).map(TypeSignatureParameter.namedTypeParameter).collect(Lists.toUnmodifiableList())
	// return NewTypeSignature(ROW, parameters)

	parameters := util.NewArrayList[*TypeSignatureParameter]()
	for i := 0; i < fields.Size(); i++ {
		f := fields.Get(i)
		parameters.Add(NamedTypeParameter(NewNamedTypeSignature(optional.Of(NewRowFieldName(f.GetName().Get())), f.GetType().GetTypeSignature())))
	}
	return NewTypeSignature2(ST_ROW, parameters)
}

// @Override
func (re *RowType) CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder {
	return NewRowBlockBuilder(re.GetTypeParameters(), blockBuilderStatus, expectedEntries)
}

// @Override
func (re *RowType) CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder {
	return NewRowBlockBuilder(re.GetTypeParameters(), blockBuilderStatus, expectedEntries)
}

// @Override
func (re *RowType) GetDisplayName() string {
	result := util.NewSB()
	result.AppendString(ST_ROW).AppendChar('(')
	for i := 0; i < re.fields.Size(); i++ {
		field := re.fields.Get(i)
		typeDisplayName := field.GetType().GetDisplayName()
		if field.GetName().IsPresent() {
			result.AppendString(field.GetName().Get()).AppendChar(' ').AppendString(typeDisplayName)
		} else {
			result.AppendString(typeDisplayName)
		}
		result.AppendString(", ")
	}

	result.SetLength(result.Length() - 2)
	result.AppendChar(')')
	return result.String()
}

// @Override
func (re *RowType) AppendTo(block Block, position int32, blockBuilder BlockBuilder) {
	if block.IsNull(position) {
		blockBuilder.AppendNull()
	} else {
		re.WriteObject(blockBuilder, re.GetObject(block, position))
	}
}

// @Override
func (re *RowType) GetObject(block Block, position int32) basic.Object {
	return block.GetObject(position, BLOCK_TYPE)
}

// @Override
func (re *RowType) WriteObject(blockBuilder BlockBuilder, value basic.Object) {
	rowBlock := value.(Block)
	entryBuilder := blockBuilder.BeginBlockEntry()
	var i int32
	for i = 0; i < rowBlock.GetPositionCount(); i++ {
		re.fields.GetByInt32(i).GetType().AppendTo(rowBlock, i, entryBuilder)
	}
	blockBuilder.CloseEntry()
}

// @Override
func (re *RowType) GetTypeParameters() *util.ArrayList[Type] {
	return re.fieldTypes
}

func (re *RowType) GetFields() *util.ArrayList[*Field] {
	return re.fields
}

// @Override
func (te *RowType) Equals(kind Type) bool {
	return basic.ObjectEqual(te, kind)
}

type Field struct {
	kind Type
	name *optional.Optional[string]
}

func NewField(name *optional.Optional[string], kind Type) *Field {
	fd := new(Field)
	fd.kind = kind
	fd.name = name
	return fd
}

func (fd *Field) GetType() Type {
	return fd.kind
}

func (fd *Field) GetName() *optional.Optional[string] {
	return fd.name
}

// @Override
func (re *RowType) IsComparable() bool {
	return re.comparable
}

// @Override
func (re *RowType) IsOrderable() bool {
	return re.orderable
}
