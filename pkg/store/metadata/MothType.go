package metadata

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MothTypeKind int8

const (
	BOOLEAN MothTypeKind = iota
	BYTE
	SHORT
	INT
	LONG
	DECIMAL
	FLOAT
	DOUBLE
	STRING
	VARCHAR
	CHAR
	BINARY
	DATE
	TIMESTAMP
	TIMESTAMP_INSTANT
	LIST
	MAP
	STRUCT
	UNION
)

type MothType struct {
	mothTypeKind MothTypeKind
	// List<MothColumnId> fieldTypeIndexes;
	fieldTypeIndexes *util.ArrayList[MothColumnId]
	// List<String> fieldNames;
	fieldNames *util.ArrayList[string]
	length     *optional.Optional[int32]
	precision  *optional.Optional[int32]
	scale      *optional.Optional[int32]

	// Map<String, String> attributes;
	attributes map[string]string
}

func NewMothType(mothTypeKind MothTypeKind) *MothType {
	return NewMothType5(mothTypeKind, util.NewArrayList[MothColumnId](), util.NewArrayList[string](), optional.Empty[int32](), optional.Empty[int32](), optional.Empty[int32](), util.EMPTY_MAP)
}
func NewMothType2(mothTypeKind MothTypeKind, length int32) *MothType {
	return NewMothType5(mothTypeKind, util.NewArrayList[MothColumnId](), util.NewArrayList[string](), optional.Of(length), optional.Empty[int32](), optional.Empty[int32](), util.EMPTY_MAP)
}
func NewMothType3(mothTypeKind MothTypeKind, precision int32, scale int32) *MothType {
	return NewMothType5(mothTypeKind, util.NewArrayList[MothColumnId](), util.NewArrayList[string](), optional.Empty[int32](), optional.Of(precision), optional.Of(scale), util.EMPTY_MAP)
}
func NewMothType4(mothTypeKind MothTypeKind, fieldTypeIndexes *util.ArrayList[MothColumnId], fieldNames *util.ArrayList[string]) *MothType {
	return NewMothType5(mothTypeKind, fieldTypeIndexes, fieldNames, optional.Empty[int32](), optional.Empty[int32](), optional.Empty[int32](), util.EMPTY_MAP)
}
func NewMothType5(mothTypeKind MothTypeKind, fieldTypeIndexes *util.ArrayList[MothColumnId], fieldNames *util.ArrayList[string], length *optional.Optional[int32], precision *optional.Optional[int32], scale *optional.Optional[int32], attributes map[string]string) *MothType {
	me := new(MothType)
	me.mothTypeKind = mothTypeKind
	me.fieldTypeIndexes = fieldTypeIndexes
	if fieldNames == nil || (fieldNames.IsEmpty() && !fieldTypeIndexes.IsEmpty()) {
		me.fieldNames = nil
	} else {
		me.fieldNames = fieldNames
	}
	me.length = length
	me.precision = precision
	me.scale = scale
	me.attributes = attributes
	return me
}

func (me *MothType) GetMothTypeKind() MothTypeKind {
	return me.mothTypeKind
}

func (me *MothType) GetFieldCount() int32 {
	return int32(me.fieldTypeIndexes.Size())
}

func (me *MothType) GetFieldTypeIndex(field int32) MothColumnId {
	return me.fieldTypeIndexes.Get(int(field))
}

func (me *MothType) GetFieldTypeIndexes() *util.ArrayList[MothColumnId] {
	return me.fieldTypeIndexes
}

func (me *MothType) GetFieldName(field int32) string {
	return me.fieldNames.Get(int(field))
}

func (me *MothType) GetFieldNames() *util.ArrayList[string] {
	return me.fieldNames
}

func (me *MothType) GetLength() *optional.Optional[int32] {
	return me.length
}

func (me *MothType) GetPrecision() *optional.Optional[int32] {
	return me.precision
}

func (me *MothType) GetScale() *optional.Optional[int32] {
	return me.scale
}

func (me *MothType) GetAttributes() map[string]string {
	return me.attributes
}

func toMothType(nextFieldTypeIndex int32, kind block.Type) *util.ArrayList[*MothType] {
	_, bf := kind.(*block.BooleanType)
	if bf {
		return util.NewArrayList(NewMothType(BOOLEAN))
	}
	_, tf := kind.(*block.TinyintType)
	if tf {
		// return ImmutableList.of(NewMothType(BYTE))
		return util.NewArrayList(NewMothType(BYTE))
	}
	_, sf := kind.(*block.SmallintType)
	if sf {
		// return ImmutableList.of(NewMothType(SHORT))
		return util.NewArrayList(NewMothType(SHORT))
	}
	_, intf := kind.(*block.IntegerType)
	if intf {
		// return ImmutableList.of(NewMothType(INT))
		return util.NewArrayList(NewMothType(INT))
	}
	_, btf := kind.(*block.BigintType)
	if btf {
		// return ImmutableList.of(NewMothType(LONG))
		return util.NewArrayList(NewMothType(LONG))
	}
	_, df := kind.(*block.DoubleType)
	if df {
		// return ImmutableList.of(NewMothType(DOUBLE))
		return util.NewArrayList(NewMothType(DOUBLE))
	}
	_, rf := kind.(*block.RealType)
	if rf {
		// return ImmutableList.of(NewMothType(FLOAT))
		return util.NewArrayList(NewMothType(FLOAT))
	}

	// if kind instanceof VarcharType {
	// 	varcharType := kind.(*VarcharType)
	// }
	vt, flag := kind.(*block.VarcharType)
	if flag {
		if vt.IsUnbounded() {
			// return ImmutableList.of(NewMothType(MothTypeKind.STRING))
			return util.NewArrayList(NewMothType(STRING))
		}
		// return ImmutableList.of(NewMothType(MothTypeKind.VARCHAR, varcharType.getBoundedLength()))
		return util.NewArrayList(NewMothType2(VARCHAR, vt.GetBoundedLength()))
	}
	// if kind instanceof CharType {
	// 	return ImmutableList.of(NewMothType(MothTypeKind.CHAR, (kind.(*CharType)).getLength()))
	// }

	ct, flag := kind.(*block.CharType)
	if flag {
		// return ImmutableList.of(NewMothType(MothTypeKind.CHAR, (kind.(*CharType)).getLength()))
		return util.NewArrayList(NewMothType2(CHAR, ct.GetLength()))
	}
	if block.VARBINARY.Equals(kind) {
		return util.NewArrayList(NewMothType(BINARY))
	}
	_, varf := kind.(*block.VarcharType)
	if varf {
		// return ImmutableList.of(NewMothType(MothTypeKind.BINARY))
		return util.NewArrayList(NewMothType(BINARY))
	}
	_, datef := kind.(*block.DateType)
	if datef {
		// return ImmutableList.of(NewMothType(MothTypeKind.DATE))
		return util.NewArrayList(NewMothType(DATE))
	}

	if block.TIMESTAMP_MILLIS.Equals(kind) || block.TIMESTAMP_MICROS.Equals(kind) || block.TIMESTAMP_NANOS.Equals(kind) {
		// return ImmutableList.of(NewMothType(MothTypeKind.TIMESTAMP))
		return util.NewArrayList(NewMothType(TIMESTAMP))
	}
	if block.TIMESTAMP_TZ_MILLIS.Equals(kind) || block.TIMESTAMP_TZ_MICROS.Equals(kind) || block.TIMESTAMP_TZ_NANOS.Equals(kind) {
		// return ImmutableList.of(NewMothType(MothTypeKind.TIMESTAMP_INSTANT))
		return util.NewArrayList(NewMothType(TIMESTAMP_INSTANT))
	}

	dt, sflag := kind.(*block.ShortDecimalType)
	if sflag {
		// return ImmutableList.of(NewMothType(MothTypeKind.DECIMAL, decimalType.getPrecision(), decimalType.getScale()))
		return util.NewArrayList(NewMothType3(DECIMAL, dt.GetPrecision(), dt.GetScale()))
	}

	dt2, lflag := kind.(*block.LongDecimalType)
	if lflag {
		// return ImmutableList.of(NewMothType(MothTypeKind.DECIMAL, decimalType.getPrecision(), decimalType.getScale()))
		return util.NewArrayList(NewMothType3(DECIMAL, dt2.GetPrecision(), dt2.GetScale()))
	}

	// if kind instanceof ArrayType {
	// 	return createMothArrayType(nextFieldTypeIndex, kind.getTypeParameters().get(0))
	// }
	at, flag := kind.(*block.ArrayType)
	if flag {
		return createMothArrayType(nextFieldTypeIndex, at.GetTypeParameters().Get(0))
	}

	// if kind instanceof MapType {
	// 	return createMothMapType(nextFieldTypeIndex, kind.getTypeParameters().get(0), kind.getTypeParameters().get(1))
	// }
	mt, flag := kind.(*block.MapType)
	if flag {
		return createMothMapType(nextFieldTypeIndex, mt.GetTypeParameters().Get(0).(block.Type), mt.GetTypeParameters().Get(1))
	}
	// if kind instanceof RowType {
	// 	fieldNames := NewArrayList()
	// 	for i := 0; i < kind.getTypeSignature().getParameters().size(); i++ {
	// 		parameter := kind.getTypeSignature().getParameters().get(i)
	// 		fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i))
	// 	}
	// 	fieldTypes := kind.getTypeParameters()
	// 	return createMothRowType(nextFieldTypeIndex, fieldNames, fieldTypes)
	// }
	rt, flag := kind.(*block.RowType)
	if flag {
		fieldNames := util.NewArrayList[string]()
		for i := 0; i < rt.GetTypeSignature().GetParameters().Size(); i++ {
			parameter := rt.GetTypeSignature().GetParameters().Get(i)
			fieldNames.Add(parameter.GetNamedTypeSignature().GetName().OrElse("field" + strconv.Itoa(i)))
		}
		fieldTypes := rt.GetTypeParameters()
		return createMothRowType(nextFieldTypeIndex, fieldNames, fieldTypes)
	}

	panic(fmt.Sprintf("Unsupported moth type: %s", reflect.TypeOf(kind)))
}

// List<MothType> createMothArrayType(int nextFieldTypeIndex, block.Type itemType)
func createMothArrayType(nextFieldTypeIndex int32, itemType block.Type) *util.ArrayList[*MothType] {
	nextFieldTypeIndex++
	// List<MothType> itemTypes = toMothType(nextFieldTypeIndex, itemType);
	itemTypes := toMothType(nextFieldTypeIndex, itemType)
	// List<MothType> mothTypes = new ArrayList<>();
	mothTypes := util.NewArrayList[*MothType]()
	mothTypes.Add(NewMothType4(LIST, util.NewArrayList(NewMothColumnId(uint32(nextFieldTypeIndex))), util.NewArrayList("item")))
	mothTypes.AddAll(itemTypes)
	return mothTypes
}

// List<MothType> createMothMapType(int nextFieldTypeIndex, block.Type keyType, block.Type valueType)
func createMothMapType(nextFieldTypeIndex int32, keyType block.Type, valueType block.Type) *util.ArrayList[*MothType] {
	nextFieldTypeIndex++
	keyTypes := toMothType(nextFieldTypeIndex, keyType)
	valueTypes := toMothType(nextFieldTypeIndex+keyTypes.SizeInt32(), valueType)
	mothTypes := util.NewArrayList[*MothType]()
	mothTypes.Add(NewMothType4(MAP, util.NewArrayList(NewMothColumnId(uint32(nextFieldTypeIndex)), NewMothColumnId(uint32(nextFieldTypeIndex+keyTypes.SizeInt32()))), util.NewArrayList("key", "value")))
	mothTypes.AddAll(keyTypes)
	mothTypes.AddAll(valueTypes)
	return mothTypes
}

// ColumnMetadata<MothType> createRootMothType(List<String> fieldNames, List<Type> fieldTypes)
func CreateRootMothType(fieldNames *util.ArrayList[string], fieldTypes *util.ArrayList[block.Type]) *ColumnMetadata[*MothType] {
	return NewColumnMetadata(createMothRowType(0, fieldNames, fieldTypes))
}

// List<MothType> createMothRowType(int nextFieldTypeIndex, List<String> fieldNames, List<Type> fieldTypes)
func createMothRowType(nextFieldTypeIndex int32, fieldNames *util.ArrayList[string], fieldTypes *util.ArrayList[block.Type]) *util.ArrayList[*MothType] {
	nextFieldTypeIndex++

	// List<MothColumnId> fieldTypeIndexes = new ArrayList<>();
	fieldTypeIndexes := util.NewArrayList[MothColumnId]()

	// List<List<MothType>> fieldTypesList = new ArrayList<>();
	fieldTypesList := util.NewArrayList[*util.ArrayList[*MothType]]()

	for i := 0; i < fieldTypes.Size(); i++ {
		fieldType := fieldTypes.Get(i).(block.Type)

		fieldTypeIndexes.Add(NewMothColumnId(uint32(nextFieldTypeIndex)))
		// List<MothType> fieldMothTypes = toMothType(nextFieldTypeIndex, fieldType);
		fieldMothTypes := toMothType(nextFieldTypeIndex, fieldType)
		fieldTypesList.Add(fieldMothTypes)
		nextFieldTypeIndex += fieldMothTypes.SizeInt32()
	}

	// ImmutableList.Builder<MothType> mothTypes = ImmutableList.builder();
	mothTypes := util.NewArrayList[*MothType]()
	mothTypes.Add(NewMothType4(STRUCT, fieldTypeIndexes, fieldNames))

	for i := 0; i < fieldTypesList.Size(); i++ {
		chList := fieldTypesList.Get(i)
		mothTypes.AddAll(chList)
	}
	return mothTypes
}
