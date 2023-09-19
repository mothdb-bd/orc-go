package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type Type interface {

	/**
	 * Gets the name of this type which must be case insensitive globally unique.
	 */
	GetTypeSignature() *TypeSignature

	GetTypeId() *TypeId
	// {
	//     return TypeId.of(getTypeSignature().toString());
	// }

	/**
	 * Returns the base name of this type. For simple types, it is the type name.
	 * For complex types (row, array, etc), it is the type name without any parameters.
	 */
	GetBaseName() string
	// {
	//     return getTypeSignature().getBase();
	// }

	/**
	 * Returns the name of this type that should be displayed to end-users.
	 */
	GetDisplayName() string

	/**
	 * True if the type supports equalTo and hash.
	 */
	IsComparable() bool

	/**
	 * True if the type supports compareTo.
	 */
	IsOrderable() bool

	/**
	 * Gets the Java class type used to represent this value on the stack during
	 * expression execution.
	 * <p>
	 * Currently, this can be {@code boolean}, {@code long}, {@code double}, or a non-primitive type.
	 */
	GetGoKind() reflect.Kind

	/**
	 * For parameterized types returns the list of parameters.
	 */
	GetTypeParameters() *util.ArrayList[Type]

	/**
	 * Creates the preferred block builder for this type. This is the builder used to
	 * store values after an expression projection within the query.
	 */
	CreateBlockBuilder(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32, expectedBytesPerEntry int32) BlockBuilder

	/**
	 * Creates the preferred block builder for this type. This is the builder used to
	 * store values after an expression projection within the query.
	 */
	CreateBlockBuilder2(blockBuilderStatus *BlockBuilderStatus, expectedEntries int32) BlockBuilder

	/**
	 * Gets the value at the {@code block} {@code position} as a boolean.
	 */
	GetBoolean(block Block, position int32) bool

	/**
	 * Gets the value at the {@code block} {@code position} as a long.
	 */
	GetLong(block Block, position int32) int64

	/**
	 * Gets the value at the {@code block} {@code position} as a double.
	 */
	GetDouble(block Block, position int32) float64

	/**
	 * Gets the value at the {@code block} {@code position} as a Slice.
	 */
	GetSlice(block Block, position int32) *slice.Slice

	/**
	 * Gets the value at the {@code block} {@code position} as an Object.
	 */
	GetObject(block Block, position int32) basic.Object

	/**
	 * Writes the boolean value into the {@code BlockBuilder}.
	 */
	WriteBoolean(blockBuilder BlockBuilder, value bool)

	/**
	 * Writes the long value into the {@code BlockBuilder}.
	 */
	WriteLong(blockBuilder BlockBuilder, value int64)

	/**
	 * Writes the double value into the {@code BlockBuilder}.
	 */
	WriteDouble(blockBuilder BlockBuilder, value float64)

	/**
	 * Writes the Slice value into the {@code BlockBuilder}.
	 */
	WriteSlice(blockBuilder BlockBuilder, value *slice.Slice)

	/**
	 * Writes the Slice value into the {@code BlockBuilder}.
	 */
	WriteSlice2(blockBuilder BlockBuilder, value *slice.Slice, offset int32, length int32)

	/**
	 * Writes the Object value into the {@code BlockBuilder}.
	 */
	WriteObject(blockBuilder BlockBuilder, value basic.Object)

	/**
	 * Append the value at {@code position} in {@code block} to {@code blockBuilder}.
	 */
	AppendTo(block Block, position int32, blockBuilder BlockBuilder)

	/**
	 * 判断是否相同
	 */
	Equals(kind Type) bool
}
