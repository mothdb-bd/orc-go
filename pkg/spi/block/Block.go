package block

import (
	"reflect"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	EMPTY_BLOCK = new(Block)
	BLOCK_TYPE  = reflect.TypeOf(EMPTY_BLOCK)
	BLOCK_KIND  = reflect.TypeOf(EMPTY_BLOCK).Kind()
)

type Block interface {

	/**
	 * Gets the length of the value at the {@code position}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	GetSliceLength(position int32) int32
	// {
	//     throw new UnsupportedOperationException();
	// }

	/**
	 * Gets a byte at {@code offset} in the value at {@code position}.
	 */
	GetByte(position int32, offset int32) byte
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets a little endian short at {@code offset} in the value at {@code position}.
	 */
	GetShort(position int32, offset int32) int16
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets a little endian int at {@code offset} in the value at {@code position}.
	 */
	GetInt(position int32, offset int32) int32
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets a little endian long at {@code offset} in the value at {@code position}.
	 */
	GetLong(position int32, offset int32) int64
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets a slice at {@code offset} in the value at {@code position}.
	 */
	GetSlice(position int32, offset int32, length int32) *slice.Slice
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets an object in the value at {@code position}.
	 */
	GetObject(position int32, clazz reflect.Type) basic.Object
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Is the byte sequences at {@code offset} in the value at {@code position} equal
	 * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	BytesEqual(position int32, offset int32, otherSlice *slice.Slice, otherOffset int32, length int32) bool
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Compares the byte sequences at {@code offset} in the value at {@code position}
	 * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	BytesCompare(position int32, offset int32, length int32, otherSlice *slice.Slice, otherOffset int32, otherLength int32) int32
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Appends the byte sequences at {@code offset} in the value at {@code position}
	 * to {@code blockBuilder}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	WriteBytesTo(position int32, offset int32, length int32, blockBuilder BlockBuilder)
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Is the byte sequences at {@code offset} in the value at {@code position} equal
	 * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
	 * in {@code otherBlock}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	Equals(position int32, offset int32, otherBlock Block, otherPosition int32, otherOffset int32, length int32) bool
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Calculates the hash code the byte sequences at {@code offset} in the
	 * value at {@code position}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	Hash(position int32, offset int32, length int32) int64
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Compares the byte sequences at {@code offset} in the value at {@code position}
	 * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
	 * in {@code otherBlock}.
	 * This method must be implemented if @{code getSlice} is implemented.
	 */
	CompareTo(leftPosition int32, leftOffset int32, leftLength int32, rightBlock Block, rightPosition int32, rightOffset int32, rightLength int32) int32
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Gets the value at the specified position as a single element block.  The method
	 * must copy the data into a new block.
	 * <p>
	 * This method is useful for operators that hold on to a single value without
	 * holding on to the entire block.
	 *
	 * @throws IllegalArgumentException if this position is not valid
	 */
	GetSingleValueBlock(position int32) Block

	/**
	 * Returns the number of positions in this block.
	 */
	GetPositionCount() int32

	/**
	 * Returns the size of this block as if it was compacted, ignoring any over-allocations
	 * and any unloaded nested blocks.
	 * For example, in dictionary blocks, this only counts each dictionary entry once,
	 * rather than each time a value is referenced.
	 */
	GetSizeInBytes() int64

	/**
	 * Returns the size of the block contents, regardless of internal representation.
	 * The same logical data values should always have the same size, no matter
	 * what block type is used or how they are represented within a specific block.
	 * <p>
	 * This can differ substantially from {@link #getSizeInBytes} for certain block
	 * types. For RLE, it will be {@code N} times larger. For dictionary, it will be
	 * larger based on how many times dictionary entries are reused.
	 */
	GetLogicalSizeInBytes() int64
	// {
	//     return getSizeInBytes();
	// }

	/**
	 * Returns the size of {@code block.getRegion(position, length)}.
	 * The method can be expensive. Do not use it outside an implementation of Block.
	 */
	GetRegionSizeInBytes(position int32, length int32) int64

	/**
	 * Returns the size of all positions marked true in the positions array.
	 * This is equivalent to multiple calls of {@code block.getRegionSizeInBytes(position, length)}
	 * where you mark all positions for the regions first.
	 */
	GetPositionsSizeInBytes(positions []bool) int64

	/**
	 * Returns the size of all positions marked true in the positions array.
	 * The 'selectedPositionsCount' variable may be used to skip iterating through
	 * the positions array in case this is a fixed-width block
	 */
	GetPositionsSizeInBytes2(positions []bool, selectedPositionsCount int32) int64
	// {
	//     return getPositionsSizeInBytes(positions);
	// }

	/**
	 * Returns the retained size of this block in memory, including over-allocations.
	 * This method is called from the inner most execution loop and must be fast.
	 */
	GetRetainedSizeInBytes() int64

	/**
	 * Returns the estimated in memory data size for stats of position.
	 * Do not use it for other purpose.
	 */
	GetEstimatedDataSizeForStats(position int32) int64

	/**
	 * Create a new block from the current block by keeping the same elements only with respect
	 * to {@code positions} that starts at {@code offset} and has length of {@code length}. The
	 * implementation may return a view over the data in this block or may return a copy, and the
	 * implementation is allowed to retain the positions array for use in the view.
	 */
	GetPositions(positions []int32, offset int32, length int32) Block
	// {
	//     checkArrayRange(positions, offset, length);

	//     return new DictionaryBlock(offset, length, this, positions, false, randomDictionaryId());
	// }

	/**
	 * Returns a block containing the specified positions.
	 * Positions to copy are stored in a subarray within {@code positions} array
	 * that starts at {@code offset} and has length of {@code length}.
	 * All specified positions must be valid for this block.
	 * <p>
	 * The returned block must be a compact representation of the original block.
	 */
	CopyPositions(positions []int32, offset int32, length int32) Block

	/**
	 * Returns a block starting at the specified position and extends for the
	 * specified length.  The specified region must be entirely contained
	 * within this block.
	 * <p>
	 * The region can be a view over this block.  If this block is released
	 * the region block may also be released.  If the region block is released
	 * this block may also be released.
	 */
	GetRegion(positionOffset int32, length int32) Block

	/**
	 * Returns a block starting at the specified position and extends for the
	 * specified length.  The specified region must be entirely contained
	 * within this block.
	 * <p>
	 * The region returned must be a compact representation of the original block, unless their internal
	 * representation will be exactly the same. This method is useful for
	 * operators that hold on to a range of values without holding on to the
	 * entire block.
	 */
	CopyRegion(position int32, length int32) Block

	/**
	 * Is it possible the block may have a null value?  If false, the block cannot contain
	 * a null, but if true, the block may or may not have a null.
	 */
	MayHaveNull() bool
	// {
	//     return true;
	// }

	/**
	 * Is the specified position null?
	 *
	 * @throws IllegalArgumentException if this position is not valid. The method may return false
	 * without throwing exception when there are no nulls in the block, even if the position is invalid
	 */
	IsNull(position int32) bool

	/**
	 * Returns true if block data is fully loaded into memory.
	 */
	IsLoaded() bool
	// {
	//     return true;
	// }

	/**
	 * Returns a fully loaded block that assures all data is in memory.
	 * Neither the returned block nor any nested block will be a {@link LazyBlock}.
	 * The same block will be returned if neither the current block nor any
	 * nested blocks are {@link LazyBlock},
	 * <p>
	 * This allows streaming data sources to skip sections that are not
	 * accessed in a query.
	 */
	GetLoadedBlock() Block
	// {
	//     return this;
	// }

	/**
	 * Gets the direct child blocks of this block.
	 */
	GetChildren() *util.ArrayList[Block]
	// {
	//     Collections.emptyList();
	// }
}
