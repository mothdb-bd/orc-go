package block

import "github.com/mothdb-bd/orc-go/pkg/slice"

type BlockBuilder interface {
	// 继承 Block
	Block

	/**
	 * Write a byte to the current entry;
	 */
	WriteByte(value byte) BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Write a short to the current entry;
	 */
	WriteShort(value int16) BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Write a int to the current entry;
	 */
	WriteInt(value int32) BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Write a long to the current entry;
	 */
	WriteLong(value int64) BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Write a byte sequences to the current entry;
	 */
	WriteBytes(source *slice.Slice, sourceIndex int32, length int32) BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
	 * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
	 */
	BeginBlockEntry() BlockBuilder
	// {
	//     throw new UnsupportedOperationException(getClass().getName());
	// }

	/**
	 * Create a new block from the current materialized block by keeping the same elements
	 * only with respect to {@code visiblePositions}.
	 */
	GetPositions(visiblePositions []int32, offset int32, length int32) Block
	// {
	//     return build().getPositions(visiblePositions, offset, length);
	// }

	/**
	 * Write a byte to the current entry;
	 */
	CloseEntry() BlockBuilder

	/**
	 * Appends a null value to the block.
	 */
	AppendNull() BlockBuilder

	/**
	 * Builds the block. This method can be called multiple times.
	 */
	Build() Block

	/**
	 * Creates a new block builder of the same type based on the current usage statistics of this block builder.
	 */
	NewBlockBuilderLike(blockBuilderStatus *BlockBuilderStatus) BlockBuilder
}
