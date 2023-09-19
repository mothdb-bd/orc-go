package block

type FixedWidthType interface {
	// 继承Type
	Type

	/**
	 * Gets the size of a value of this type in bytes. All values
	 * of a FixedWidthType are the same size.
	 */
	GetFixedSize() int32

	/**
	 * Creates a block builder for this type sized to hold the specified number
	 * of positions.
	 */
	CreateFixedSizeBlockBuilder(positionCount int32) BlockBuilder
}
