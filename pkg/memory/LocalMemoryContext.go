package memory

type LocalMemoryContext interface {
	GetBytes() int64

	/**
	 * When this method returns, the bytes tracked by this LocalMemoryContext has been updated.
	 * The returned future will tell the caller whether it should block before reserving more memory
	 * (which happens when the memory pools are low on memory).
	 * <p/>
	 * Note: Canceling the returned future will complete it immediately even though the memory pools are low
	 * on memory, and callers blocked on this future will proceed to allocating more memory from the exhausted
	 * pools, which will violate the protocol of Moth MemoryPool implementation.
	 */
	SetBytes(bytes int64)

	/**
	 * This method can return false when there is not enough memory available to satisfy a positive delta allocation
	 * ({@code bytes} is greater than the bytes tracked by this LocalMemoryContext).
	 * <p/>
	 *
	 * @return true if the bytes tracked by this LocalMemoryContext can be set to {@code bytes}.
	 */
	TrySetBytes(bytes int64) bool

	/**
	 * Closes this LocalMemoryContext. Once closed the bytes tracked by this LocalMemoryContext will be set to 0, and
	 * none of its methods (except {@code getBytes()}) can be called.
	 */
	Close()
}
