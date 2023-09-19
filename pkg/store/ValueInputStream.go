package store

type IValueInputStream interface {
	Skip(items int64)

	String() string
	SeekToCheckpoint(checkpoint StreamCheckpoint)
}

// public interface ValueInputStream<C extends StreamCheckpoint>
// checkpoint.StreamCheckpoint
type ValueInputStream[T StreamCheckpoint] interface {
	// 继承
	IValueInputStream

	// seekToCheckpoint(checkpoint *C)
	// SeekToCheckpoint(checkpoint *StreamCheckpoint)

	Skip(items int64)

	String() string
}
