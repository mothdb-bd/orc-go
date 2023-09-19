package store

import "github.com/mothdb-bd/orc-go/pkg/store/metadata"

/**
 *
 *
 */
type LongOutputStream interface {
	// 继承
	ValueOutputStream[LongStreamCheckpoint]

	WriteLong(value int64)
}

func CreateLengthOutputStream(compression metadata.CompressionKind, bufferSize int32) LongOutputStream {
	return NewLongOutputStreamV2(compression, bufferSize, false, metadata.LENGTH)
}
