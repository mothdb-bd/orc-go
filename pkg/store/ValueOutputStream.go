package store

import (
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// public interface ValueOutputStream<C extends StreamCheckpoint>
type ValueOutputStream[T StreamCheckpoint] interface {
	RecordCheckpoint()
	Close()
	GetCheckpoints() *util.ArrayList[T]
	GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput

	/**
	 * This method returns the size of the flushed data plus any unflushed data.
	 * If the output is compressed, flush data size is the size after compression.
	 */
	GetBufferedBytes() int64
	GetRetainedBytes() int64
	Reset()
}
