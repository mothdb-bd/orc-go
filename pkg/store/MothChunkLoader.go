package store

import (
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

func CreateChunkLoader(mothDataSourceId *common.MothDataSourceId, chunk *slice.Slice, decompressor *optional.Optional[MothDecompressor], memoryContext memory.AggregatedMemoryContext) MothChunkLoader {
	return CreateChunkLoader2(NewMemoryMothDataReader(mothDataSourceId, chunk, int64(chunk.Size())), decompressor, memoryContext)
}

func CreateChunkLoader2(dataReader MothDataReader, decompressor *optional.Optional[MothDecompressor], memoryContext memory.AggregatedMemoryContext) MothChunkLoader {
	if decompressor.IsPresent() {
		return NewCompressedMothChunkLoader(dataReader, decompressor.Get(), memoryContext)
	}
	return NewUncompressedMothChunkLoader(dataReader, memoryContext)
}

type MothChunkLoader interface {
	GetMothDataSourceId() *common.MothDataSourceId
	HasNextChunk() bool
	NextChunk() *slice.Slice
	GetLastCheckpoint() int64
	SeekToCheckpoint(checkpoint int64)
	String() string
}
