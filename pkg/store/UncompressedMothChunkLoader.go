package store

import (
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type UncompressedMothChunkLoader struct {
	dataReader            MothDataReader
	dataReaderMemoryUsage memory.LocalMemoryContext
	lastCheckpoint        int64
	nextPosition          int32
}

func NewUncompressedMothChunkLoader(dataReader MothDataReader, memoryContext memory.AggregatedMemoryContext) *UncompressedMothChunkLoader {
	ur := new(UncompressedMothChunkLoader)
	ur.dataReader = dataReader
	ur.dataReaderMemoryUsage = memoryContext.NewLocalMemoryContext("UncompressedMothChunkLoader")
	ur.dataReaderMemoryUsage.SetBytes(dataReader.GetRetainedSize())
	return ur
}

// @Override
func (ur *UncompressedMothChunkLoader) GetMothDataSourceId() *common.MothDataSourceId {
	return ur.dataReader.GetMothDataSourceId()
}

func (ur *UncompressedMothChunkLoader) getCurrentCompressedOffset() int32 {
	if ur.HasNextChunk() {
		return 0
	} else {
		return ur.dataReader.GetSize()
	}
}

// @Override
func (ur *UncompressedMothChunkLoader) HasNextChunk() bool {
	return ur.nextPosition < ur.dataReader.GetSize()
}

// @Override
func (ur *UncompressedMothChunkLoader) GetLastCheckpoint() int64 {
	return ur.lastCheckpoint
}

// @Override
func (ur *UncompressedMothChunkLoader) SeekToCheckpoint(checkpoint int64) {
	compressedOffset := DecodeCompressedBlockOffset(checkpoint)
	if compressedOffset != 0 {
		panic("Uncompressed stream does not support seeking to a compressed offset")
	}
	decompressedOffset := DecodeDecompressedOffset(checkpoint)
	ur.nextPosition = decompressedOffset
	ur.lastCheckpoint = checkpoint
}

// @Override
func (ur *UncompressedMothChunkLoader) NextChunk() *slice.Slice {
	if ur.nextPosition >= ur.dataReader.GetSize() {
		panic("Read past end of stream")
	}
	chunk := ur.dataReader.SeekBuffer(ur.nextPosition)
	ur.dataReaderMemoryUsage.SetBytes(ur.dataReader.GetRetainedSize())
	ur.lastCheckpoint = CreateInputStreamCheckpoint2(0, ur.nextPosition)
	ur.nextPosition += chunk.Length()
	return chunk
}

// @Override
func (ur *UncompressedMothChunkLoader) String() string {
	return util.NewSB().AddString("loader", ur.dataReader.String()).AddInt32("compressedOffset", ur.getCurrentCompressedOffset()).String()
}
