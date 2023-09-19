package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type CompressedMothChunkLoader struct {
	// 继承
	MothChunkLoader

	dataReader                     MothDataReader
	dataReaderMemoryUsage          memory.LocalMemoryContext
	decompressor                   MothDecompressor
	decompressionBufferMemoryUsage memory.LocalMemoryContext
	compressedBufferStream         slice.FixedLengthSliceInput
	compressedBufferStart          int32
	nextUncompressedOffset         int32
	lastCheckpoint                 int64
	decompressorOutputBuffer       []byte
}

func NewCompressedMothChunkLoader(dataReader MothDataReader, decompressor MothDecompressor, memoryContext memory.AggregatedMemoryContext) *CompressedMothChunkLoader {
	cr := new(CompressedMothChunkLoader)

	cr.compressedBufferStream = slice.EMPTY_SLICE.GetInput()
	cr.dataReader = dataReader
	cr.decompressor = decompressor
	cr.dataReaderMemoryUsage = memoryContext.NewLocalMemoryContext("CompressedMothChunkLoader")
	cr.dataReaderMemoryUsage.SetBytes(dataReader.GetRetainedSize())
	cr.decompressionBufferMemoryUsage = memoryContext.NewLocalMemoryContext("CompressedMothChunkLoader")
	return cr
}

// @Override
func (cr *CompressedMothChunkLoader) GetMothDataSourceId() *common.MothDataSourceId {
	return cr.dataReader.GetMothDataSourceId()
}

func (cr *CompressedMothChunkLoader) getCurrentCompressedOffset() int32 {
	return util.Int32Exact(int64(cr.compressedBufferStart) + cr.compressedBufferStream.Position())
}

// @Override
func (cr *CompressedMothChunkLoader) HasNextChunk() bool {
	return cr.getCurrentCompressedOffset() < cr.dataReader.GetSize()
}

// @Override
func (cr *CompressedMothChunkLoader) GetLastCheckpoint() int64 {
	return cr.lastCheckpoint
}

// @Override
func (cr *CompressedMothChunkLoader) SeekToCheckpoint(checkpoint int64) {
	compressedOffset := DecodeCompressedBlockOffset(checkpoint)
	if compressedOffset >= cr.dataReader.GetSize() {
		panic(fmt.Sprintf("%s Seek past end of stream", cr.dataReader.GetMothDataSourceId()))
	}
	if cr.compressedBufferStart <= compressedOffset && compressedOffset < cr.compressedBufferStart+int32(cr.compressedBufferStream.Length()) {
		cr.compressedBufferStream.SetPosition(int64(compressedOffset - cr.compressedBufferStart))
	} else {
		cr.compressedBufferStart = compressedOffset
		cr.compressedBufferStream = slice.EMPTY_SLICE.GetInput()
	}
	cr.nextUncompressedOffset = DecodeDecompressedOffset(checkpoint)
	cr.lastCheckpoint = checkpoint
}

// @Override
func (cr *CompressedMothChunkLoader) NextChunk() *slice.Slice {
	cr.ensureCompressedBytesAvailable(3)
	cr.lastCheckpoint = CreateInputStreamCheckpoint2(cr.getCurrentCompressedOffset(), cr.nextUncompressedOffset)
	b0 := cr.compressedBufferStream.ReadUnsignedByte()
	b1 := cr.compressedBufferStream.ReadUnsignedByte()
	b2 := cr.compressedBufferStream.ReadUnsignedByte()
	isUncompressed := (b0 & 0x01) == 1
	chunkLength := int32(b2)<<15 | int32(b1)<<7 | (maths.UnsignedRightShiftInt32(int32(b0), 1))
	cr.ensureCompressedBytesAvailable(chunkLength)
	chunk := cr.compressedBufferStream.ReadSlice(chunkLength)
	if !isUncompressed {
		// chunk.byteArrayOffset()
		uncompressedSize := cr.decompressor.Decompress(chunk.AvailableBytes(), 0, chunk.Length(), cr.createOutputBuffer())
		chunk = slice.NewWithBuf(cr.decompressorOutputBuffer[0:uncompressedSize])
	}
	if cr.nextUncompressedOffset != 0 {
		chunk, _ = chunk.MakeSlice(int(cr.nextUncompressedOffset), int(chunk.Length()-cr.nextUncompressedOffset))
		cr.nextUncompressedOffset = 0
		if chunk.Length() == 0 {
			chunk = cr.NextChunk()
		}
	}
	return chunk
}

func (cr *CompressedMothChunkLoader) ensureCompressedBytesAvailable(size int32) {
	if int64(size) <= cr.compressedBufferStream.Remaining() {
		return
	}
	if size > cr.dataReader.GetMaxBufferSize() {
		panic(fmt.Sprintf("Requested read size (%d bytes) is greater than max buffer size (%d bytes", size, cr.dataReader.GetMaxBufferSize()))
	}
	if cr.compressedBufferStart+int32(cr.compressedBufferStream.Position())+size > cr.dataReader.GetSize() {
		panic("Read past end of stream")
	}
	cr.compressedBufferStart = cr.compressedBufferStart + util.Int32Exact(cr.compressedBufferStream.Position())
	compressedBuffer := cr.dataReader.SeekBuffer(cr.compressedBufferStart)
	cr.dataReaderMemoryUsage.SetBytes(cr.dataReader.GetRetainedSize())
	if compressedBuffer.Length() < size {
		panic(fmt.Sprintf("Requested read of %d bytes but only %d were bytes", size, compressedBuffer.SizeInt32()))
	}
	cr.compressedBufferStream = compressedBuffer.GetInput()
}

func (cr *CompressedMothChunkLoader) createOutputBuffer() OutputBuffer {
	return NewOutputBuffer(cr)
}

// @Override
func (cr *CompressedMothChunkLoader) ToString() string {
	return util.NewSB().AddString("loader", cr.dataReader.String()).AddInt32("compressedOffset", cr.getCurrentCompressedOffset()).AddString("decompressor", cr.decompressor.String()).String()
}

type OutputBufferImpl struct {
	// 继承
	OutputBuffer

	cr *CompressedMothChunkLoader
}

func NewOutputBuffer(cr *CompressedMothChunkLoader) OutputBuffer {
	br := new(OutputBufferImpl)
	br.cr = cr
	return br
}

// @Override
func (ol *OutputBufferImpl) Initialize(size int32) []byte {
	if ol.cr.decompressorOutputBuffer == nil || size > util.Lens(ol.cr.decompressorOutputBuffer) {
		ol.cr.decompressorOutputBuffer = make([]byte, size)
		ol.cr.decompressionBufferMemoryUsage.SetBytes(int64(size))
	}
	return ol.cr.decompressorOutputBuffer
}

// @Override
func (ol *OutputBufferImpl) Grow(size int32) []byte {
	if size > util.Lens(ol.cr.decompressorOutputBuffer) {
		newB := make([]byte, size)
		copy(newB, ol.cr.decompressorOutputBuffer)
		ol.cr.decompressorOutputBuffer = newB
		ol.cr.decompressionBufferMemoryUsage.SetBytes(int64(size))
	}
	return ol.cr.decompressorOutputBuffer
}
