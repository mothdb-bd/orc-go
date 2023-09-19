package store

import (
	"fmt"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	MOTHOUTPUT_INSTANCE_SIZE         int32 = util.SizeOf(&MothOutputBuffer{})
	INITIAL_BUFFER_SIZE              int32 = 256
	DIRECT_FLUSH_SIZE                int32 = 32 * 1024
	MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE int32 = 4 * 1024
	MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE int32 = 1024 * 1024
)

type MothOutputBuffer struct {
	// 继承
	slice.SliceOutput

	maxBufferSize          int32
	compressedOutputStream *ChunkedSliceOutput //@Nullable
	compressor             Compressor
	compressionBuffer      []byte
	slice                  *slice.Slice
	bufferOffset           int64
	bufferPosition         int32
}

func NewMothOutputBuffer(compression metadata.CompressionKind, maxBufferSize int32) *MothOutputBuffer {
	mr := new(MothOutputBuffer)

	mr.compressionBuffer = make([]byte, 0)

	mr.maxBufferSize = maxBufferSize
	buffer := make([]byte, INITIAL_BUFFER_SIZE)
	mr.slice = slice.NewBaseBuf(buffer)
	mr.compressedOutputStream = NewChunkedSliceOutput(MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE, MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE)
	mr.compressor = getCompressor(compression)
	return mr
}

// @Nullable
func getCompressor(compression metadata.CompressionKind) Compressor {
	switch compression {
	case metadata.NONE:
		return nil
	case metadata.SNAPPY:
		return NewSnappyCompressor()
	case metadata.ZLIB:
		return NewDeflateCompressor()
	case metadata.LZ4:
		return NewLz4Compressor()
		// case metadata.ZSTD:
		// 	return NewZstdCompressor()
	}
	panic(fmt.Sprintf("Unsupported compression %d", compression))
}

func (mr *MothOutputBuffer) GetOutputDataSize() int64 {
	util.CheckState2(mr.bufferPosition == 0, "Buffer must be flushed before getOutputDataSize can be called")
	return int64(mr.compressedOutputStream.Size())
}

func (mr *MothOutputBuffer) EstimateOutputDataSize() int64 {
	return int64(mr.compressedOutputStream.Size() + mr.bufferPosition)
}

func (mr *MothOutputBuffer) WriteDataTo(outputStream slice.SliceOutput) int64 {
	util.CheckState2(mr.bufferPosition == 0, "Buffer must be closed before writeDataTo can be called")
	for _, slice := range mr.compressedOutputStream.GetSlices().ToArray() {
		outputStream.WriteSlice(slice)
	}
	return int64(mr.compressedOutputStream.Size())
}

func (mr *MothOutputBuffer) GetCheckpoint() int64 {
	if mr.compressor == nil {
		return int64(mr.Size())
	}
	return CreateInputStreamCheckpoint2(mr.compressedOutputStream.Size(), mr.bufferPosition)
}

// @Override
func (mr *MothOutputBuffer) Flush() {
	mr.flushBufferToOutputStream()
}

// @Override
func (mr *MothOutputBuffer) Close() error {
	mr.flushBufferToOutputStream()
	return nil
}

// @Override
func (mr *MothOutputBuffer) Reset() {
	mr.compressedOutputStream.Reset()
	buffer := make([]byte, INITIAL_BUFFER_SIZE)
	mr.slice = slice.NewBaseBuf(buffer)
	mr.bufferOffset = 0
	mr.bufferPosition = 0
}

// @Override
func (mr *MothOutputBuffer) Reset2(position int32) {
	panic("unsupported operation")
}

// @Override
func (mr *MothOutputBuffer) Size() int32 {
	return util.Int32Exact(mr.bufferOffset + int64(mr.bufferPosition))
}

// @Override
func (mr *MothOutputBuffer) GetRetainedSize() int64 {
	return int64(MOTHOUTPUT_INSTANCE_SIZE) + mr.compressedOutputStream.GetRetainedSize() + int64(mr.slice.GetRetainedSize()) + util.LensInt64(mr.compressionBuffer)
}

// @Override
func (mr *MothOutputBuffer) WritableBytes() int32 {
	return math.MaxInt32
}

// @Override
func (mr *MothOutputBuffer) IsWritable() bool {
	return true
}

// @Override
func (mr *MothOutputBuffer) WriteByte(value byte) error {
	mr.ensureWritableBytes(util.BYTE_BYTES)
	mr.slice.WriteByte(value)
	mr.bufferPosition += util.BYTE_BYTES
	return nil
}

// @Override
func (mr *MothOutputBuffer) WriteShort(value int16) {
	mr.ensureWritableBytes(util.INT16_BYTES)
	mr.slice.WriteInt16LE(value)
	mr.bufferPosition += util.INT16_BYTES
}

// @Override
func (mr *MothOutputBuffer) WriteInt(value int32) {
	mr.ensureWritableBytes(util.INT32_BYTES)
	mr.slice.WriteInt32LE(value)
	mr.bufferPosition += util.INT32_BYTES
}

// @Override
func (mr *MothOutputBuffer) WriteLong(value int64) {
	mr.ensureWritableBytes(util.INT64_BYTES)
	mr.slice.WriteInt64LE(value)
	mr.bufferPosition += util.INT64_BYTES
}

// @Override
func (mr *MothOutputBuffer) WriteFloat(value float32) {
	mr.WriteInt(int32(math.Float32bits(value)))
}

// @Override
func (mr *MothOutputBuffer) WriteDouble(value float64) {
	mr.WriteLong(int64(math.Float64bits(value)))
}

// @Override
func (mr *MothOutputBuffer) WriteSlice(source *slice.Slice) {
	mr.WriteSlice2(source, 0, source.Length())
}

// @Override
func (mr *MothOutputBuffer) WriteSlice2(source *slice.Slice, sourceIndex int32, length int32) {
	if length >= DIRECT_FLUSH_SIZE {
		mr.flushBufferToOutputStream()
		// source.byteArrayOffset()
		mr.writeDirectlyToOutputStream(source.AvailableBytes(), sourceIndex, length)
		mr.bufferOffset += int64(length)
	} else {
		mr.ensureWritableBytes(length)
		mr.slice.WriteSlice(source, int(sourceIndex), int(length))
		mr.bufferPosition += length
	}
}

func (mr *MothOutputBuffer) Write(b byte) {
	mr.WriteBytes([]byte{b})
}

// @Override
func (mr *MothOutputBuffer) WriteBytes(source []byte) {
	mr.WriteBytes2(source, 0, util.Lens(source))
}

func (mr *MothOutputBuffer) WriteBS(source []byte) (n int, err error) {
	l := util.Lens(source)
	mr.WriteBytes2(source, 0, l)
	return int(l), nil
}

func (mr *MothOutputBuffer) WriteBS2(source []byte, sourceIndex int32, length int32) (n int, err error) {
	mr.WriteBytes2(source, sourceIndex, length)
	return int(length), nil
}

// @Override
func (mr *MothOutputBuffer) WriteBytes2(source []byte, sourceIndex int32, length int32) {
	if length >= DIRECT_FLUSH_SIZE {
		mr.flushBufferToOutputStream()
		mr.writeDirectlyToOutputStream(source, sourceIndex, length)
		mr.bufferOffset += int64(length)
	} else {
		mr.ensureWritableBytes(length)
		mr.slice.WriteBytes(source[sourceIndex : sourceIndex+length])
		mr.bufferPosition += length
	}
}

// @Override
func (mr *MothOutputBuffer) WriteInputStream(in mothio.InputStream, length int32) {
	for length > 0 {
		batch := mr.ensureBatchSize(length)
		b := make([]byte, batch)
		in.ReadBS2(b)
		mr.slice.WriteBytes(b)
		length -= batch
		mr.bufferPosition += batch
	}
}

// @Override
func (mr *MothOutputBuffer) WriteZero(length int32) {
	util.CheckArgument2(length >= 0, "length must be 0 or greater than 0.")
	for length > 0 {
		batch := mr.ensureBatchSize(length)
		b := make([]byte, batch)
		util.FillArrays(b, 0, batch, 0)

		mr.slice.WriteBytes(b)
		length -= batch
		mr.bufferPosition += batch
	}
}

// @Override
func (mr *MothOutputBuffer) AppendLong(value int64) slice.SliceOutput {
	mr.WriteLong(value)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendDouble(value float64) slice.SliceOutput {
	mr.WriteDouble(value)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendInt(value int32) slice.SliceOutput {
	mr.WriteInt(value)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendShort(value int16) slice.SliceOutput {
	mr.WriteShort(value)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendByte(value byte) slice.SliceOutput {
	mr.WriteByte(value)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendBytes(source []byte, sourceIndex int32, length int32) slice.SliceOutput {
	mr.WriteBytes2(source, sourceIndex, length)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendBytes2(source []byte) slice.SliceOutput {
	mr.WriteBytes(source)
	return mr
}

// @Override
func (mr *MothOutputBuffer) AppendSlice(slice *slice.Slice) slice.SliceOutput {
	mr.WriteSlice(slice)
	return mr
}

// @Override
func (mr *MothOutputBuffer) Slice() *slice.Slice {
	panic("unsupported operation")
}

// @Override
func (mr *MothOutputBuffer) GetUnderlyingSlice() *slice.Slice {
	panic("unsupported operation")
}

// @Override
func (mr *MothOutputBuffer) String() string {
	builder := util.NewSB().AppendString("OutputStreamSliceOutputAdapter{")
	builder.AppendString("outputStream=").AppendString(mr.compressedOutputStream.String())
	builder.AppendString("bufferSize=").AppendInt32(mr.slice.SizeInt32())
	builder.AppendInt8('}')
	return builder.String()
}

func (mr *MothOutputBuffer) ensureWritableBytes(minWritableBytes int32) {
	neededBufferSize := mr.bufferPosition + minWritableBytes
	if int(neededBufferSize) <= mr.slice.Capacity() {
		return
	}
	if mr.slice.SizeInt32() >= mr.maxBufferSize {
		mr.flushBufferToOutputStream()
		return
	}
	newBufferSize := maths.MinInt32(maths.MaxInt32(mr.slice.SizeInt32()*2, minWritableBytes), mr.maxBufferSize)
	if neededBufferSize > newBufferSize {
		mr.flushBufferToOutputStream()
		buffer := make([]byte, newBufferSize)
		mr.slice = slice.NewBaseBuf(buffer)
	}
}

func (mr *MothOutputBuffer) ensureBatchSize(length int32) int32 {
	mr.ensureWritableBytes(maths.MinInt32(DIRECT_FLUSH_SIZE, length))
	return maths.MinInt32(length, int32(mr.slice.Capacity())-mr.bufferPosition)
}

func (mr *MothOutputBuffer) flushBufferToOutputStream() {
	if mr.bufferPosition > 0 {
		mr.writeChunkToOutputStream(mr.slice.UnsafeBytes()[0:mr.slice.Size()], 0, mr.slice.SizeInt32())
		mr.bufferOffset += int64(mr.bufferPosition)
		mr.bufferPosition = 0
	}
}

func (mr *MothOutputBuffer) writeChunkToOutputStream(chunk []byte, offset int32, length int32) {
	if mr.compressor == nil {
		mr.compressedOutputStream.WriteBytes2(chunk, offset, length)
		return
	}
	minCompressionBufferSize := mr.compressor.MaxCompressedLength(length)
	if util.Lens(mr.compressionBuffer) < minCompressionBufferSize {
		mr.compressionBuffer = make([]byte, minCompressionBufferSize)
	}
	compressedSize := mr.compressor.Compress(chunk, offset, length, mr.compressionBuffer, 0, util.Lens(mr.compressionBuffer))
	if compressedSize < length {
		chunkHeader := (compressedSize << 1)
		mr.compressedOutputStream.Write(byte(chunkHeader & 0x00_00FF))
		mr.compressedOutputStream.Write(byte((chunkHeader & 0x00_FF00) >> 8))
		mr.compressedOutputStream.Write(byte((chunkHeader & 0xFF_0000) >> 16))
		mr.compressedOutputStream.WriteBytes2(mr.compressionBuffer, 0, compressedSize)
	} else {
		header := (length << 1) + 1
		mr.compressedOutputStream.Write(byte(header & 0x00_00FF))
		mr.compressedOutputStream.Write(byte((header & 0x00_FF00) >> 8))
		mr.compressedOutputStream.Write(byte((header & 0xFF_0000) >> 16))
		mr.compressedOutputStream.WriteBytes2(chunk, offset, length)
	}
}

func (mr *MothOutputBuffer) writeDirectlyToOutputStream(bytes []byte, bytesOffset int32, length int32) {
	if mr.compressor == nil {
		mr.compressedOutputStream.WriteBytes2(bytes, bytesOffset, length)
		return
	}
	for length > 0 {
		chunkSize := maths.MinInt32(length, mr.slice.SizeInt32())
		mr.writeChunkToOutputStream(bytes, bytesOffset, chunkSize)
		length -= chunkSize
		bytesOffset += chunkSize
	}
}

// @VisibleForTesting
func (mr *MothOutputBuffer) getBufferCapacity() int32 {
	return int32(mr.slice.Capacity())
}
