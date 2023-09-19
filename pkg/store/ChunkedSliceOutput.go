package store

import (
	"math"
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	CHUNKED_INSTANCE_SIZE  int32 = util.SizeOf(&ChunkedSliceOutput{})
	MINIMUM_CHUNK_SIZE     int32 = 4096
	MAXIMUM_CHUNK_SIZE     int32 = 16 * 1024 * 1024
	MAX_UNUSED_BUFFER_SIZE int32 = 128
)

type ChunkedSliceOutput struct {
	// 继承
	slice.SliceOutput

	chunkSupplier            *ChunkSupplier
	slice                    *slice.Slice
	buffer                   []byte
	closedSlices             *util.ArrayList[*slice.Slice]
	closedSlicesRetainedSize int64
	streamOffset             int64
	bufferPosition           int32
}

func NewChunkedSliceOutput(minChunkSize int32, maxChunkSize int32) *ChunkedSliceOutput {
	ct := new(ChunkedSliceOutput)
	ct.closedSlices = util.NewArrayList[*slice.Slice]()

	ct.chunkSupplier = NewChunkSupplier(minChunkSize, maxChunkSize)
	ct.buffer = ct.chunkSupplier.Get()
	ct.slice = slice.NewBaseBuf(ct.buffer)
	return ct
}

func (ct *ChunkedSliceOutput) GetSlices() *util.ArrayList[*slice.Slice] {
	s, _ := ct.slice.MakeSlice(0, int(ct.bufferPosition))
	list := util.NewArrayList[*slice.Slice]()
	list.AddAll(ct.closedSlices).Add(s)
	return list
}

// @Override
func (ct *ChunkedSliceOutput) Reset() {
	ct.chunkSupplier.Reset()
	ct.closedSlices.Clear()
	ct.buffer = ct.chunkSupplier.Get()
	ct.slice = slice.NewBaseBuf(ct.buffer)
	ct.closedSlicesRetainedSize = 0
	ct.streamOffset = 0
	ct.bufferPosition = 0
}

// @Override
func (ct *ChunkedSliceOutput) Reset2(position int32) {
	panic("Unsupported operation")
}

// @Override
func (ct *ChunkedSliceOutput) Size() int32 {
	return util.Int32Exact(ct.streamOffset + int64(ct.bufferPosition))
}

// @Override
func (ct *ChunkedSliceOutput) GetRetainedSize() int64 {
	return int64(ct.slice.GetRetainedSize()) + ct.closedSlicesRetainedSize + int64(CHUNKED_INSTANCE_SIZE)
}

// @Override
func (ct *ChunkedSliceOutput) WritableBytes() int32 {
	return math.MaxInt32
}

// @Override
func (ct *ChunkedSliceOutput) IsWritable() bool {
	return true
}

// @Override
func (ct *ChunkedSliceOutput) Write(b byte) {
	ct.WriteByte(b)
}

// @Override
func (ct *ChunkedSliceOutput) WriteByte(b byte) error {
	ct.ensureWritableBytes(util.BYTE_BYTES)
	ct.slice.WriteByte(b)
	ct.bufferPosition += util.BYTE_BYTES
	return nil
}

// @Override
func (ct *ChunkedSliceOutput) WriteShort(value int16) {
	ct.ensureWritableBytes(util.INT16_BYTES)
	ct.slice.WriteInt16LE(value)
	ct.bufferPosition += util.INT16_BYTES
}

// @Override
func (ct *ChunkedSliceOutput) WriteInt(value int32) {
	ct.ensureWritableBytes(util.INT32_BYTES)
	ct.slice.WriteInt32LE(value)
	ct.bufferPosition += util.INT32_BYTES
}

// @Override
func (ct *ChunkedSliceOutput) WriteLong(value int64) {
	ct.ensureWritableBytes(util.INT64_BYTES)
	ct.slice.WriteInt64LE(value)
	ct.bufferPosition += util.INT64_BYTES
}

// @Override
func (ct *ChunkedSliceOutput) WriteFloat(value float32) {
	ct.WriteInt(int32(math.Float32bits(value)))
}

// @Override
func (ct *ChunkedSliceOutput) WriteDouble(value float64) {
	ct.WriteLong(int64(math.Float64bits(value)))
}

// @Override
func (ct *ChunkedSliceOutput) WriteSlice(source *slice.Slice) {
	ct.WriteSlice2(source, 0, source.SizeInt32())
}

// @Override
func (ct *ChunkedSliceOutput) WriteSlice2(source *slice.Slice, sourceIndex int32, length int32) {
	for length > 0 {
		batch := ct.tryEnsureBatchSize(length)

		ct.slice.WriteSlice(source, int(sourceIndex), int(batch))
		ct.bufferPosition += batch
		sourceIndex += batch
		length -= batch
	}
}

// @Override
func (ct *ChunkedSliceOutput) WriteBytes(source []byte) {
	ct.WriteBytes2(source, 0, util.Lens(source))
}

// @Override
func (ct *ChunkedSliceOutput) WriteBytes2(source []byte, sourceIndex int32, length int32) {
	for length > 0 {
		batch := ct.tryEnsureBatchSize(length)
		ct.slice.WriteBytes(source[sourceIndex : sourceIndex+batch])
		ct.bufferPosition += batch
		sourceIndex += batch
		length -= batch
	}
}

// @Override
func (ct *ChunkedSliceOutput) WriteInputStream(in mothio.InputStream, length int32) {
	for length > 0 {
		batch := ct.tryEnsureBatchSize(length)
		b := make([]byte, batch)
		in.ReadBS2(b)
		ct.slice.WriteBytes(b)
		ct.bufferPosition += batch
		length -= batch
	}
}

// @Override
func (ct *ChunkedSliceOutput) WriteZero(length int32) {
	util.CheckArgument2(length >= 0, "length must be greater than or equal to 0")
	for length > 0 {
		batch := ct.tryEnsureBatchSize(length)
		b := make([]byte, batch)
		util.FillArrays(b, 0, batch, 0)
		ct.slice.WriteBytes(b)
		ct.bufferPosition += batch
		length -= batch
	}
}

// @Override
func (ct *ChunkedSliceOutput) AppendLong(value int64) slice.SliceOutput {
	ct.WriteLong(value)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendDouble(value float64) slice.SliceOutput {
	ct.WriteDouble(value)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendInt(value int32) slice.SliceOutput {
	ct.WriteInt(value)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendShort(value int16) slice.SliceOutput {
	ct.WriteShort(value)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendByte(value byte) slice.SliceOutput {
	ct.WriteByte(value)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendBytes(source []byte, sourceIndex int32, length int32) slice.SliceOutput {
	ct.WriteBytes2(source, sourceIndex, length)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendBytes2(source []byte) slice.SliceOutput {
	ct.WriteBytes(source)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) AppendSlice(slice *slice.Slice) slice.SliceOutput {
	ct.WriteSlice(slice)
	return ct
}

// @Override
func (ct *ChunkedSliceOutput) Slice() *slice.Slice {
	panic("unsupported operation")
}

// @Override
func (ct *ChunkedSliceOutput) GetUnderlyingSlice() *slice.Slice {
	panic("unsupported operation")
}

// @Override
func (ct *ChunkedSliceOutput) String() string {
	return ct.ToString()
}

// @Override
func (ct *ChunkedSliceOutput) ToString() string {
	builder := util.NewSB().AppendString("OutputStreamSliceOutputAdapter{")
	builder.AppendString("position=").AppendInt32(ct.Size())
	builder.AppendString("bufferSize=").AppendInt32(ct.slice.SizeInt32())
	builder.AppendInt8('}')
	return builder.String()
}

func (ct *ChunkedSliceOutput) tryEnsureBatchSize(length int32) int32 {
	ct.ensureWritableBytes(maths.MinInt32(MAX_UNUSED_BUFFER_SIZE, length))
	return maths.MinInt32(length, int32(ct.slice.Capacity())-ct.bufferPosition)
}

func (ct *ChunkedSliceOutput) ensureWritableBytes(minWritableBytes int32) {
	util.CheckArgument(minWritableBytes <= MAX_UNUSED_BUFFER_SIZE)
	if int(ct.bufferPosition+minWritableBytes) > ct.slice.Capacity() {
		ct.closeChunk()
	}
}

func (ct *ChunkedSliceOutput) closeChunk() {
	s, _ := ct.slice.MakeSlice(0, int(ct.bufferPosition))
	ct.closedSlices.Add(s)
	ct.closedSlicesRetainedSize += int64(ct.slice.GetRetainedSize())
	ct.buffer = ct.chunkSupplier.Get()
	ct.slice = slice.NewBaseBuf(ct.buffer)
	ct.streamOffset += int64(ct.bufferPosition)
	ct.bufferPosition = 0
}

type ChunkSupplier struct {
	maxChunkSize int32
	bufferPool   *util.ArrayList[[]byte]
	usedBuffers  *util.ArrayList[[]byte]
	currentSize  int32
}

func NewChunkSupplier(minChunkSize int32, maxChunkSize int32) *ChunkSupplier {
	cr := new(ChunkSupplier)
	util.CheckArgument2(minChunkSize >= MINIMUM_CHUNK_SIZE, "minimum chunk size of "+strconv.Itoa(int(MINIMUM_CHUNK_SIZE))+" required")
	util.CheckArgument2(maxChunkSize <= MAXIMUM_CHUNK_SIZE, "maximum chunk size of "+strconv.Itoa(int(MAXIMUM_CHUNK_SIZE))+" required")
	util.CheckArgument2(minChunkSize <= maxChunkSize, "minimum chunk size must be less than maximum chunk size")
	cr.currentSize = minChunkSize
	cr.maxChunkSize = maxChunkSize

	cr.bufferPool = util.NewArrayList[[]byte]()
	cr.usedBuffers = util.NewArrayList[[]byte]()
	return cr
}

func (cr *ChunkSupplier) Reset() {
	cr.bufferPool.AddAllIndex(0, cr.usedBuffers)
	cr.usedBuffers.Clear()
}

func (cr *ChunkSupplier) Get() []byte {
	var buffer []byte
	if cr.bufferPool.IsEmpty() {
		cr.currentSize = maths.MinInt32(multiplyExact(cr.currentSize, 2), cr.maxChunkSize)
		buffer = make([]byte, cr.currentSize)
	} else {
		buffer = cr.bufferPool.Remove(0)
		cr.currentSize = util.Lens(buffer)
	}
	cr.usedBuffers.Add(buffer)
	return buffer
}

func multiplyExact(x int32, y int32) int32 {
	r := int64(x) * int64(y)
	if r > math.MaxInt32 || r < math.MinInt32 {
		panic("integer overflow")
	}
	return int32(r)
}
