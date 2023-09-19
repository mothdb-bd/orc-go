package slice

import (
	"fmt"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	DEFAULT_BUFFER_SIZE  int32 = 4 * 1024
	MINIMUM_CHUNK_SIZE   int32 = 1024
	OUTPUT_INSTANCE_SIZE int32 = util.SizeOf(&OutputStreamSliceOutput{})
)

type OutputStreamSliceOutput struct {
	// 继承
	SliceOutput

	outputStream mothio.OutputStream
	slice        *Slice
	// buffer         []byte
	bufferOffset   int64
	bufferPosition int32
	bufferSize     int32
}

func NewOutputStreamSliceOutput(inputStream mothio.OutputStream) *OutputStreamSliceOutput {
	return NewOutputStreamSliceOutput2(inputStream, DEFAULT_BUFFER_SIZE)
}
func NewOutputStreamSliceOutput2(outputStream mothio.OutputStream, bufferSize int32) *OutputStreamSliceOutput {
	ot := new(OutputStreamSliceOutput)
	util.CheckArgument2(bufferSize >= MINIMUM_CHUNK_SIZE, fmt.Sprintf("minimum buffer size of %d required", DEFAULT_BUFFER_SIZE))
	if outputStream == nil {
		panic("outputStream is null")
	}
	ot.outputStream = outputStream
	// ot.buffer = make([]byte, bufferSize)
	ot.slice = NewWithSize(int(bufferSize))
	ot.bufferSize = bufferSize
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) Flush() {
	ot.flushBufferToOutputStream()
	ot.outputStream.Flush()
}

// @Override
func (ot *OutputStreamSliceOutput) Close() error {
	util.Try(ot.flushBufferToOutputStream, nil, func() {
		ot.outputStream.Close()
	})
	return nil
}

// @Override
func (ot *OutputStreamSliceOutput) Reset() {
	panic("OutputStream cannot be reset")
}

// @Override
func (ot *OutputStreamSliceOutput) Reset2(position int32) {
	panic("OutputStream cannot be reset")
}

// @Override
func (ot *OutputStreamSliceOutput) Size() int32 {
	return util.Int32Exact(ot.LongSize())
}

func (ot *OutputStreamSliceOutput) LongSize() int64 {
	return ot.bufferOffset + int64(ot.bufferPosition)
}

// @Override
func (ot *OutputStreamSliceOutput) GetRetainedSize() int64 {
	return int64(ot.slice.GetRetainedSize() + INSTANCE_SIZE)
}

// @Override
func (ot *OutputStreamSliceOutput) WritableBytes() int32 {
	return math.MaxInt32
}

// @Override
func (ot *OutputStreamSliceOutput) IsWritable() bool {
	return true
}

// @Override
func (ot *OutputStreamSliceOutput) WriteBoolean(value bool) {
	if value {
		ot.WriteByte(1)
	} else {
		ot.WriteByte(0)
	}
}

// @Override
func (ot *OutputStreamSliceOutput) WriteB(value byte) (n int, err error) {
	ot.WriteByte(value)
	return 1, nil
}

// @Override
func (ot *OutputStreamSliceOutput) WriteByte(value byte) error {
	ot.ensureWritableBytes(util.BYTE_BYTES)
	ot.slice.WriteByte(value)
	ot.bufferPosition += util.BYTE_BYTES
	return nil
}

// @Override
func (ot *OutputStreamSliceOutput) WriteShort(value int16) {
	ot.ensureWritableBytes(util.INT16_BYTES)
	ot.slice.WriteInt16LE(value)
	ot.bufferPosition += util.INT16_BYTES
}

// @Override
func (ot *OutputStreamSliceOutput) WriteInt(value int32) {
	ot.ensureWritableBytes(util.INT32_BYTES)
	ot.slice.WriteInt32LE(value)
	ot.bufferPosition += util.INT32_BYTES
}

// @Override
func (ot *OutputStreamSliceOutput) WriteLong(value int64) {
	ot.ensureWritableBytes(util.INT64_BYTES)
	ot.slice.WriteInt64LE(value)
	ot.bufferPosition += util.INT64_BYTES
}

// @Override
func (ot *OutputStreamSliceOutput) WriteFloat(value float32) {
	ot.WriteInt(int32(math.Float32bits(value)))
}

// @Override
func (ot *OutputStreamSliceOutput) WriteDouble(value float64) {
	ot.WriteLong(int64(math.Float64bits(value)))
}

// @Override
func (ot *OutputStreamSliceOutput) WriteSlice(source *Slice) {
	ot.WriteSlice2(source, 0, source.SizeInt32())
}

// @Override
func (ot *OutputStreamSliceOutput) WriteSlice2(source *Slice, sourceIndex int32, length int32) {
	if length >= MINIMUM_CHUNK_SIZE {
		ot.flushBufferToOutputStream()
		ot.WriteToOutputStream2(source, sourceIndex, length)
		ot.bufferOffset += int64(length)
	} else {
		ot.ensureWritableBytes(length)
		// ot.slice.SetBytes(ot.bufferPosition, source, sourceIndex, length)
		ot.slice.WriteSlice(source, int(sourceIndex), int(length))
		ot.bufferPosition += length
	}
}

// @Override
func (ot *OutputStreamSliceOutput) WriteBS(source []byte) (n int, err error) {
	ot.WriteBytes(source)
	return len(source), nil
}

// @Override
func (ot *OutputStreamSliceOutput) WriteBytes(source []byte) {
	ot.WriteBytes2(source, 0, util.Lens(source))
}

// @Override
func (ot *OutputStreamSliceOutput) WriteBytes2(source []byte, sourceIndex int32, length int32) {
	if length >= MINIMUM_CHUNK_SIZE {
		ot.flushBufferToOutputStream()
		ot.WriteToOutputStream(source, sourceIndex, length)
		ot.bufferOffset += int64(length)
	} else {
		ot.ensureWritableBytes(length)
		// slice.setBytes(bufferPosition, source, sourceIndex, length)
		ot.slice.WriteBytes(source[sourceIndex : sourceIndex+length : length])
		ot.bufferPosition += length
	}
}

// @Override
func (ot *OutputStreamSliceOutput) WriteBS2(source []byte, sourceIndex int32, length int32) (n int, err error) {
	ot.WriteBytes2(source, sourceIndex, length)
	return int(length), nil
}

// @Override
func (ot *OutputStreamSliceOutput) WriteInputStream(in mothio.InputStream, length int32) {
	for length > 0 {
		batch := ot.ensureBatchSize(length)

		b := make([]byte, length)
		in.ReadBS2(b)
		ot.slice.WriteBytes(b)
		// slice.setBytes(bufferPosition, in, batch)
		ot.bufferPosition += batch
		length -= batch
	}
}

// @Override
func (ot *OutputStreamSliceOutput) WriteZero(length int32) {
	util.CheckArgument2(length >= 0, "length must be 0 or greater than 0.")
	for length > 0 {
		batch := ot.ensureBatchSize(length)
		// Arrays.fill(ot.buffer, ot.bufferPosition, ot.bufferPosition+batch, 0)

		b := make([]byte, length)
		util.FillArrays(b, 0, length, 0)
		ot.slice.WriteBytes(b)
		ot.bufferPosition += batch
		length -= batch
	}
}

// @Override
func (ot *OutputStreamSliceOutput) AppendByte(value byte) SliceOutput {
	ot.WriteByte(value)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendShort(value int16) SliceOutput {
	ot.WriteShort(value)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendInt(value int32) SliceOutput {
	ot.WriteInt(value)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendLong(value int64) SliceOutput {
	ot.WriteLong(value)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendDouble(value float64) SliceOutput {
	ot.WriteDouble(value)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendBytes(source []byte, sourceIndex int32, length int32) SliceOutput {
	ot.WriteBytes2(source, sourceIndex, length)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendBytes2(source []byte) SliceOutput {
	ot.WriteBytes(source)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) AppendSlice(slice *Slice) SliceOutput {
	ot.WriteSlice(slice)
	return ot
}

// @Override
func (ot *OutputStreamSliceOutput) Slice() *Slice {
	panic("unsupported operation")
}

// @Override
func (ot *OutputStreamSliceOutput) WriteChar(value byte) {
	panic("Unsupported")
}

// @Override
func (ot *OutputStreamSliceOutput) WriteChars(s string) {
	panic("Unsupported")
}

// @Override
func (ot *OutputStreamSliceOutput) WriteString(s string) {
	panic("Unsupported")
}

// @Override
func (ot *OutputStreamSliceOutput) GetUnderlyingSlice() *Slice {
	panic("unsupported operation")
}

// @Override
func (ot *OutputStreamSliceOutput) ToString() string {
	return ot.String()
}

// @Override
func (ot *OutputStreamSliceOutput) String() string {
	builder := util.NewSB().AppendString("OutputStreamSliceOutputAdapter{")
	builder.AppendString("outputStream=").AppendString("ot.outputStream")
	builder.AppendString("bufferSize=").AppendInt32(ot.slice.Length())
	builder.AppendInt8('}')
	return builder.String()
}

func (ot *OutputStreamSliceOutput) ensureWritableBytes(minWritableBytes int32) {
	if ot.bufferPosition+minWritableBytes > ot.slice.Length() {
		ot.flushBufferToOutputStream()
	}
}

func (ot *OutputStreamSliceOutput) ensureBatchSize(length int32) int32 {
	ot.ensureWritableBytes(maths.MinInt32(MINIMUM_CHUNK_SIZE, length))
	return maths.MinInt32(length, ot.slice.Length()-ot.bufferPosition)
}

func (ot *OutputStreamSliceOutput) flushBufferToOutputStream() {
	// b := make([]byte, ot.slice.Size())
	// ot.slice.ReadBytes(b)

	// ot.WriteToOutputStream(b, 0, ot.bufferPosition)
	ot.WriteToOutputStream(ot.slice.buf, 0, ot.slice.SizeInt32())

	ot.bufferOffset += int64(ot.bufferPosition)
	ot.slice = NewWithSize(int(ot.bufferSize))
	ot.bufferPosition = 0
}

func (ot *OutputStreamSliceOutput) WriteToOutputStream(source []byte, sourceIndex int32, length int32) {
	ot.outputStream.WriteBS2(source, sourceIndex, length)
}

func (ot *OutputStreamSliceOutput) WriteToOutputStream2(source *Slice, sourceIndex int32, length int32) {
	// b := make([]byte, maths.MinInt32(length, source.SizeInt32()-sourceIndex))
	// source.GetBytes(b, int(sourceIndex), int(sourceIndex+length))
	ot.outputStream.WriteBS(source.buf[sourceIndex : sourceIndex+length])
}
