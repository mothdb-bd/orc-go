package slice

import (
	"io"
	"reflect"
	"unsafe"
)

// Slice Slice itself
type Slice struct {
	buf []byte

	capacity int
	refCount int

	readerMarker int
	writerMarker int
	readerIndex  int
	writerIndex  int
}

var (
	EMPTY_SLICE   = &Slice{buf: make([]byte, 0), capacity: 0, readerMarker: -1, writerMarker: -1}
	INSTANCE_SIZE = int32(unsafe.Sizeof(EMPTY_SLICE))

	SLICE_KIND = reflect.TypeOf(EMPTY_SLICE).Kind()
)

func (s *Slice) GetRetainedSize() int32 {
	return INSTANCE_SIZE
}

func NewByString(s string) (*Slice, error) {
	return NewWithString(s), nil
}

func NewWithSize(size int) *Slice {
	b := &Slice{
		buf: make([]byte, size),

		capacity:     size,
		readerMarker: -1,
		writerMarker: -1,
	}
	return b
}

func NewWithString(s string) *Slice {
	sb := []byte(s)
	return NewWithBuf(sb)
}

/**
 *
 *  基于buf做基础,buf 做为写入
 */
func NewWithBuf(buf []byte) *Slice {
	lb := len(buf)
	b := &Slice{
		buf:          buf,
		capacity:     lb,
		readerMarker: -1,
		writerMarker: -1,
	}

	b.writerIndex += lb
	return b
}

/**
 *
 *  基于buf做基础,buf 不做写入
 */
func NewBaseBuf(buf []byte) *Slice {
	lb := len(buf)
	b := &Slice{
		buf:          buf,
		capacity:     lb,
		readerMarker: -1,
		writerMarker: -1,
	}
	return b
}

// NewSlice create new Slice, pass any []byte in as initial buffer, it will be auto capped
func NewSlice(bufs ...[]byte) (*Slice, error) {
	b := &Slice{
		buf: make([]byte, ChunkSize),

		capacity:     ChunkSize,
		readerMarker: -1,
		writerMarker: -1,
	}

	for _, buf := range bufs {
		n, err := b.Write(buf)
		if err != nil {
			return nil, err
		}
		if n != len(buf) {
			return nil, io.ErrShortWrite
		}
	}

	return b, nil
}

// NewSlice create new Slice, pass any []byte in as initial buffer, it will be auto capped
func New(size int, bufs ...[]byte) (*Slice, error) {
	b := &Slice{
		buf: make([]byte, size),

		capacity:     size,
		readerMarker: -1,
		writerMarker: -1,
	}

	for _, buf := range bufs {
		n, err := b.Write(buf)
		if err != nil {
			return nil, err
		}
		if n != len(buf) {
			return nil, io.ErrShortWrite
		}
	}

	return b, nil
}

func (b *Slice) GetInput() *BasicSliceInput {
	return NewBasicSliceInput(b)
}

// ReaderIndex access current reader index
func (b *Slice) ReaderIndex() int {
	return b.readerIndex
}

// WriterIndex access current writer index
func (b *Slice) WriterIndex() int {
	return b.writerIndex
}

// AvailableBytes get available bytes([:size])
func (b *Slice) AvailableBytes() []byte {
	p := make([]byte, b.writerIndex)
	copy(p, b.buf)
	return p
}

// Bytes get all the bytes in buffer ([:capacity])
func (b *Slice) Bytes() []byte {
	p := make([]byte, b.capacity)
	copy(p, b.buf)
	return p
}

// UnsafeBytes DANGER! access underlying buffer directly
func (b *Slice) UnsafeBytes() []byte {
	return b.buf
}

// Size get current written buffer size
func (b *Slice) Size() int {
	return b.writerIndex
}

// Size get current written buffer size
func (b *Slice) Length() int32 {
	return int32(b.writerIndex)
}

// Size get current written buffer size
func (b *Slice) LenInt64() int64 {
	return int64(b.writerIndex)
}

func (b *Slice) SizeInt32() int32 {
	return int32(b.writerIndex)
}

// Capacity get buffer capacity (how much it could read/write)
func (b *Slice) Capacity() int {
	return b.capacity
}

// MarkReaderIndex mark current reader index for reset in the future
func (b *Slice) MarkReaderIndex() {
	b.readerMarker = b.readerIndex
}

// ResetReaderIndex reset reader index to marked index
func (b *Slice) ResetReaderIndex() {
	if b.readerMarker != -1 {
		b.readerIndex = b.readerMarker
		b.readerMarker = -1
	}
}

// MarkWriterIndex mark current writer index for reset in the future
func (b *Slice) MarkWriterIndex() {
	b.writerMarker = b.writerIndex
}

// ResetWriterIndex reset writer index to marked index
func (b *Slice) ResetWriterIndex() {
	if b.writerMarker != -1 {
		b.writerIndex = b.writerMarker
		b.writerMarker = -1
	}
}

// Flush flush all bytes and reset indexes
func (b *Slice) Flush() {
	b.buf = nil
	b.buf = make([]byte, ChunkSize)

	b.readerIndex = 0
	b.readerMarker = -1
	b.writerIndex = 0
	b.writerMarker = 0

	b.capacity = ChunkSize
}

// IsReadable if buf is readable (b.writerIndex > b.readerIndex)
func (b *Slice) IsReadable() bool {
	return b.writerIndex > b.readerIndex
}

func (b *Slice) IsCompact() bool {
	return b.writerIndex > b.capacity
}

// ReadableBytes how much bytes are there to be read
func (b *Slice) ReadableBytes() int {
	return b.writerIndex - b.readerIndex
}

// Ref increment reference counter
func (b *Slice) Ref() {
	b.refCount++
}

// Release decrement reference counter, underlying buf will be cleared once reference count is 0
func (b *Slice) Release() {
	b.refCount--
	if b.refCount == 0 {
		b.ForceRelease()
	}
}

// ForceRelease force release bufs
func (b *Slice) ForceRelease() {
	b.refCount = 0
	b.buf = nil
}
