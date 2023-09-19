package slice

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
)

// Global parameters, users can modify accordingly
var (
	// ChunkSize how much byte to allocate if there were not enough room
	ChunkSize = 1024
)

// Errors
var (
	ErrOutOfBound = errors.New("index is out of bound")
)

// ByteIterator type used for byte iteration
type ByteIterator func(byte) bool

// EnsureCapacity allocate more bytes to ensure the capacity
func (b *Slice) EnsureCapacity(size int) {
	if size <= b.capacity {
		return
	}
	capacity := ((size-1)/ChunkSize + 1) * ChunkSize
	newBuf := make([]byte, capacity)
	copy(newBuf, b.buf)
	b.buf = nil
	b.buf = newBuf
	b.capacity = capacity
}

// Index index a byte slice inside buffer, Index(p), Index(p, start), Index(p, start, end)
func (b *Slice) Index(p []byte, indexes ...int) int {
	if len(indexes) >= 2 {
		if indexes[1] > indexes[0] && indexes[1] < b.capacity && indexes[0] > 0 {
			return bytes.Index(b.buf[indexes[0]:indexes[1]], p)
		}
		return -1
	} else if len(indexes) >= 1 {
		if indexes[0] > 0 && indexes[0] < b.capacity {
			return bytes.Index(b.buf[indexes[0]:], p)
		}
		return -1
	}
	return bytes.Index(b.buf, p)
}

// Equal if current Slice is equal to another Slice, compared by underlying byte slice
func (b *Slice) Equal(ob *Slice) bool {
	return bytes.Equal(ob.buf, b.buf)
}

// Equal if current Slice is equal to another Slice, compared by underlying byte slice
func (b *Slice) Equal2(offset int, ob *Slice, otherOffset int, length int) bool {
	srcBytes := make([]byte, length)
	b.GetBytes(srcBytes, offset, offset+length)

	destBytes := make([]byte, length)
	ob.GetBytes(destBytes, otherOffset, otherOffset+length)
	return bytes.Equal(srcBytes, destBytes)
}

func (b *Slice) CompareTo(ob *Slice) int {
	srcBytes := b.AvailableBytes()
	destBytes := ob.AvailableBytes()
	return bytes.Compare(srcBytes, destBytes)
}

func (b *Slice) CompareTo2(offset int, length int, ob *Slice, otherOffset int, otherLength int) int {
	srcBytes := make([]byte, length)
	b.GetBytes(srcBytes, offset, offset+length)

	destBytes := make([]byte, length)
	ob.GetBytes(destBytes, otherOffset, otherOffset+otherLength)
	return bytes.Compare(srcBytes, destBytes)
}

func (b *Slice) HashCodeValue(offset int, length int) int32 {
	srcBytes := make([]byte, length)
	b.GetBytes(srcBytes, offset, offset+length)
	return BytesHashCode(srcBytes)
}

func BytesHashCode(b []byte) int32 {
	v := int32(crc32.ChecksumIEEE(b))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == Min
	return 0
}

func (b *Slice) HashCode() int32 {
	return b.HashCodeValue(0, b.Size())
}

// DiscardReadBytes discard bytes that are read, adjust readerIndex/writerIndex accordingly
func (b *Slice) DiscardReadBytes() {
	b.buf = b.buf[b.readerIndex:]
	b.writerIndex -= b.readerIndex
	b.readerIndex = 0
}

// Copy deep copy to create an brand new Slice
func (b *Slice) Copy() *Slice {
	p := make([]byte, len(b.buf))
	copy(p, b.buf)
	return &Slice{
		buf: p,

		capacity: b.capacity,

		readerIndex:  b.readerIndex,
		readerMarker: b.readerMarker,
		writerIndex:  b.writerIndex,
		writerMarker: b.writerMarker,
	}
}

// ForEachByte iterate through readable bytes, ForEachByte(iterator, start), ForEachByte(iterator, start, end)
func (b *Slice) ForEachByte(iterator ByteIterator, indexes ...int) int {
	start := b.readerIndex
	end := b.writerIndex
	if len(indexes) >= 1 {
		start = indexes[0]
	}
	if len(indexes) >= 2 && indexes[1] < end {
		end = indexes[1]
	}

	if start > end {
		return 0
	}

	if end > b.capacity {
		end = b.capacity
	}

	count := 0
	for ; start < end; start++ {
		if !iterator(b.buf[start]) {
			break
		}
		count++
	}

	return count
}

// NewReader create Slice from io.Reader
func NewReader(reader io.Reader) (*Slice, error) {
	b, err := NewSlice()
	if err != nil {
		return nil, err
	}
	return b, b.FromReader(reader)
}

// String buf to string
func (b *Slice) String() string {
	return string(b.AvailableBytes())
}
