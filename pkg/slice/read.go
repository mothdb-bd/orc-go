package slice

import (
	"fmt"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
)

// Read implements io.Reader
func (b *Slice) Read(p []byte) (int, error) {
	if b.readerIndex >= b.writerIndex {
		return 0, io.EOF
	}

	n := len(p)
	if len(p)+b.readerIndex > b.writerIndex {
		n = b.writerIndex - b.readerIndex
	}

	copy(p, b.buf[b.readerIndex:b.readerIndex+n])
	b.readerIndex += n

	return n, nil
}

// ReadBool read a bool
func (b *Slice) ReadBool() (bool, error) {
	if b.readerIndex > b.writerIndex-1 {
		return false, ErrOutOfBound
	}

	val, _ := b.GetBool(b.readerIndex)
	b.readerIndex++
	return val, nil
}

// ReadByte read a byte
func (b *Slice) ReadByte() (byte, error) {
	if b.readerIndex > b.writerIndex-1 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetByte(b.readerIndex)
	b.readerIndex++
	return val, nil
}

// ReadBytes read bytes
func (b *Slice) ReadBytes(p []byte) (int, error) {
	if b.readerIndex > b.writerIndex-len(p) {
		return 0, ErrOutOfBound
	}
	end := maths.MinInt(b.readerIndex+len(p), b.writerIndex)
	n, _ := b.GetBytes(p, b.readerIndex, end)
	b.readerIndex += n
	return n, nil
}

// ReadSlice read bytes with given size into a new Slice
func (b *Slice) ReadSlice(size int) (*Slice, error) {
	p := make([]byte, size)
	n, err := b.ReadBytes(p)
	if err == nil || err == io.EOF {
		p = p[:n]
		return NewWithBuf(p), err
	} else {
		return nil, err
	}
}

// ReadUInt8 read a uint8
func (b *Slice) ReadUInt8() (uint8, error) {
	if b.readerIndex > b.writerIndex-1 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt8(b.readerIndex)
	b.readerIndex++
	return val, nil
}

// ReadInt8 read an int8
func (b *Slice) ReadInt8() (int8, error) {
	if b.readerIndex > b.writerIndex-1 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt8(b.readerIndex)
	b.readerIndex++
	return val, nil
}

// ReadUInt16BE read a uint16 in big endian
func (b *Slice) ReadUInt16BE() (uint16, error) {
	if b.readerIndex > b.writerIndex-2 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt16BE(b.readerIndex)
	b.readerIndex += 2
	return val, nil
}

// ReadUInt16LE read a uint16 in little endian
func (b *Slice) ReadUInt16LE() (uint16, error) {
	if b.readerIndex > b.writerIndex-2 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt16LE(b.readerIndex)
	b.readerIndex += 2
	return val, nil
}

// ReadInt16BE read an int16 in big endian
func (b *Slice) ReadInt16BE() (int16, error) {
	if b.readerIndex > b.writerIndex-2 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt16BE(b.readerIndex)
	b.readerIndex += 2
	return val, nil
}

// ReadInt16LE read an int16 in little endian
func (b *Slice) ReadInt16LE() (int16, error) {
	if b.readerIndex > b.writerIndex-2 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt16LE(b.readerIndex)
	b.readerIndex += 2
	return val, nil
}

// ReadUInt32BE read a uint32 in big endian
func (b *Slice) ReadUInt32BE() (uint32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt32BE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadUInt32LE read a uint32 in little endian
func (b *Slice) ReadUInt32LE() (uint32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt32LE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadInt32BE read an int32 in big endian
func (b *Slice) ReadInt32BE() (int32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt32BE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadInt32LE read an int32 in little endian
func (b *Slice) ReadInt32LE() (int32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt32LE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadUInt64BE read a uint64 in big endian
func (b *Slice) ReadUInt64BE() (uint64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt64BE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadUInt64LE read a uint64 in little endian
func (b *Slice) ReadUInt64LE() (uint64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetUInt64LE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadInt64BE read an int64 in big endian
func (b *Slice) ReadInt64BE() (int64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt64BE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadInt64LE read an int64 in little endian
func (b *Slice) ReadInt64LE() (int64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetInt64LE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadFloat32BE read a float32 in big endian
func (b *Slice) ReadFloat32BE() (float32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetFloat32BE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadFloat32LE read a float32 in little endian
func (b *Slice) ReadFloat32LE() (float32, error) {
	if b.readerIndex > b.writerIndex-4 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetFloat32LE(b.readerIndex)
	b.readerIndex += 4
	return val, nil
}

// ReadFloat64BE read a float64 in big endian
func (b *Slice) ReadFloat64BE() (float64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetFloat64BE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadFloat64LE read a float64 in little endian
func (b *Slice) ReadFloat64LE() (float64, error) {
	if b.readerIndex > b.writerIndex-8 {
		return 0, ErrOutOfBound
	}
	val, _ := b.GetFloat64LE(b.readerIndex)
	b.readerIndex += 8
	return val, nil
}

// ReadFrom read from io.reader
func (b *Slice) FromReader(reader io.Reader) error {
	for {
		chunk := make([]byte, ChunkSize)
		n, err := reader.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		_, err = b.Write(chunk[:n])
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Slice) Skip(n int) (int, error) {
	if b.readerIndex > b.writerIndex-n {
		return 0, ErrOutOfBound
	}
	b.readerIndex += n
	return n, nil
}

func (b *Slice) MakeSlice(index int, length int) (*Slice, error) {
	if length == 0 || b.Size() == 0 {
		return EMPTY_SLICE, nil
	}
	if (index == 0) && (length == b.Size()) {
		return b, nil
	}
	b.checkIndexLength(index, length)
	if length == 0 {
		return EMPTY_SLICE, nil
	}
	end := index + length
	return NewWithBuf(b.buf[index:end]), nil
}

func (b *Slice) checkIndexLength(index, length int) {
	checkPositionIndexes(index, index+length, b.Size())
}

func checkPositionIndexes(start, end, size int) {
	// Carefully optimized for execution by hotspot (explanatory comment above)
	if start < 0 || end < start || end > size {
		panic(badPositionIndexes(start, end, size))
	}
}

func badPositionIndexes(start, end, size int) string {
	if start < 0 || start > size {
		return badPositionIndex(start, size, "start index")
	}
	if end < 0 || end > size {
		return badPositionIndex(end, size, "end index")
	}
	// end < start
	return fmt.Sprintf("end index (%d) must not be less than start index (%d)", end, start)
}

func badPositionIndex(index, size int, desc string) string {
	if index < 0 {
		return fmt.Sprintf("%s (%d) must not be negative", desc, index)
	} else if size < 0 {
		return fmt.Sprintf("negative size: %d", size)
	} else { // index > size
		return fmt.Sprintf("%s (%d) must not be greater than size (%d)", desc, index, size)
	}
}
