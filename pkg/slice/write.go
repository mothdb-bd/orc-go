package slice

import "io"

// Write implements io.Writer
func (b *Slice) Write(p []byte) (int, error) {
	n := len(p)
	b.EnsureCapacity(b.writerIndex + n)

	copy(b.buf[b.writerIndex:], p)
	b.writerIndex += n

	return n, nil
}

// WriteBool write a bool
func (b *Slice) WriteBool(val bool) error {
	b.EnsureCapacity(b.writerIndex + 1)
	b.PutBool(b.writerIndex, val)
	b.writerIndex++
	return nil
}

// WriteByte write a byte
func (b *Slice) WriteByte(val byte) error {
	b.EnsureCapacity(b.writerIndex + 1)
	b.PutByte(b.writerIndex, val)
	b.writerIndex++
	return nil
}

// WriteBytes write bytes
func (b *Slice) WriteBytes(p []byte) error {
	b.EnsureCapacity(b.writerIndex + len(p))
	b.PutBytes(b.writerIndex, p)
	b.writerIndex += len(p)
	return nil
}

func (b *Slice) WriteSlice(slice *Slice, offset int, length int) error {
	// bytes := make([]byte, length)
	// slice.GetBytes(bytes, offset, offset+length)
	return b.WriteBytes(slice.buf[offset : offset+length])
}

// WriteUInt8 write an uint8
func (b *Slice) WriteUInt8(val uint8) error {
	b.EnsureCapacity(b.writerIndex + 1)
	b.PutUInt8(b.writerIndex, val)
	b.writerIndex++
	return nil
}

// WriteInt8 write an int8
func (b *Slice) WriteInt8(val int8) error {
	b.EnsureCapacity(b.writerIndex + 1)
	b.PutInt8(b.writerIndex, val)
	b.writerIndex++
	return nil
}

// WriteUInt16BE write an uint16 in big endian
func (b *Slice) WriteUInt16BE(val uint16) error {
	b.EnsureCapacity(b.writerIndex + 2)
	b.PutUInt16BE(b.writerIndex, val)
	b.writerIndex += 2
	return nil
}

// WriteUInt16LE write an uint16 in little endian
func (b *Slice) WriteUInt16LE(val uint16) error {
	b.EnsureCapacity(b.writerIndex + 2)
	b.PutUInt16LE(b.writerIndex, val)
	b.writerIndex += 2
	return nil
}

// WriteInt16BE write an int16 in big endian
func (b *Slice) WriteInt16BE(val int16) error {
	b.EnsureCapacity(b.writerIndex + 2)
	b.PutInt16BE(b.writerIndex, val)
	b.writerIndex += 2
	return nil
}

// WriteInt16LE write an int16 in little endian
func (b *Slice) WriteInt16LE(val int16) error {
	b.EnsureCapacity(b.writerIndex + 2)
	b.PutInt16LE(b.writerIndex, val)
	b.writerIndex += 2
	return nil
}

// WriteUInt32BE write an uint32 in big endian
func (b *Slice) WriteUInt32BE(val uint32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutUInt32BE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteUInt32LE write an uint32 in little endian
func (b *Slice) WriteUInt32LE(val uint32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutUInt32LE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteInt32BE write an int32 in big endian
func (b *Slice) WriteInt32BE(val int32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutInt32BE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteInt32LE write an int32 in little endian
func (b *Slice) WriteInt32LE(val int32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutInt32LE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteUInt64BE write an uint64 in big endian
func (b *Slice) WriteUInt64BE(val uint64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutUInt64BE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteUInt64LE write an uint64 in little endian
func (b *Slice) WriteUInt64LE(val uint64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutUInt64LE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteInt64BE write an int64 in big endian
func (b *Slice) WriteInt64BE(val int64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutInt64BE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteInt64LE write an int64 in little endian
func (b *Slice) WriteInt64LE(val int64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutInt64LE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteFloat32BE write a float32 in big endian
func (b *Slice) WriteFloat32BE(val float32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutFloat32BE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteFloat32LE write a float32 in little endian
func (b *Slice) WriteFloat32LE(val float32) error {
	b.EnsureCapacity(b.writerIndex + 4)
	b.PutFloat32LE(b.writerIndex, val)
	b.writerIndex += 4
	return nil
}

// WriteFloat64BE write a float64 in big endian
func (b *Slice) WriteFloat64BE(val float64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutFloat64BE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteFloat64LE write a float64 in little endian
func (b *Slice) WriteFloat64LE(val float64) error {
	b.EnsureCapacity(b.writerIndex + 8)
	b.PutFloat64LE(b.writerIndex, val)
	b.writerIndex += 8
	return nil
}

// WriteTo write to io.Writer
func (b *Slice) ToWriter(writer io.Writer) error {
	for {
		chunk := make([]byte, ChunkSize)
		n, err := b.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		_, err = writer.Write(chunk[:n])
		if err != nil {
			return err
		}
	}

	return nil
}
