package iostream

import (
	"encoding"
	"io"
	"math"
)

// OutputStream 流的写入器
type OutputStream struct {
	bytes  [10]byte
	out    io.Writer
	offset int64
}

// NewOutputStream creates a output stream .
func NewOutputStream(out io.Writer) *OutputStream {
	if w, ok := out.(*OutputStream); ok {
		return w
	}
	return &OutputStream{
		out: out,
	}
}

// Reset resets the writer and makes it ready to be reused.
func (w *OutputStream) Reset(out io.Writer) {
	w.out = out
	w.offset = 0
}

// Offset returns the number of bytes written through this writer.
func (w *OutputStream) Offset() int64 {
	return w.offset
}

// Write implements io.Writer interface
func (w *OutputStream) Write(p []byte) (int, error) {
	n, err := w.out.Write(p)
	w.offset += int64(n)
	return n, err
}

// Write writes the contents of p into the buffer.
func (w *OutputStream) write(p []byte) error {
	n, err := w.out.Write(p)
	w.offset += int64(n)
	return err
}

// Flush flushes the writer to the underlying stream and returns its error. If
// the underlying io.Writer does not have a Flush() error method, it's a no-op.
func (w *OutputStream) Flush() error {
	if flusher, ok := w.out.(interface {
		Flush() error
	}); ok {
		return flusher.Flush()
	}
	return nil
}

// Close closes the writer's underlying stream and return its error. If the
// underlying io.Writer is not an io.Closer, it's a no-op.
func (w *OutputStream) Close() error {
	if closer, ok := w.out.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// WriteUvarint writes a variable size unsigned integer
func (w *OutputStream) WriteUvarint(x uint64) error {
	i := 0
	for x >= 0x80 {
		w.bytes[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	w.bytes[i] = byte(x)
	return w.write(w.bytes[:(i + 1)])
}

// WriteUint writes a Uint
func (w *OutputStream) WriteUint(v uint) error {
	return w.WriteUint64(uint64(v))
}

// WriteUint8 writes a Uint8
func (w *OutputStream) WriteUint8(v uint8) error {
	w.bytes[0] = v
	return w.write(w.bytes[:1])
}

// WriteUint16 writes a Uint16
func (w *OutputStream) WriteUint16(v uint16) error {
	w.bytes[0] = byte(v)
	w.bytes[1] = byte(v >> 8)
	return w.write(w.bytes[:2])
}

// WriteUint32 writes a Uint32
func (w *OutputStream) WriteUint32(v uint32) error {
	w.bytes[0] = byte(v)
	w.bytes[1] = byte(v >> 8)
	w.bytes[2] = byte(v >> 16)
	w.bytes[3] = byte(v >> 24)
	return w.write(w.bytes[:4])
}

// WriteUint64 writes a Uint64
func (w *OutputStream) WriteUint64(v uint64) error {
	w.bytes[0] = byte(v)
	w.bytes[1] = byte(v >> 8)
	w.bytes[2] = byte(v >> 16)
	w.bytes[3] = byte(v >> 24)
	w.bytes[4] = byte(v >> 32)
	w.bytes[5] = byte(v >> 40)
	w.bytes[6] = byte(v >> 48)
	w.bytes[7] = byte(v >> 56)
	return w.write(w.bytes[:8])
}

// WriteVarint writes a variable size signed integer
func (w *OutputStream) WriteVarint(v int64) error {
	x := uint64(v) << 1
	if v < 0 {
		x = ^x
	}

	i := 0
	for x >= 0x80 {
		w.bytes[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	w.bytes[i] = byte(x)
	return w.write(w.bytes[:(i + 1)])
}

// WriteInt writes an int
func (w *OutputStream) WriteInt(v int) error {
	return w.WriteUint64(uint64(v))
}

// WriteInt8 writes an int8
func (w *OutputStream) WriteInt8(v int8) error {
	return w.WriteUint8(uint8(v))
}

// WriteInt16 writes an int16
func (w *OutputStream) WriteInt16(v int16) error {
	return w.WriteUint16(uint16(v))
}

// WriteInt32 writes an int32
func (w *OutputStream) WriteInt32(v int32) error {
	return w.WriteUint32(uint32(v))
}

// WriteInt64 writes an int64
func (w *OutputStream) WriteInt64(v int64) error {
	return w.WriteUint64(uint64(v))
}

// WriteFloat32 a 32-bit floating point number
func (w *OutputStream) WriteFloat32(v float32) error {
	return w.WriteUint32(math.Float32bits(v))
}

// WriteFloat64 a 64-bit floating point number
func (w *OutputStream) WriteFloat64(v float64) error {
	return w.WriteUint64(math.Float64bits(v))
}

// WriteBinary marshals the type to its binary representation and writes it
// downstream, prefixed with its size as a variable-size integer.
func (w *OutputStream) WriteBinary(v encoding.BinaryMarshaler) error {
	out, err := v.MarshalBinary()
	if err == nil {
		err = w.WriteBytes(out)
	}
	return err
}

// WriteText marshals the type to its text representation and writes it
// downstream, prefixed with its size as a variable-size integer.
func (w *OutputStream) WriteText(v encoding.TextMarshaler) error {
	out, err := v.MarshalText()
	if err == nil {
		err = w.WriteBytes(out)
	}
	return err
}

// WriteSelf uses the provider io.WriterTo in order to write the data into
// the destination writer.
func (w *OutputStream) WriteSelf(v io.WriterTo) error {
	_, err := v.WriteTo(w)
	return err
}

// --------------------------- Strings ---------------------------

// WriteString writes a string prefixed with a variable-size integer.
func (w *OutputStream) WriteString(v string) error {
	if err := w.WriteUvarint(uint64(len(v))); err != nil {
		return err
	}
	return w.write(toBytes(v))
}

// WriteBytes writes a byte slice prefixed with a variable-size integer.
func (w *OutputStream) WriteBytes(v []byte) error {
	if err := w.WriteUvarint(uint64(len(v))); err != nil {
		return err
	}
	return w.write(v)
}

// WriteRange writes a specified length of an array and for each element of that
// array calls the callback function with its index.
func (w *OutputStream) WriteRange(length int, fn func(i int, w *OutputStream) error) error {
	if err := w.WriteUvarint(uint64(length)); err != nil {
		return err
	}

	for i := 0; i < length; i++ {
		if err := fn(i, w); err != nil {
			return err
		}
	}

	return nil
}

// WriteBool writes a single boolean value into the buffer
func (w *OutputStream) WriteBool(v bool) error {
	w.bytes[0] = 0
	if v {
		w.bytes[0] = 1
	}
	return w.write(w.bytes[:1])
}
