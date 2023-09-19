package iostream

import (
	"encoding"
	"io"
	"math"
)

// InputStream 流的读取器
type InputStream struct {
	src source
}

// NewInputStream creates a input stream.
func NewInputStream(src io.Reader) *InputStream {
	if r, ok := src.(*InputStream); ok {
		return r
	}
	return &InputStream{src: newSource(src)}
}

// Offset returns the number of bytes read through this reader.
func (r *InputStream) Offset() int64 {
	return r.src.Offset()
}

// Read implements io.Reader interface
func (r *InputStream) Read(p []byte) (n int, err error) {
	return r.src.Read(p)
}

func (r *InputStream) Seek(offset int64, whence int) (int64, error) {
	return r.src.Seek(offset, whence)
}

// ReadUvarint reads a variable-length Uint64 from the buffer.
func (r *InputStream) ReadUvarint() (uint64, error) {
	return r.src.ReadUvarint()
}

// ReadUint8 reads an uint8
func (r *InputStream) ReadUint8() (uint8, error) {
	return r.src.ReadByte()
}

// ReadUint16 reads an uint16
func (r *InputStream) ReadUint16() (out uint16, err error) {
	var b []byte
	if b, err = r.src.Slice(2); err == nil {
		_ = b[1] // bounds check hint to compile
		out = uint16(b[0]) | uint16(b[1])<<8
	}
	return
}

// ReadUint32 reads an uint32
func (r *InputStream) ReadUint32() (out uint32, err error) {
	var b []byte
	if b, err = r.src.Slice(4); err == nil {
		_ = b[3] // bounds check hint to compile
		out = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	}
	return
}

// ReadUint64 reads an uint64
func (r *InputStream) ReadUint64() (out uint64, err error) {
	var b []byte
	if b, err = r.src.Slice(8); err == nil {
		_ = b[7] // bounds check hint to compile
		out = uint64(b[0]) |
			uint64(b[1])<<8 |
			uint64(b[2])<<16 |
			uint64(b[3])<<24 |
			uint64(b[4])<<32 |
			uint64(b[5])<<40 |
			uint64(b[6])<<48 |
			uint64(b[7])<<56
	}
	return
}

// ReadUint 由于现代计算机都是64位的，这里假设int即是int64
func (r *InputStream) ReadUint() (uint, error) {
	out, err := r.ReadUint64()
	return uint(out), err
}

func (r *InputStream) ReadVarint() (int64, error) {
	return r.src.ReadVarint()
}

// ReadInt8 reads an int8
func (r *InputStream) ReadInt8() (int8, error) {
	u, err := r.ReadUint8()
	return int8(u), err
}

// ReadInt16 reads an int16
func (r *InputStream) ReadInt16() (int16, error) {
	u, err := r.ReadUint16()
	return int16(u), err
}

// ReadInt32 reads an int32
func (r *InputStream) ReadInt32() (int32, error) {
	u, err := r.ReadUint32()
	return int32(u), err
}

// ReadInt64 reads an int64
func (r *InputStream) ReadInt64() (int64, error) {
	u, err := r.ReadUint64()
	return int64(u), err
}

// ReadInt reads an int
func (r *InputStream) ReadInt() (int, error) {
	u, err := r.ReadInt64()
	return int(u), err
}

// ReadFloat32 reads a float32
func (r *InputStream) ReadFloat32() (out float32, err error) {
	var v uint32
	if v, err = r.ReadUint32(); err == nil {
		out = math.Float32frombits(v)
	}
	return
}

// ReadFloat64 reads a float64
func (r *InputStream) ReadFloat64() (out float64, err error) {
	var v uint64
	if v, err = r.ReadUint64(); err == nil {
		out = math.Float64frombits(v)
	}
	return
}

// sliceBytes 读取一个由长度分隔的string byte.
// 返回给包外是不安全的
func (r *InputStream) sliceBytes() (out []byte, err error) {
	size, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	return r.src.Slice(int(size))
}

// ReadBinary 从流中读取bytes，并按实现encoding.BinaryUnmarshaler接口的函数的方式
// 进行unmarshaler
func (r *InputStream) ReadBinary(v encoding.BinaryUnmarshaler) error {
	b, err := r.sliceBytes()
	if err != nil {
		return err
	}
	return v.UnmarshalBinary(b)
}

// ReadText 同上，只是unmarshaler方法换成实现encoding.TextUnmarshaler接口的
func (r *InputStream) ReadText(v encoding.TextUnmarshaler) error {
	b, err := r.sliceBytes()
	if err != nil {
		return err
	}
	return v.UnmarshalText(b)
}

// ReadSelf 通过io.ReaderFrom由reader本身读取
func (r *InputStream) ReadSelf(v io.ReaderFrom) error {
	_, err := v.ReadFrom(r)
	return err
}

// ReadString 通过可变长度的读取string
func (r *InputStream) ReadString() (out string, err error) {
	var b []byte
	if b, err = r.ReadBytes(); err == nil {
		out = toString(&b)
	}
	return
}

// ReadBytes 读取可变长度的byte string
func (r *InputStream) ReadBytes() (out []byte, err error) {
	size, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out = make([]byte, int(size))
	_, err = io.ReadAtLeast(r.src, out, int(size))
	return
}

// ReadRange 通过回调函数从stream里读取指定长度的数组
func (r *InputStream) ReadRange(fn func(i int, r *InputStream) error) error {
	length, err := r.ReadUvarint()
	if err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		if err := fn(i, r); err != nil {
			return err
		}
	}
	return nil
}

// ReadBool 由流中读取一个bool值
func (r *InputStream) ReadBool() (bool, error) {
	b, err := r.src.ReadByte()
	return b == 1, err
}
