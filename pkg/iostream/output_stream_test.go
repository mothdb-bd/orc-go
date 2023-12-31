package iostream

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var Fixtures = map[string]struct {
	Encode func(*OutputStream) error
	Decode func(*InputStream) (interface{}, error)
	Buffer []byte
	Value  interface{}
}{
	"uvarint": {
		Encode: func(w *OutputStream) error { return w.WriteUvarint(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUvarint() },
		Buffer: []byte{0x91, 0xa2, 0xc4, 0x88, 0x91, 0xa2, 0xc4, 0x88, 0x11},
		Value:  uint64(0x1111111111111111),
	},
	"uint": {
		Encode: func(w *OutputStream) error { return w.WriteUint(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUint() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		Value:  uint(0x1111111111111111),
	},
	"uint8": {
		Encode: func(w *OutputStream) error { return w.WriteUint8(0x11) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUint8() },
		Buffer: []byte{0x11},
		Value:  uint8(0x11),
	},
	"uint16": {
		Encode: func(w *OutputStream) error { return w.WriteUint16(0x1111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUint16() },
		Buffer: []byte{0x11, 0x11},
		Value:  uint16(0x1111),
	},
	"uint32": {
		Encode: func(w *OutputStream) error { return w.WriteUint32(0x11111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUint32() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11},
		Value:  uint32(0x11111111),
	},
	"uint64": {
		Encode: func(w *OutputStream) error { return w.WriteUint64(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadUint64() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		Value:  uint64(0x1111111111111111),
	},
	"varint": {
		Encode: func(w *OutputStream) error { return w.WriteVarint(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadVarint() },
		Buffer: []byte{0xa2, 0xc4, 0x88, 0x91, 0xa2, 0xc4, 0x88, 0x91, 0x22},
		Value:  int64(0x1111111111111111),
	},
	"varint-negative": {
		Encode: func(w *OutputStream) error { return w.WriteVarint(-0x10) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadVarint() },
		Buffer: []byte{0x1f},
		Value:  int64(-0x10),
	},
	"int": {
		Encode: func(w *OutputStream) error { return w.WriteInt(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadInt() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		Value:  int(0x1111111111111111),
	},
	"int8": {
		Encode: func(w *OutputStream) error { return w.WriteInt8(0x11) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadInt8() },
		Buffer: []byte{0x11},
		Value:  int8(0x11),
	},
	"int16": {
		Encode: func(w *OutputStream) error { return w.WriteInt16(0x1111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadInt16() },
		Buffer: []byte{0x11, 0x11},
		Value:  int16(0x1111),
	},
	"int32": {
		Encode: func(w *OutputStream) error { return w.WriteInt32(0x11111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadInt32() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11},
		Value:  int32(0x11111111),
	},
	"int64": {
		Encode: func(w *OutputStream) error { return w.WriteInt64(0x1111111111111111) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadInt64() },
		Buffer: []byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		Value:  int64(0x1111111111111111),
	},
	"float32": {
		Encode: func(w *OutputStream) error { return w.WriteFloat32(0x11) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadFloat32() },
		Buffer: []byte{0x0, 0x0, 0x88, 0x41},
		Value:  float32(0x11),
	},
	"float64": {
		Encode: func(w *OutputStream) error { return w.WriteFloat64(0x11) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadFloat64() },
		Buffer: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x31, 0x40},
		Value:  float64(0x11),
	},
	"string": {
		Encode: func(w *OutputStream) error { return w.WriteString("hello") },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadString() },
		Buffer: []byte{0x5, 0x68, 0x65, 0x6c, 0x6c, 0x6f},
		Value:  "hello",
	},
	"bytes": {
		Encode: func(w *OutputStream) error { return w.WriteBytes([]byte("hello")) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadBytes() },
		Buffer: []byte{0x5, 0x68, 0x65, 0x6c, 0x6c, 0x6f},
		Value:  []byte("hello"),
	},
	"bool": {
		Encode: func(w *OutputStream) error { return w.WriteBool(true) },
		Decode: func(r *InputStream) (interface{}, error) { return r.ReadBool() },
		Buffer: []byte{0x1},
		Value:  true,
	},
	"time-binary": {
		Encode: func(w *OutputStream) error { return w.WriteBinary(time.Unix(60, 0).UTC()) },
		Decode: func(r *InputStream) (interface{}, error) {
			var out time.Time
			err := r.ReadBinary(&out)
			return out, err
		},
		Buffer: []byte{0xf, 0x1, 0x0, 0x0, 0x0, 0xe, 0x77, 0x91, 0xf7, 0x3c, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff},
		Value:  time.Unix(60, 0).UTC(),
	},
	"time-text": {
		Encode: func(w *OutputStream) error { return w.WriteText(time.Unix(60, 0).UTC()) },
		Decode: func(r *InputStream) (interface{}, error) {
			var out time.Time
			err := r.ReadText(&out)
			return out, err
		},
		Buffer: []byte{0x14, 0x31, 0x39, 0x37, 0x30, 0x2d, 0x30, 0x31, 0x2d, 0x30, 0x31, 0x54, 0x30, 0x30, 0x3a, 0x30, 0x31, 0x3a, 0x30, 0x30, 0x5a},
		Value:  time.Unix(60, 0).UTC(),
	},
	"person": {
		Encode: func(w *OutputStream) error {
			return w.WriteSelf(&person{Name: "Roman"})
		},
		Decode: func(r *InputStream) (interface{}, error) {
			var out person
			err := r.ReadSelf(&out)
			return out, err
		},
		Buffer: []byte{0x5, 0x52, 0x6f, 0x6d, 0x61, 0x6e},
		Value:  person{Name: "Roman"},
	},
	"range": {
		Encode: func(w *OutputStream) error {
			v := []person{{Name: "Roman"}, {Name: "Florimond"}}
			return w.WriteRange(len(v), func(i int, w *OutputStream) error {
				return w.WriteSelf(&v[i])
			})
		},
		Decode: func(r *InputStream) (interface{}, error) {
			var arr []person
			err := r.ReadRange(func(i int, r *InputStream) error {
				var out person
				if err := r.ReadSelf(&out); err != nil {
					return err
				}

				arr = append(arr, out)
				return nil
			})
			return arr, err
		},
		Buffer: []byte{0x2, 0x5, 0x52, 0x6f, 0x6d, 0x61, 0x6e, 0x9, 0x46, 0x6c, 0x6f, 0x72, 0x69, 0x6d, 0x6f, 0x6e, 0x64},
		Value:  []person{{Name: "Roman"}, {Name: "Florimond"}},
	},
}

func TestWrite(t *testing.T) {
	for n, tc := range Fixtures {
		assertWrite(t, n, tc.Encode, tc.Buffer)
	}
}

func TestWriteFailuresString(t *testing.T) {
	assertWriteN(t, "string-err", func(w *OutputStream) error {
		return w.WriteString("hello")
	}, nil, 0)

	assertWriteN(t, "bytes-err", func(w *OutputStream) error {
		return w.WriteBytes([]byte("hello"))
	}, nil, 0)
}

func TestWriteFailures(t *testing.T) {
	for n, tc := range Fixtures {
		for x := 0; x < int(len(tc.Buffer))-1; x++ {
			assertWriteN(t, n, tc.Encode, nil, x)
		}
	}
}

func TestWriteMethod(t *testing.T) {
	w := NewOutputStream(bytes.NewBuffer(nil))
	_, err := w.Write(nil)
	assert.NoError(t, err)
}

func TestNewOutputStream(t *testing.T) {
	w1 := NewOutputStream(bytes.NewBuffer(nil))
	w2 := NewOutputStream(w1)
	assert.Equal(t, w1, w2)
	assert.NoError(t, w1.Close())
}

func TestWriterClose(t *testing.T) {
	w := NewOutputStream(new(limitWriter))
	assert.NoError(t, w.Close())
}

func TestWriterFlush(t *testing.T) {
	assert.NoError(t, NewOutputStream(new(limitWriter)).Flush())
	assert.NoError(t, NewOutputStream(bytes.NewBuffer(nil)).Flush())
}

// assertWrite asserts a single write operation
func assertWrite(t *testing.T, name string, fn func(*OutputStream) error, expect []byte) {
	assertWriteN(t, name, fn, expect, 99999)
}

// assertWriteMax asserts a single write operation
func assertWriteN(t *testing.T, name string, fn func(*OutputStream) error, expect []byte, max int) {
	t.Run(name, func(t *testing.T) {
		msg := fmt.Sprintf("write %v", name)
		dst := newLimitWriter(max)
		wrt := NewOutputStream(dst)

		wrt.Reset(dst)
		err := fn(wrt)

		// Failure case, must have an error
		if expect == nil {
			assert.Error(t, err, msg)
			return
		}

		// Successfully encoded, check the output
		assert.NoError(t, err, msg)
		assert.Equal(t, expect, dst.buffer.Bytes(), msg)
		assert.Equal(t, len(expect), int(wrt.Offset()))
	})
}

// limitWriter for test only
type limitWriter struct {
	buffer *bytes.Buffer
	value  uint32
	Limit  int
}

func newLimitWriter(limit int) *limitWriter {
	return &limitWriter{
		buffer: bytes.NewBuffer(nil),
		Limit:  limit,
	}
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if n := atomic.AddUint32(&w.value, uint32(len(p))); int(n) > w.Limit {
		return 0, io.ErrShortBuffer
	}

	return w.buffer.Write(p)
}

func (w *limitWriter) Close() error {
	return nil
}

func (w *limitWriter) Flush() error {
	return nil
}

type person struct {
	Name string
}

func (p *person) WriteTo(dst io.Writer) (int64, error) {
	w := NewOutputStream(dst)
	err := w.WriteString(p.Name)
	return w.Offset(), err
}

func (p *person) ReadFrom(src io.Reader) (int64, error) {
	r := NewInputStream(src)
	name, err := r.ReadString()
	p.Name = name
	return r.Offset(), err
}
