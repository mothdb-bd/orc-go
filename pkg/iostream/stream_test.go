package iostream

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertString(t *testing.T) {
	s := "hello world"
	b := toBytes(s)
	assert.NotEmpty(t, b)
	assert.Equal(t, s, string(b))

	other := toString(&b)
	assert.NotEmpty(t, b)
	assert.Equal(t, s, other)
}

func TestNewSource(t *testing.T) {
	assert.IsType(t, &sliceSource{}, newSource(nil))
	assert.IsType(t, &sliceSource{}, newSource(&sliceSource{}))
	assert.IsType(t, &streamSource{}, newSource(&fakeSource{}))
	assert.IsType(t, &streamSource{}, newStreamSource(&bytes.Buffer{}))
}

func TestReadUvarintEOF(t *testing.T) {
	input := []byte{0x91, 0xa2, 0xc4, 0x88, 0x91, 0xa2, 0xc4, 0x88, 0x11}
	for size := 0; size < len(input)-1; size++ {
		src := newSource(bytes.NewBuffer(input[:size]))
		_, err := src.ReadUvarint()
		assert.ErrorIs(t, err, io.EOF)
	}
}

func TestReadUvarintOverflow(t *testing.T) {
	input := []byte{0x91, 0xa2, 0xc4, 0x88, 0x91, 0xa2, 0xc4, 0x88, 0xa2, 0xc4, 0x88, 0x11}
	for size := 0; size < len(input)-1; size++ {
		src := newSource(bytes.NewBuffer(input[:size]))
		_, err := src.ReadUvarint()
		assert.Error(t, err)
	}
}

func TestReadUvarintUnexpectedOverflow(t *testing.T) {
	input := []byte{0x91, 0xa2, 0xc4, 0x88, 0x91, 0xa2, 0xc4, 0x88, 0x91, 0x11}
	src := newSource(bytes.NewBuffer(input))
	_, err := src.ReadUvarint()
	assert.ErrorIs(t, err, errOverflow)
}

func TestReadEOF(t *testing.T) {
	src := newSource(bytes.NewBuffer(nil))
	_, err := src.Read([]byte{})
	assert.ErrorIs(t, err, io.EOF)
}

func TestReadByteEOF(t *testing.T) {
	src := newSource(bytes.NewBuffer(nil))
	_, err := src.ReadByte()
	assert.ErrorIs(t, err, io.EOF)
}

func TestSliceEOF(t *testing.T) {
	src := newSliceSource([]byte{})
	_, err := src.Slice(10)
	assert.ErrorIs(t, err, io.EOF)
}

type fakeSource struct {
	r io.Reader
}

func newFakeSource(data []byte) io.Reader {
	return &fakeSource{
		r: bytes.NewBuffer(data),
	}
}

func (f *fakeSource) Read(p []byte) (n int, err error) {
	return f.r.Read(p)
}
