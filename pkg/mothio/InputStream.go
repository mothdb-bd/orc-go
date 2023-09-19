package mothio

import (
	"errors"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
)

var MAX_SKIP_BUFFER_SIZE int32 = 2048

type InputStream interface {
	ReadBS() (byte, error)
	ReadBS2(b []byte) (n int, err error)
	ReadBS3(b []byte, off int, l int) (n int, err error)
	Skip(n int64) int64
	Available() int32
	Close()
	Mark(readlimit int32)
	Reset()
	MarkSupported() bool

	GetReader() io.Reader
}

type InputStreamStruct struct {
	// 继承
	InputStream

	reader io.ReadCloser
}

func NewInputStream(reader io.ReadCloser) InputStream {
	om := new(InputStreamStruct)
	om.reader = reader
	return om
}

func (im *InputStreamStruct) ReadBS() (byte, error) {
	b := make([]byte, 1)
	i, e := im.ReadBS2(b)
	if e == nil && i > 0 {
		return b[0], nil
	} else {
		return 0, e
	}
}

func (im *InputStreamStruct) ReadBS2(b []byte) (n int, err error) {
	return im.reader.Read(b)
}

func (im *InputStreamStruct) ReadBS3(b []byte, off int, l int) (n int, err error) {
	bLen := len(b)
	if b == nil {
		return -1, errors.New("nil")
	} else if off < 0 || l < 0 || l > bLen-off {
		return -1, errors.New("index out of bounds")
	} else if l == 0 {
		return 0, nil
	}
	r, e := im.ReadBS2(b[off : off+l])
	return r, e
}

func (im *InputStreamStruct) Skip(n int64) int64 {
	remaining := n
	var nr int
	if n <= 0 {
		return 0
	}
	size := int32(maths.Min(int64(MAX_SKIP_BUFFER_SIZE), remaining))
	skipBuffer := make([]byte, size)
	for remaining > 0 {
		nr, _ = im.ReadBS3(skipBuffer, 0, int(maths.Min(int64(size), remaining)))
		if nr < 0 {
			break
		}
		remaining -= int64(nr)
	}
	return n - remaining
}

func (im *InputStreamStruct) Available() int32 {
	return 0
}

func (im *InputStreamStruct) Close() {
	im.reader.Close()
}

func (im *InputStreamStruct) Mark(readlimit int32) {
}

func (im *InputStreamStruct) Reset() {
	panic("mark/reset not supported")
}

func (im *InputStreamStruct) MarkSupported() bool {
	return false
}
