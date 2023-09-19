package mothio

import (
	"errors"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type OutputStream interface {
	WriteB(b byte) (n int, err error)
	WriteBS(b []byte) (n int, err error)
	WriteBS2(b []byte, off int32, len int32) (n int, err error)
	Flush()
	Close() error
}

type OutputStreamStruct struct {
	//继承
	OutputStream

	writer io.WriteCloser
}

func NewOutputStream(writer io.WriteCloser) OutputStream {
	om := new(OutputStreamStruct)
	om.writer = writer
	return om
}

// 基础写操作
func (om *OutputStreamStruct) WriteB(b byte) (n int, err error) {
	return om.writer.Write([]byte{b})
}

func (om *OutputStreamStruct) WriteBS(b []byte) (n int, err error) {
	return om.writer.Write(b)
}

func (om *OutputStreamStruct) WriteBS2(b []byte, off int32, len int32) (n int, err error) {
	bLen := util.Lens(b)
	if b == nil {
		return -1, errors.New("byte is nil")
	} else if (off < 0) || (off > bLen) || (len < 0) || ((off + len) > bLen) || ((off + len) < 0) {
		return -1, errors.New("out of bounds")
	} else if len == 0 {
		return 0, nil
	}
	if off == 0 && bLen == len {
		return om.writer.Write(b)
	} else {
		return om.WriteBS(b[off : off+len : len])
	}
}

func (om *OutputStreamStruct) Flush() {

}

func (om *OutputStreamStruct) Close() error {
	return om.writer.Close()
}
