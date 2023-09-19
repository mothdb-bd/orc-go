package store

import (
	"bytes"

	"github.com/mothdb-bd/orc-go/pkg/slice"
)

// 压缩
type Compressor interface {
	MaxCompressedLength(uncompressedSize int32) int32

	/**
	 * @return number of bytes written to the output
	 */
	Compress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32

	Compress2(input *bytes.Buffer, output *bytes.Buffer)
}

type BufferWriter struct {
	slice *slice.Slice
}

func NewBufferWriter(bufferSize int) *BufferWriter {
	br := new(BufferWriter)
	br.slice = slice.NewWithSize(bufferSize)
	return br
}

func CreateBufferWriter(baseBuf []byte) *BufferWriter {
	br := new(BufferWriter)
	br.slice = slice.NewBaseBuf(baseBuf)
	return br
}

func (br *BufferWriter) Write(p []byte) (n int, err error) {
	return br.slice.Write(p)
}

func (br *BufferWriter) Bytes() []byte {
	return br.slice.AvailableBytes()
}

func (br *BufferWriter) Size() int32 {
	return br.slice.SizeInt32()
}
