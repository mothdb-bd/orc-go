package store

import (
	"bytes"
	"compress/zlib"
	"fmt"
)

var (
	EXTRA_COMPRESSION_SPACE int32 = 16
	COMPRESSION_LEVEL       int   = 4
)

type DeflateCompressor struct {
	// 继承
	Compressor
}

func NewDeflateCompressor() Compressor {
	return new(DeflateCompressor)
}

//@Override
func (dr *DeflateCompressor) MaxCompressedLength(uncompressedSize int32) int32 {
	return uncompressedSize + ((uncompressedSize + 7) >> 3) + ((uncompressedSize + 63) >> 6) + 5 + EXTRA_COMPRESSION_SPACE
}

//@Override
func (dr *DeflateCompressor) Compress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32 {
	maxCompressedLength := dr.MaxCompressedLength(inputLength)
	if maxOutputLength < maxCompressedLength {
		panic(fmt.Sprintf("Output buffer must be at least %d bytes", maxCompressedLength))
	}
	writer := CreateBufferWriter(output[outputOffset : outputOffset+maxOutputLength])
	deflater, _ := zlib.NewWriterLevel(writer, COMPRESSION_LEVEL)
	deflater.Write(input[inputOffset : inputOffset+inputLength])

	deflater.Flush()
	deflater.Close()
	return writer.Size()
}

//@Override
func (dr *DeflateCompressor) Decompress2(input *bytes.Buffer, output *bytes.Buffer) {
	panic("Compression of byte buffer not supported for deflate")
}
