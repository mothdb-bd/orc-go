package store

import (
	"bytes"
	"fmt"

	"github.com/pierrec/lz4"
)

type Lz4Compressor struct {
	// 继承
	Compressor
}

func NewLz4Compressor() Compressor {
	return new(Lz4Compressor)
}

//@Override
func (sr *Lz4Compressor) MaxCompressedLength(uncompressedSize int32) int32 {
	return int32(lz4.CompressBlockBound(int(uncompressedSize)))
}

//@Override
func (sr *Lz4Compressor) Compress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32 {
	maxCompressedLength := sr.MaxCompressedLength(inputLength)
	if maxOutputLength < maxCompressedLength {
		panic(fmt.Sprintf("Output buffer must be at least %d bytes", maxCompressedLength))
	}

	ht := make([]int, 64<<10) // buffer for the compression table
	size, _ := lz4.CompressBlock(input[inputOffset:inputOffset+inputLength], output[outputOffset:outputOffset+maxCompressedLength], ht)

	return int32(size)
}

//@Override
func (dr *Lz4Compressor) Decompress2(input *bytes.Buffer, output *bytes.Buffer) {
	panic("Compression of byte buffer not supported for snappy")
}
