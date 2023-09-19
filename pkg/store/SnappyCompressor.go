package store

import (
	"bytes"
	"fmt"

	"github.com/golang/snappy"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type SnappyCompressor struct {
	// 继承
	Compressor
}

func NewSnappyCompressor() Compressor {
	return new(SnappyCompressor)
}

// @Override
func (sr *SnappyCompressor) MaxCompressedLength(uncompressedSize int32) int32 {
	return int32(snappy.MaxEncodedLen(int(uncompressedSize)))
}

// @Override
func (sr *SnappyCompressor) Compress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32 {
	maxCompressedLength := sr.MaxCompressedLength(inputLength)
	if maxOutputLength < maxCompressedLength {
		panic(fmt.Sprintf("Output buffer must be at least %d bytes", maxCompressedLength))
	}

	b := snappy.Encode(output[outputOffset:outputOffset+maxCompressedLength], input[inputOffset:inputOffset+inputLength])
	return util.Lens(b)
}

// @Override
func (dr *SnappyCompressor) Decompress2(input *bytes.Buffer, output *bytes.Buffer) {
	panic("Compression of byte buffer not supported for snappy")
}
