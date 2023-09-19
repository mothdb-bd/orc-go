package store

// import (
// 	"bytes"
// 	"fmt"

// 	"github.com/mothdb-bd/orc-go/pkg/util"
// 	"github.com/DataDog/zstd"
// )

// type ZstdCompressor struct {
// 	// 继承
// 	Compressor
// }

// func NewZstdCompressor() Compressor {
// 	return new(ZstdCompressor)
// }

// //@Override
// func (sr *ZstdCompressor) MaxCompressedLength(uncompressedSize int32) int32 {
// 	return int32(zstd.CompressBound(int(uncompressedSize)))
// }

// //@Override
// func (sr *ZstdCompressor) Compress(input []byte, inputOffset int32, inputLength int32, output []byte, outputOffset int32, maxOutputLength int32) int32 {
// 	maxCompressedLength := sr.MaxCompressedLength(inputLength)
// 	if maxOutputLength < maxCompressedLength {
// 		panic(fmt.Sprintf("Output buffer must be at least %d bytes", maxCompressedLength))
// 	}

// 	dest := make([]byte, maxCompressedLength)
// 	zstd.Compress(input[inputOffset:inputOffset+inputLength], dest)

// 	util.CopyArrays(dest, 0, output, outputOffset, maxCompressedLength)

// 	return maxCompressedLength
// }

// //@Override
// func (dr *ZstdCompressor) Decompress2(input *bytes.Buffer, output *bytes.Buffer) {
// 	panic("Compression of byte buffer not supported for snappy")
// }
