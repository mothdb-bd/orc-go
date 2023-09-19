package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

var MAX_BUFFER_SIZE int32 = 4 * 1024 * 1024

func CreateMothDecompressor(mothDataSourceId *common.MothDataSourceId, compression metadata.CompressionKind, bufferSize int32) *optional.Optional[MothDecompressor] {
	if (compression != metadata.NONE) && ((bufferSize <= 0) || (bufferSize > MAX_BUFFER_SIZE)) {
		panic(fmt.Sprintf("Invalid compression block size: %d", bufferSize))
	}
	switch compression {
	case metadata.NONE:
		return optional.Empty[MothDecompressor]()
	case metadata.ZLIB:
		var zlib MothDecompressor = NewMothZlibDecompressor(mothDataSourceId, bufferSize)
		return optional.Of(zlib)
	case metadata.SNAPPY:
		var snappy MothDecompressor = NewMothSnappyDecompressor(mothDataSourceId, bufferSize)
		return optional.Of(snappy)
	case metadata.LZ4:
		var lz4 MothDecompressor = NewMothLz4Decompressor(mothDataSourceId, bufferSize)
		return optional.Of(lz4)
	case metadata.ZSTD:
		var zstd MothDecompressor = NewMothZstdDecompressor(mothDataSourceId, bufferSize)
		return optional.Of(zstd)
	}
	panic(fmt.Sprintf("Unknown compression type: %d", compression))
}

type MothDecompressor interface {
	Decompress(input []byte, offset int32, length int32, output OutputBuffer) int32

	String() string
}

type OutputBuffer interface {
	Initialize(size int32) []byte
	Grow(size int32) []byte
}
