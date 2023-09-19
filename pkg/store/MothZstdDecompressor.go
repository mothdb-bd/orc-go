package store

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
)

type MothZstdDecompressor struct {
	// 继承
	MothDecompressor

	mothDataSourceId *common.MothDataSourceId
	maxBufferSize    int32
}

func NewMothZstdDecompressor(mothDataSourceId *common.MothDataSourceId, maxBufferSize int32) *MothZstdDecompressor {
	mr := new(MothZstdDecompressor)
	mr.mothDataSourceId = mothDataSourceId
	mr.maxBufferSize = maxBufferSize
	return mr
}

// @Override
func (mr *MothZstdDecompressor) Decompress(input []byte, offset int32, length int32, output OutputBuffer) int32 {

	zDecoder, _ := zstd.NewReader(bytes.NewBuffer(input[offset:offset+length]), zstd.WithDecoderMaxMemory(uint64(mr.maxBufferSize)))
	defer zDecoder.Close()

	re := output.Initialize(mr.maxBufferSize)
	uncompressedLength, _ := zDecoder.Read(re)
	if int32(uncompressedLength) > mr.maxBufferSize {
		panic(fmt.Sprintf("Zstd requires buffer (%d) larger than max size (%d)", uncompressedLength, mr.maxBufferSize))
	}
	return int32(uncompressedLength)
}

// @Override
func (mr *MothZstdDecompressor) String() string {
	return "zstd"
}
