package store

import (
	"fmt"

	"github.com/golang/snappy"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MothSnappyDecompressor struct {
	// 继承
	MothDecompressor

	mothDataSourceId *common.MothDataSourceId
	maxBufferSize    int32
}

func NewMothSnappyDecompressor(mothDataSourceId *common.MothDataSourceId, maxBufferSize int32) *MothSnappyDecompressor {
	mr := new(MothSnappyDecompressor)
	mr.mothDataSourceId = mothDataSourceId
	mr.maxBufferSize = maxBufferSize
	return mr
}

// @Override
func (mr *MothSnappyDecompressor) Decompress(input []byte, offset int32, length int32, output OutputBuffer) int32 {

	decodeBuf := input[offset : offset+length]
	totalBuf := input[offset:]

	// uncompressedLength := SnappyDecompressor.getUncompressedLength(input, offset)
	uncompressedLength, _ := snappy.DecodedLen(totalBuf)

	if int32(uncompressedLength) > mr.maxBufferSize {
		panic(fmt.Sprintf("Snappy requires buffer (%d) larger than max size (%d)", uncompressedLength, mr.maxBufferSize))
	}
	buffer := output.Initialize(int32(uncompressedLength) + util.INT64_BYTES)

	snappy.Decode(buffer, decodeBuf)

	if len(decodeBuf) == len(totalBuf) {
		return 0
	} else {
		dLen, _ := snappy.DecodedLen(decodeBuf)
		return int32(uncompressedLength - dLen)
	}
}

// @Override
func (mr *MothSnappyDecompressor) String() string {
	return "snappy"
}
