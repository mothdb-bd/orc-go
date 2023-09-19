package store

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var EXPECTED_COMPRESSION_RATIO int32 = 5

type MothZlibDecompressor struct {
	// 继承
	MothDecompressor

	mothDataSourceId *common.MothDataSourceId
	maxBufferSize    int32
}

func NewMothZlibDecompressor(mothDataSourceId *common.MothDataSourceId, maxBufferSize int32) *MothZlibDecompressor {
	mr := new(MothZlibDecompressor)
	mr.mothDataSourceId = mothDataSourceId
	mr.maxBufferSize = maxBufferSize
	return mr
}

// @Override
func (mr *MothZlibDecompressor) Decompress(input []byte, offset int32, length int32, output OutputBuffer) int32 {
	b := bytes.NewBuffer(input[offset : offset+length])
	inflater, err := zlib.NewReader(b)

	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("zlib reader error: %s", err.Error()))
	}

	buffer := output.Initialize(maths.MinInt32(length*EXPECTED_COMPRESSION_RATIO, mr.maxBufferSize))

	var finishError error
	var size int
	uncompressedLength := 0
	for {

		size, finishError = inflater.Read(buffer)
		if finishError != nil && finishError != io.EOF {
			panic(fmt.Sprintf("uncompressed error: %s", finishError.Error()))
		}

		uncompressedLength += size
		bLen := util.Lens(buffer)

		if finishError == io.EOF || bLen >= mr.maxBufferSize || int32(size) <= bLen {
			break
		}

		buffer = output.Grow(maths.MinInt32(bLen, mr.maxBufferSize))
		nLen := util.Lens(buffer)
		if nLen <= bLen {
			panic(fmt.Sprintf("Buffer failed to grow. Old size %d, current size %d", bLen, nLen))
		}
	}
	return int32(uncompressedLength)
}

// @Override
func (mr *MothZlibDecompressor) String() string {
	return "zlib"
}
