package store

import (
	"fmt"
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/common"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// public final class MothInputStream  extends InputStream
type MothInputStream struct {
	mothio.InputStream

	chunkLoader    MothChunkLoader
	current        slice.FixedLengthSliceInput
	lastCheckpoint int64
}

func NewMothInputStream(chunkLoader MothChunkLoader) *MothInputStream {
	mm := new(MothInputStream)

	mm.current = slice.EMPTY_SLICE.GetInput()
	mm.chunkLoader = chunkLoader
	return mm
}

func (mm *MothInputStream) GetReader() io.Reader {
	return mm
}

func (mm *MothInputStream) Read(p []byte) (n int, err error) {
	return mm.ReadBS3(p, 0, len(p))
}

// @Override
func (mm *MothInputStream) Close() {
}

// @Override
func (mm *MothInputStream) Available() int32 {
	if mm.current == nil {
		return 0
	}
	return mm.current.Available()
}

// @Override
func (mm *MothInputStream) MarkSupported() bool {
	return false
}

// @Override
func (mm *MothInputStream) ReadBS() (byte, error) {
	if mm.current == nil {
		return 0, io.EOF
	}
	result, err := mm.current.ReadBS()
	if err == nil || err != io.EOF {
		return result, nil
	}
	mm.advance()
	return mm.ReadBS()
}

func (mm *MothInputStream) ReadBS2(b []byte) (n int, err error) {
	size := len(b)
	return mm.ReadBS3(b, 0, size)
}

// @Override
func (mm *MothInputStream) ReadBS3(b []byte, off int, length int) (n int, err error) {
	if mm.current == nil {
		return 0, io.EOF
	}
	if mm.current.Remaining() == 0 {
		mm.advance()
		if mm.current == nil {
			return 0, io.EOF
		}
	}
	return mm.current.ReadBS3(b, off, length)
}

func (mm *MothInputStream) SkipFully(length int64) {
	for length > 0 {
		result := mm.Skip(length)
		if result < 0 {
			panic(fmt.Sprintf("%s Unexpected end of stream", mm.chunkLoader.GetMothDataSourceId()))
		}
		length -= result
	}
}

func (mm *MothInputStream) ReadFully(buffer []byte, offset int, length int) {
	for offset < length {
		result, err := mm.ReadBS3(buffer, offset, length-offset)
		if err == io.EOF {
			panic(fmt.Sprintf("%s Unexpected end of stream", mm.chunkLoader.GetMothDataSourceId()))
		}
		offset += result
	}
}

func (mm *MothInputStream) ReadFully2(buffer *slice.Slice, offset int, length int) {
	for length > 0 {
		if mm.current != nil && mm.current.Remaining() == 0 {
			mm.advance()
		}
		if mm.current == nil {
			panic(fmt.Sprintf("%s Unexpected end of stream", mm.chunkLoader.GetMothDataSourceId()))
		}
		chunkSize := maths.MinInt(length, int(mm.current.Remaining()))
		mm.current.ReadSlice4(buffer, int32(offset), int32(chunkSize))
		length -= chunkSize
		offset += chunkSize
	}
}

func (mm *MothInputStream) GetMothDataSourceId() *common.MothDataSourceId {
	return mm.chunkLoader.GetMothDataSourceId()
}

func (mm *MothInputStream) GetCheckpoint() int64 {
	checkpoint := mm.chunkLoader.GetLastCheckpoint()
	if mm.current != nil && mm.current.Position() > 0 {
		checkpoint = CreateInputStreamCheckpoint2(DecodeCompressedBlockOffset(checkpoint), util.Int32Exact(int64(DecodeDecompressedOffset(checkpoint))+mm.current.Position()))
	}
	return checkpoint
}

func (mm *MothInputStream) SeekToCheckpoint(checkpoint int64) {
	compressedOffset := DecodeCompressedBlockOffset(checkpoint)
	decompressedOffset := DecodeDecompressedOffset(checkpoint)
	currentDecompressedBufferOffset := DecodeDecompressedOffset(mm.lastCheckpoint)
	if mm.current != nil && compressedOffset == DecodeCompressedBlockOffset(mm.lastCheckpoint) && decompressedOffset < currentDecompressedBufferOffset+int32(mm.current.Length()) {
		mm.current.SetPosition(int64(decompressedOffset - currentDecompressedBufferOffset))
		return
	}
	mm.current = slice.EMPTY_SLICE.GetInput()
	mm.chunkLoader.SeekToCheckpoint(checkpoint)
	mm.lastCheckpoint = checkpoint
}

// @Override
func (mm *MothInputStream) Skip(n int64) int64 {
	if mm.current == nil || n <= 0 {
		return -1
	}
	result := mm.current.Skip(n)
	if result != 0 {
		return result
	}
	_, err := mm.ReadBS()
	if err == io.EOF {
		return 0
	}
	return 1 + mm.current.Skip(n-1)
}

func (mm *MothInputStream) advance() {
	if !mm.chunkLoader.HasNextChunk() {
		mm.current = nil
		return
	}
	mm.current = mm.chunkLoader.NextChunk().GetInput()
	mm.lastCheckpoint = mm.chunkLoader.GetLastCheckpoint()
}

// @Override
func (mm *MothInputStream) String() string {
	return util.NewSB().AddString("source", mm.chunkLoader.String()).AddInt64("uncompressedOffset", util.Ternary(mm.current == nil, 0, mm.current.Position())).String()
}
