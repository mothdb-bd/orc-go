package store

import "github.com/mothdb-bd/orc-go/pkg/util"

func CreateInputStreamCheckpoint(compressed bool, positionsList *ColumnPositionsList) int64 {
	if compressed {
		return CreateInputStreamCheckpoint2(positionsList.NextPosition(), positionsList.NextPosition())
	} else {
		return CreateInputStreamCheckpoint2(0, positionsList.NextPosition())
	}
}

func CreateInputStreamCheckpoint2(compressedBlockOffset int32, decompressedOffset int32) int64 {
	return (int64(compressedBlockOffset) << 32) | int64(decompressedOffset)
}

func DecodeCompressedBlockOffset(inputStreamCheckpoint int64) int32 {

	return int32(inputStreamCheckpoint >> 32)
}

func DecodeDecompressedOffset(inputStreamCheckpoint int64) int32 {
	// low order bits contain the decompressed offset, so a simple cast here will suffice
	return int32(inputStreamCheckpoint)
}

func CreateInputStreamPositionList(compressed bool, inputStreamCheckpoint int64) *util.ArrayList[int32] {
	l := util.NewArrayList[int32]()
	if compressed {
		l.Add(DecodeCompressedBlockOffset(inputStreamCheckpoint), DecodeDecompressedOffset(inputStreamCheckpoint))
		// return ImmutableList.of(DecodeCompressedBlockOffset(inputStreamCheckpoint), DecodeDecompressedOffset(inputStreamCheckpoint))
	} else {
		l.Add(DecodeDecompressedOffset(inputStreamCheckpoint))
		// return ImmutableList.of(DecodeDecompressedOffset(inputStreamCheckpoint))
	}
	return l
}
