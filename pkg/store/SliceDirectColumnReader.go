package store

import (
	"fmt"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	SLICE_DIRECT_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&SliceDirectColumnReader{})
	ONE_GIGABYTE                             int32 = util.Int32Exact(int64(util.GB.Bytes()))
)

type SliceDirectColumnReader struct {
	// 继承
	ColumnReader

	maxCodePointCount   int32
	isCharType          bool
	column              *MothColumn
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	lengthStreamSource  InputStreamSource // [LongInputStream] //@Nullable
	lengthStream        LongInputStream
	dataByteSource      InputStreamSource // [*ByteArrayInputStream] //@Nullable
	dataStream          *ByteArrayInputStream
	rowGroupOpen        bool
}

func NewSliceDirectColumnReader(column *MothColumn, maxCodePointCount int32, isCharType bool) *SliceDirectColumnReader {
	sr := new(SliceDirectColumnReader)

	sr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]()
	sr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]()
	sr.dataByteSource = MissingStreamSource()      //[*ByteArrayInputStream]()

	sr.maxCodePointCount = maxCodePointCount
	sr.isCharType = isCharType
	sr.column = column
	return sr
}

// @Override
func (sr *SliceDirectColumnReader) PrepareNextRead(batchSize int32) {
	sr.readOffset += sr.nextBatchSize
	sr.nextBatchSize = batchSize
}

// @Override
func (sr *SliceDirectColumnReader) ReadBlock() block.Block {
	if !sr.rowGroupOpen {
		sr.openRowGroup()
	}
	if sr.readOffset > 0 {
		if sr.presentStream != nil {
			sr.readOffset = sr.presentStream.CountBitsSet(sr.readOffset)
		}
		if sr.readOffset > 0 {
			if sr.lengthStream == nil {
				panic("Value is not null but length stream is missing")
			}
			dataSkipSize := sr.lengthStream.Sum(sr.readOffset)
			if dataSkipSize > 0 {
				if sr.dataStream == nil {
					panic("Value is not null but data stream is missing")
				}
				sr.dataStream.Skip(dataSkipSize)
			}
		}
	}
	if sr.lengthStream == nil {
		if sr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		sr.presentStream.Skip(int64(sr.nextBatchSize))
		nullValueBlock := sr.readAllNullsBlock()
		sr.readOffset = 0
		sr.nextBatchSize = 0
		return nullValueBlock
	}
	var isNullVector []bool = nil
	offsetVector := make([]int32, sr.nextBatchSize+1)
	if sr.presentStream == nil {
		sr.lengthStream.Next3(offsetVector, sr.nextBatchSize)
	} else {
		isNullVector = make([]bool, sr.nextBatchSize)
		nullCount := sr.presentStream.GetUnsetBits(sr.nextBatchSize, isNullVector)
		if nullCount == sr.nextBatchSize {
			nullValueBlock := sr.readAllNullsBlock()
			sr.readOffset = 0
			sr.nextBatchSize = 0
			return nullValueBlock
		}
		if sr.lengthStream == nil {
			panic("Value is not null but length stream is missing")
		}
		if nullCount == 0 {
			isNullVector = nil
			sr.lengthStream.Next3(offsetVector, sr.nextBatchSize)
		} else {
			sr.lengthStream.Next3(offsetVector, sr.nextBatchSize-nullCount)
			UnpackLengthNulls(offsetVector, isNullVector, sr.nextBatchSize-nullCount)
		}
	}
	totalLength := util.INT64_ZERO
	for i := util.INT32_ZERO; i < sr.nextBatchSize; i++ {
		totalLength += int64(offsetVector[i])
	}
	currentBatchSize := sr.nextBatchSize
	sr.readOffset = 0
	sr.nextBatchSize = 0
	if totalLength == 0 {
		return block.NewVariableWidthBlock(currentBatchSize, slice.EMPTY_SLICE, offsetVector, optional.OfNullable(isNullVector))
	}
	if totalLength > int64(ONE_GIGABYTE) {
		panic(fmt.Sprintf("Values in column \"%s\" are too large to process for Moth. %d column values are larger than 1GB [%s]", sr.column.GetPath(), sr.nextBatchSize, sr.column.GetMothDataSourceId()))
	}
	if sr.dataStream == nil {
		panic("Value is not null but data stream is missing")
	}
	data := make([]byte, util.Int32Exact(totalLength))
	var s *slice.Slice = nil
	if sr.maxCodePointCount < 0 {
		sr.dataStream.Next2(data, 0, len(data))
		s = slice.NewWithBuf(data)
		ConvertLengthVectorToOffsetVector(offsetVector)
	} else {
		currentLength := offsetVector[0]
		offsetVector[0] = 0
		for i := int32(1); i <= currentBatchSize; i++ {
			nextLength := offsetVector[i]
			if isNullVector != nil && isNullVector[i-1] {
				util.CheckState2(currentLength == 0, "Corruption in slice direct stream: length is non-zero for null entry")
				offsetVector[i] = offsetVector[i-1]
				currentLength = nextLength
				continue
			}
			offset := offsetVector[i-1]

			sr.dataStream.Next2(data, int(offset), int(offset+currentLength))
			s = slice.NewWithBuf(data)

			truncatedLength := ComputeTruncatedLength(s, offset, currentLength, sr.maxCodePointCount, sr.isCharType)
			util.Verify(truncatedLength >= 0)
			offsetVector[i] = offset + truncatedLength
			currentLength = nextLength
		}
	}
	return block.NewVariableWidthBlock(currentBatchSize, s, offsetVector, optional.OfNullable(isNullVector))
}

func (sr *SliceDirectColumnReader) readAllNullsBlock() *block.RunLengthEncodedBlock {
	return block.NewRunLengthEncodedBlock(block.NewVariableWidthBlock(1, slice.EMPTY_SLICE, make([]int32, 2), optional.Of([]bool{true})), sr.nextBatchSize)
}

func (sr *SliceDirectColumnReader) openRowGroup() {
	pe := sr.presentStreamSource.OpenStream()
	if pe != nil {
		sr.presentStream = pe.(*BooleanInputStream)
	} else {
		sr.presentStream = nil
	}

	le := sr.lengthStreamSource.OpenStream()
	if le != nil {
		sr.lengthStream = le.(LongInputStream)
	} else {
		sr.lengthStream = nil
	}

	de := sr.dataByteSource.OpenStream()
	if de != nil {
		sr.dataStream = de.(*ByteArrayInputStream)
	} else {
		sr.dataStream = nil
	}

	sr.rowGroupOpen = true
}

// @Override
func (sr *SliceDirectColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	sr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]()
	sr.lengthStreamSource = MissingStreamSource()  //[LongInputStream]()
	sr.dataByteSource = MissingStreamSource()      // [*ByteArrayInputStream]()
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.lengthStream = nil
	sr.dataStream = nil
	sr.rowGroupOpen = false
}

// @Override
func (sr *SliceDirectColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	sr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, sr.column, metadata.PRESENT)
	sr.lengthStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, sr.column, metadata.LENGTH)
	sr.dataByteSource = GetInputStreamSource[*ByteArrayInputStream](dataStreamSources, sr.column, metadata.DATA)
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.lengthStream = nil
	sr.dataStream = nil
	sr.rowGroupOpen = false
}

// @Override
func (sr *SliceDirectColumnReader) ToString() string {
	return util.NewSB().AppendString(sr.column.String()).String()
}

// @Override
func (sr *SliceDirectColumnReader) Close() {
}

// @Override
func (sr *SliceDirectColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(SLICE_DIRECT_COLUMN_READER_INSTANCE_SIZE)
}
