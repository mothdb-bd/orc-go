package store

import (
	"bytes"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	SLICE_DICTIONARY_COLUMN_READER_INSTANCE_SIZE int32   = util.SizeOf(&SliceDictionaryColumnReader{})
	EMPTY_DICTIONARY_DATA                        []byte  = make([]byte, 0)
	EMPTY_DICTIONARY_OFFSETS                     []int32 = make([]int32, 2)
)

type SliceDictionaryColumnReader struct {
	// 继承
	ColumnReader

	column                       *MothColumn
	maxCodePointCount            int32
	isCharType                   bool
	readOffset                   int32
	nextBatchSize                int32
	presentStreamSource          InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream                *BooleanInputStream
	dictionaryDataStreamSource   InputStreamSource // [*ByteArrayInputStream]
	dictionaryOpen               bool
	dictionarySize               int32
	dictionaryLength             []int32
	dictionaryData               []byte
	dictionaryOffsetVector       []int32
	dictionaryBlock              *block.VariableWidthBlock
	currentDictionaryData        []byte
	dictionaryLengthStreamSource InputStreamSource // [LongInputStream]
	dataStreamSource             InputStreamSource // [LongInputStream] //@Nullable
	dataStream                   LongInputStream
	rowGroupOpen                 bool
	nonNullValueTemp             []int32
	nonNullPositionList          []int32
	memoryContext                memory.LocalMemoryContext
}

func NewSliceDictionaryColumnReader(column *MothColumn, memoryContext memory.LocalMemoryContext, maxCodePointCount int32, isCharType bool) *SliceDictionaryColumnReader {
	sr := new(SliceDictionaryColumnReader)

	sr.presentStreamSource = MissingStreamSource()        //[*BooleanInputStream]
	sr.dictionaryDataStreamSource = MissingStreamSource() // [*ByteArrayInputStream]
	sr.dictionaryLength = make([]int32, 0)
	sr.dictionaryData = EMPTY_DICTIONARY_DATA
	sr.dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS

	s := slice.NewBaseBuf(EMPTY_DICTIONARY_DATA)
	sr.dictionaryBlock = block.NewVariableWidthBlock(1, s, EMPTY_DICTIONARY_OFFSETS, optional.Of([]bool{true}))
	sr.currentDictionaryData = EMPTY_DICTIONARY_DATA
	sr.dictionaryLengthStreamSource = MissingStreamSource() //[LongInputStream]
	sr.dataStreamSource = MissingStreamSource()             //[LongInputStream]
	sr.nonNullValueTemp = make([]int32, 0)
	sr.nonNullPositionList = make([]int32, 0)

	sr.maxCodePointCount = maxCodePointCount
	sr.isCharType = isCharType
	sr.column = column
	sr.memoryContext = memoryContext
	return sr
}

// @Override
func (sr *SliceDictionaryColumnReader) PrepareNextRead(batchSize int32) {
	sr.readOffset += sr.nextBatchSize
	sr.nextBatchSize = batchSize
}

// @Override
func (sr *SliceDictionaryColumnReader) ReadBlock() block.Block {
	if !sr.rowGroupOpen {
		sr.openRowGroup()
	}
	if sr.readOffset > 0 {
		if sr.presentStream != nil {
			sr.readOffset = sr.presentStream.CountBitsSet(sr.readOffset)
		}
		if sr.readOffset > 0 {
			if sr.dataStream == nil {
				panic("Value is not null but data stream is missing")
			}
			sr.dataStream.Skip(int64(sr.readOffset))
		}
	}
	var block block.Block
	if sr.dataStream == nil {
		if sr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		sr.presentStream.Skip(int64(sr.nextBatchSize))
		block = sr.readAllNullsBlock()
	} else if sr.presentStream == nil {
		block = sr.readNonNullBlock()
	} else {
		isNull := make([]bool, sr.nextBatchSize)
		nullCount := sr.presentStream.GetUnsetBits(sr.nextBatchSize, isNull)
		if nullCount == 0 {
			block = sr.readNonNullBlock()
		} else if nullCount != sr.nextBatchSize {
			block = sr.readNullBlock(isNull, sr.nextBatchSize-nullCount)
		} else {
			block = sr.readAllNullsBlock()
		}
	}
	sr.readOffset = 0
	sr.nextBatchSize = 0
	return block
}

func (sr *SliceDictionaryColumnReader) readAllNullsBlock() *block.RunLengthEncodedBlock {
	return block.NewRunLengthEncodedBlock(block.NewVariableWidthBlock(1, slice.EMPTY_SLICE, make([]int32, 2), optional.Of([]bool{true})), sr.nextBatchSize)
}

func (sr *SliceDictionaryColumnReader) readNonNullBlock() block.Block {

	values := make([]int32, sr.nextBatchSize)
	sr.dataStream.Next3(values, sr.nextBatchSize)
	return block.NewDictionaryBlock2(sr.nextBatchSize, sr.dictionaryBlock, values)
}

func (sr *SliceDictionaryColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {

	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(sr.nonNullValueTemp) < minNonNullValueSize {
		sr.nonNullValueTemp = make([]int32, minNonNullValueSize)
		sr.nonNullPositionList = make([]int32, minNonNullValueSize)
		sr.memoryContext.SetBytes(sr.GetRetainedSizeInBytes())
	}
	sr.dataStream.Next3(sr.nonNullValueTemp, nonNullCount)
	nonNullPosition := util.INT32_ZERO
	for i := util.INT32_ZERO; i < util.Lens(isNull); i++ {
		sr.nonNullPositionList[nonNullPosition] = i
		if !isNull[i] {
			nonNullPosition++
		}
	}
	result := make([]int32, util.Lens(isNull))
	util.FillInt32s(result, sr.dictionarySize)
	for i := util.INT32_ZERO; i < nonNullPosition; i++ {
		result[sr.nonNullPositionList[i]] = sr.nonNullValueTemp[i]
	}
	return block.NewDictionaryBlock2(sr.nextBatchSize, sr.dictionaryBlock, result)
}

func (sr *SliceDictionaryColumnReader) setDictionaryBlockData(dictionaryData []byte, dictionaryOffsets []int32, positionCount int32) {
	if !bytes.Equal(sr.currentDictionaryData, dictionaryData) {
		isNullVector := make([]bool, positionCount)
		isNullVector[positionCount-1] = true
		dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount-1]
		s := slice.NewWithBuf(dictionaryData)
		sr.dictionaryBlock = block.NewVariableWidthBlock(positionCount, s, dictionaryOffsets, optional.Of(isNullVector))
		sr.currentDictionaryData = dictionaryData
		sr.memoryContext.SetBytes(sr.GetRetainedSizeInBytes())
	}
}

func (sr *SliceDictionaryColumnReader) openRowGroup() {
	if !sr.dictionaryOpen {
		if sr.dictionarySize > 0 {
			if util.Lens(sr.dictionaryLength) < sr.dictionarySize {
				sr.dictionaryLength = make([]int32, sr.dictionarySize)
			}
			le := sr.dictionaryLengthStreamSource.OpenStream()
			var lengthStream LongInputStream = nil
			if le != nil {
				lengthStream = le.(LongInputStream)
			}

			if lengthStream == nil {
				panic("Dictionary is not empty but dictionary length stream is missing")
			}
			lengthStream.Next3(sr.dictionaryLength, sr.dictionarySize)
			dataLength := util.INT64_ZERO
			for i := util.INT32_ZERO; i < sr.dictionarySize; i++ {
				dataLength += int64(sr.dictionaryLength[i])
			}
			i, _ := util.ToInt32Exact(dataLength)
			sr.dictionaryData = make([]byte, i)
			sr.dictionaryOffsetVector = make([]int32, i+2)
			de := sr.dictionaryDataStreamSource.OpenStream()
			var dictionaryDataStream *ByteArrayInputStream = nil
			if de != nil {
				dictionaryDataStream = de.(*ByteArrayInputStream)
			}
			readDictionary(dictionaryDataStream, sr.dictionarySize, sr.dictionaryLength, 0, sr.dictionaryData, sr.dictionaryOffsetVector, sr.maxCodePointCount, sr.isCharType)
		} else {
			sr.dictionaryData = EMPTY_DICTIONARY_DATA
			sr.dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS
		}
	}
	sr.dictionaryOpen = true
	sr.setDictionaryBlockData(sr.dictionaryData, sr.dictionaryOffsetVector, sr.dictionarySize+1)
	pe := sr.presentStreamSource.OpenStream()
	if pe != nil {
		sr.presentStream = pe.(*BooleanInputStream)
	} else {
		sr.presentStream = nil
	}

	de := sr.dataStreamSource.OpenStream()
	if de != nil {
		sr.dataStream = de.(LongInputStream)
	} else {
		sr.dataStream = nil
	}

	sr.rowGroupOpen = true
}

func readDictionary(dictionaryDataStream *ByteArrayInputStream, dictionarySize int32, dictionaryLengthVector []int32, offsetVectorOffset int32, data []byte, offsetVector []int32, maxCodePointCount int32, isCharType bool) {
	if offsetVectorOffset == 0 {
		offsetVector[0] = 0
	}
	for i := util.INT32_ZERO; i < dictionarySize; i++ {
		offsetIndex := offsetVectorOffset + i
		offset := offsetVector[offsetIndex]
		length := dictionaryLengthVector[i]
		var truncatedLength int32
		if length > 0 {
			dictionaryDataStream.Next2(data, int(offset), int(offset+length))
			s := slice.NewWithBuf(data)
			truncatedLength = ComputeTruncatedLength(s, offset, length, maxCodePointCount, isCharType)
			util.Verify(truncatedLength >= 0)
		} else {
			truncatedLength = 0
		}
		offsetVector[offsetIndex+1] = offsetVector[offsetIndex] + truncatedLength
	}
}

// @Override
func (sr *SliceDictionaryColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	sr.dictionaryDataStreamSource = GetInputStreamSource[*ByteArrayInputStream](dictionaryStreamSources, sr.column, metadata.DICTIONARY_DATA)
	sr.dictionaryLengthStreamSource = GetInputStreamSource[LongInputStream](dictionaryStreamSources, sr.column, metadata.LENGTH)
	sr.dictionarySize = int32(encoding.Get(sr.column.GetColumnId()).GetDictionarySize())
	sr.dictionaryOpen = false
	sr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	sr.dataStreamSource = MissingStreamSource()    //[LongInputStream]
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.dataStream = nil
	sr.rowGroupOpen = false
}

// @Override
func (sr *SliceDictionaryColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	sr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, sr.column, metadata.PRESENT)
	sr.dataStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, sr.column, metadata.DATA)
	sr.readOffset = 0
	sr.nextBatchSize = 0
	sr.presentStream = nil
	sr.dataStream = nil
	sr.rowGroupOpen = false
}

// @Override
func (sr *SliceDictionaryColumnReader) String() string {
	return util.NewSB().AppendString(sr.column.String()).String()
}

// @Override
func (sr *SliceDictionaryColumnReader) Close() {
	sr.memoryContext.Close()
}

// @Override
func (sr *SliceDictionaryColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(SLICE_DICTIONARY_COLUMN_READER_INSTANCE_SIZE + util.SizeOf(sr.nonNullValueTemp) + util.SizeOf(sr.nonNullPositionList) + util.SizeOf(sr.dictionaryData) + util.SizeOf(sr.dictionaryLength) + util.SizeOf(sr.dictionaryOffsetVector) + (util.Ternary(basic.ObjectEqual(sr.currentDictionaryData, sr.dictionaryData), 0, util.SizeOf(sr.currentDictionaryData))))
}
