package store

import (
	"time"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DECIMAL_COLUMN_SIZE int32 = util.SizeOf(&DecimalColumnReader{})

type DecimalColumnReader struct {
	//继承
	ColumnReader

	kind                block.IDecimalType
	column              *MothColumn
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource // [*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	decimalStreamSource InputStreamSource // [*DecimalInputStream] //@Nullable
	decimalStream       *DecimalInputStream
	scaleStreamSource   InputStreamSource // [LongInputStream] //@Nullable
	scaleStream         LongInputStream
	rowGroupOpen        bool
	nonNullValueTemp    []int64
	memoryContext       memory.LocalMemoryContext
}

func NewDecimalColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *DecimalColumnReader {
	dr := new(DecimalColumnReader)

	// 初始化变量
	dr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]
	dr.decimalStreamSource = MissingStreamSource() // [*DecimalInputStream]
	dr.scaleStreamSource = MissingStreamSource()   //[LongInputStream]
	dr.nonNullValueTemp = make([]int64, 0)

	dr.kind = kind.(block.IDecimalType)
	dr.column = column
	dr.memoryContext = memoryContext
	return dr
}

// @Override
func (dr *DecimalColumnReader) PrepareNextRead(batchSize int32) {
	dr.readOffset += dr.nextBatchSize
	dr.nextBatchSize = batchSize
}

// @Override
func (dr *DecimalColumnReader) ReadBlock() block.Block {
	if !dr.rowGroupOpen {
		dr.openRowGroup()
	}
	dr.seekToOffset()
	var b block.Block
	if dr.decimalStream == nil && dr.scaleStream == nil {
		if dr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		dr.presentStream.Skip(int64(dr.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(dr.kind, nil, dr.nextBatchSize)
	} else if dr.presentStream == nil {
		dr.checkDataStreamsArePresent()
		b = dr.readNonNullBlock()
	} else {
		dr.checkDataStreamsArePresent()
		isNull := make([]bool, dr.nextBatchSize)
		nullCount := dr.presentStream.GetUnsetBits(dr.nextBatchSize, isNull)
		if nullCount == 0 {
			b = dr.readNonNullBlock()
		} else if nullCount != dr.nextBatchSize {
			b = dr.readNullBlock(isNull, dr.nextBatchSize-nullCount)
		} else {
			b = block.CreateRunLengthEncodedBlock(block.DOUBLE, nil, dr.nextBatchSize)
		}
	}
	dr.readOffset = 0
	dr.nextBatchSize = 0
	return b
}

func (dr *DecimalColumnReader) checkDataStreamsArePresent() {
	if dr.decimalStream == nil {
		panic("Value is not null but decimal stream is missing")
	}
	if dr.scaleStream == nil {
		panic("Value is not null but scale stream is missing")
	}
}

func (dr *DecimalColumnReader) readNonNullBlock() block.Block {
	var b block.Block
	if dr.kind.IsShort() {
		b = dr.readShortNotNullBlock()
	} else {
		b = dr.readLongNotNullBlock()
	}
	return b
}

func (dr *DecimalColumnReader) readShortNotNullBlock() block.Block {
	data := make([]int64, dr.nextBatchSize)
	dr.decimalStream.NextShortDecimal(data, dr.nextBatchSize)
	for i := util.INT32_ZERO; i < dr.nextBatchSize; i++ {
		sourceScale := dr.scaleStream.Next()
		if int32(sourceScale) != dr.kind.GetScale() {
			data[i] = block.Rescale(data[i], int32(sourceScale), dr.kind.GetScale())
		}
	}
	return block.NewLongArrayBlock(dr.nextBatchSize, optional.Empty[[]bool](), data)
}

func (dr *DecimalColumnReader) readLongNotNullBlock() block.Block {
	data := make([]int64, dr.nextBatchSize*2)
	dr.decimalStream.NextLongDecimal(data, dr.nextBatchSize)
	for offset := util.INT32_ZERO; offset < util.Lens(data); offset += 2 {
		sourceScale := int32(dr.scaleStream.Next())
		if sourceScale != dr.kind.GetScale() {
			block.Int128Rescale(data[offset], data[offset+1], dr.kind.GetScale()-sourceScale, data, offset)
		}
	}
	return block.NewInt128ArrayBlock(dr.nextBatchSize, optional.Empty[[]bool](), data)
}

func (dr *DecimalColumnReader) readNullBlock(isNull []bool, nonNullCount int32) block.Block {
	var b block.Block
	if dr.kind.IsShort() {
		b = dr.readShortNullBlock(isNull, nonNullCount)
	} else {
		b = dr.readLongNullBlock(isNull, nonNullCount)
	}
	return b
}

func (dr *DecimalColumnReader) readShortNullBlock(isNull []bool, nonNullCount int32) block.Block {

	minNonNullValueSize := MinNonNullValueSize(nonNullCount)
	if util.Lens(dr.nonNullValueTemp) < minNonNullValueSize {
		dr.nonNullValueTemp = make([]int64, minNonNullValueSize)
		dr.memoryContext.SetBytes(util.SizeOfInt64(dr.nonNullValueTemp))
	}
	dr.decimalStream.NextShortDecimal(dr.nonNullValueTemp, nonNullCount)
	for i := util.INT32_ZERO; i < nonNullCount; i++ {
		sourceScale := int32(dr.scaleStream.Next())
		if sourceScale != dr.kind.GetScale() {
			dr.nonNullValueTemp[i] = block.Rescale(dr.nonNullValueTemp[i], sourceScale, dr.kind.GetScale())
		}
	}
	result := UnpackLongNulls(dr.nonNullValueTemp, isNull)
	return block.NewLongArrayBlock(dr.nextBatchSize, optional.Of(isNull), result)
}

func (dr *DecimalColumnReader) readLongNullBlock(isNull []bool, nonNullCount int32) block.Block {
	minTempSize := MinNonNullValueSize(nonNullCount) * 2
	if util.Lens(dr.nonNullValueTemp) < minTempSize {
		dr.nonNullValueTemp = make([]int64, minTempSize)
		dr.memoryContext.SetBytes(util.SizeOfInt64(dr.nonNullValueTemp))
	}
	dr.decimalStream.NextLongDecimal(dr.nonNullValueTemp, nonNullCount)
	for offset := util.INT32_ZERO; offset < nonNullCount*2; offset += 2 {
		sourceScale := int32(dr.scaleStream.Next())
		if sourceScale != dr.kind.GetScale() {
			block.Int128Rescale(dr.nonNullValueTemp[offset], dr.nonNullValueTemp[offset+1], dr.kind.GetScale()-sourceScale, dr.nonNullValueTemp, offset)
		}
	}
	result := UnpackInt128Nulls(dr.nonNullValueTemp, isNull)
	return block.NewInt128ArrayBlock(dr.nextBatchSize, optional.Of(isNull), result)
}

func (dr *DecimalColumnReader) openRowGroup() {
	pe := dr.presentStreamSource.OpenStream()
	if pe != nil {
		dr.presentStream = pe.(*BooleanInputStream)
	} else {
		dr.presentStream = nil
	}
	de := dr.decimalStreamSource.OpenStream()
	if de != nil {
		dr.decimalStream = de.(*DecimalInputStream)
	} else {
		dr.decimalStream = nil
	}

	se := dr.scaleStreamSource.OpenStream()
	if se != nil {
		dr.scaleStream = se.(LongInputStream)
	} else {
		dr.scaleStream = nil
	}

	dr.rowGroupOpen = true
}

func (dr *DecimalColumnReader) seekToOffset() {
	if dr.readOffset > 0 {
		if dr.presentStream != nil {
			dr.readOffset = dr.presentStream.CountBitsSet(dr.readOffset)
		}
		if dr.readOffset > 0 {
			dr.checkDataStreamsArePresent()
			dr.decimalStream.Skip(int64(dr.readOffset))
			dr.scaleStream.Skip(int64(dr.readOffset))
		}
	}
}

// @Override
func (dr *DecimalColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	dr.presentStreamSource = MissingStreamSource() //[*BooleanInputStream]
	dr.decimalStreamSource = MissingStreamSource() //[*DecimalInputStream]
	dr.scaleStreamSource = MissingStreamSource()   //[LongInputStream]
	dr.readOffset = 0
	dr.nextBatchSize = 0
	dr.presentStream = nil
	dr.decimalStream = nil
	dr.scaleStream = nil
	dr.rowGroupOpen = false
}

// @Override
func (dr *DecimalColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	dr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, dr.column, metadata.PRESENT)
	dr.decimalStreamSource = GetInputStreamSource[*DecimalInputStream](dataStreamSources, dr.column, metadata.DATA)
	dr.scaleStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, dr.column, metadata.SECONDARY)
	dr.readOffset = 0
	dr.nextBatchSize = 0
	dr.presentStream = nil
	dr.decimalStream = nil
	dr.scaleStream = nil
	dr.rowGroupOpen = false
}

// @Override
func (dr *DecimalColumnReader) String() string {
	return util.NewSB().AppendString(dr.column.String()).String()
}

// @Override
func (dr *DecimalColumnReader) Close() {
	dr.memoryContext.Close()
}

// @Override
func (dr *DecimalColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(DECIMAL_COLUMN_SIZE)
}
