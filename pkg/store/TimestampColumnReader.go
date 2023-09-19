package store

import (
	"fmt"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type TimestampKind int8

const (
	TIMESTAMP_MILLIS TimestampKind = iota
	TIMESTAMP_MICROS
	TIMESTAMP_NANOS
	INSTANT_MILLIS
	INSTANT_MICROS
	INSTANT_NANOS
	UNKONW
)

var (
	// *LocalDateTime = LocalDateTime.of(2015, 1, 1, 0, 0, 0, 0)
	MOTH_EPOCH = time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local)
	// .toEpochSecond(ZoneOffset.UTC)
	BASE_INSTANT_IN_SECONDS               int64 = MOTH_EPOCH.UTC().Unix()
	TIMESTAMP_COLUMN_READER_INSTANCE_SIZE int32 = util.SizeOf(&TimestampColumnReader{})

	POWERS_OF_TEN []int32 = []int32{
		1,
		10,
		100,
		1_000,
		10_000,
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
	}
)

type TimestampColumnReader struct {
	// 继承
	ColumnReader

	kind                   block.Type
	column                 *MothColumn
	timestampKind          TimestampKind
	baseTimestampInSeconds int64

	// fileDateTimeZone       *DateTimeZone
	fileDateTimeZone    *time.Location
	readOffset          int32
	nextBatchSize       int32
	presentStreamSource InputStreamSource //[*BooleanInputStream] //@Nullable
	presentStream       *BooleanInputStream
	secondsStreamSource InputStreamSource //[LongInputStream] //@Nullable
	secondsStream       LongInputStream
	nanosStreamSource   InputStreamSource //[LongInputStream] //@Nullable
	nanosStream         LongInputStream
	rowGroupOpen        bool
	memoryContext       memory.LocalMemoryContext
}

func NewTimestampColumnReader(kind block.Type, column *MothColumn, memoryContext memory.LocalMemoryContext) *TimestampColumnReader {
	tr := new(TimestampColumnReader)

	tr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]()
	tr.secondsStreamSource = MissingStreamSource() // [LongInputStream]()
	tr.nanosStreamSource = MissingStreamSource()   // [LongInputStream]()

	tr.kind = kind
	tr.column = column
	tr.timestampKind = getTimestampKind(kind, column)
	tr.memoryContext = memoryContext
	return tr
}

func getTimestampKind(kind block.Type, column *MothColumn) TimestampKind {
	if kind.Equals(block.TIMESTAMP_MILLIS) && (column.GetColumnType() == metadata.TIMESTAMP) {
		return TIMESTAMP_MILLIS
	}
	if kind.Equals(block.TIMESTAMP_MICROS) && (column.GetColumnType() == metadata.TIMESTAMP) {
		return TIMESTAMP_MICROS
	}
	if kind.Equals(block.TIMESTAMP_NANOS) && (column.GetColumnType() == metadata.TIMESTAMP) {
		return TIMESTAMP_NANOS
	}
	if kind.Equals(block.TIMESTAMP_TZ_MILLIS) && (column.GetColumnType() == metadata.TIMESTAMP_INSTANT) {
		return INSTANT_MILLIS
	}
	if kind.Equals(block.TIMESTAMP_TZ_MICROS) && (column.GetColumnType() == metadata.TIMESTAMP_INSTANT) {
		return INSTANT_MICROS
	}
	if kind.Equals(block.TIMESTAMP_TZ_NANOS) && (column.GetColumnType() == metadata.TIMESTAMP_INSTANT) {
		return INSTANT_NANOS
	}
	InvalidStreamType(column, kind)
	return UNKONW
}

// @Override
func (tr *TimestampColumnReader) PrepareNextRead(batchSize int32) {
	tr.readOffset += tr.nextBatchSize
	tr.nextBatchSize = batchSize
}

// @Override
func (tr *TimestampColumnReader) ReadBlock() block.Block {
	if !tr.rowGroupOpen {
		tr.openRowGroup()
	}
	if tr.readOffset > 0 {
		if tr.presentStream != nil {
			tr.readOffset = tr.presentStream.CountBitsSet(tr.readOffset)
		}
		if tr.readOffset > 0 {
			tr.verifyStreamsPresent()
			tr.secondsStream.Skip(int64(tr.readOffset))
			tr.nanosStream.Skip(int64(tr.readOffset))
		}
	}
	var b block.Block
	if tr.secondsStream == nil && tr.nanosStream == nil {
		if tr.presentStream == nil {
			panic("Value is null but present stream is missing")
		}
		tr.presentStream.Skip(int64(tr.nextBatchSize))
		b = block.CreateRunLengthEncodedBlock(tr.kind, nil, tr.nextBatchSize)
	} else if tr.presentStream == nil {
		b = tr.readNonNullBlock()
	} else {
		isNull := make([]bool, tr.nextBatchSize)
		nullCount := tr.presentStream.GetUnsetBits(tr.nextBatchSize, isNull)
		if nullCount == 0 {
			b = tr.readNonNullBlock()
		} else if nullCount != tr.nextBatchSize {
			b = tr.readNullBlock(isNull)
		} else {
			b = block.CreateRunLengthEncodedBlock(tr.kind, nil, tr.nextBatchSize)
		}
	}
	tr.readOffset = 0
	tr.nextBatchSize = 0
	return b
}

func (tr *TimestampColumnReader) verifyStreamsPresent() {
	if tr.secondsStream == nil {
		panic("Value is not null but seconds stream is missing")
	}
	if tr.nanosStream == nil {
		panic("Value is not null but nanos stream is missing")
	}
}

func (tr *TimestampColumnReader) readNonNullBlock() block.Block {
	tr.verifyStreamsPresent()
	switch tr.timestampKind {
	case TIMESTAMP_MILLIS:
		return tr.readNonNullTimestampMillis()
	case TIMESTAMP_MICROS:
		return tr.readNonNullTimestampMicros()
	case TIMESTAMP_NANOS:
		return tr.readNonNullTimestampNanos()
	case INSTANT_MILLIS:
		return tr.readNonNullInstantMillis()
	case INSTANT_MICROS:
		return tr.readNonNullInstantMicros()
	case INSTANT_NANOS:
		return tr.readNonNullInstantNanos()
	}
	panic(fmt.Sprintf("Unhandled timestmap kind: %d", tr.timestampKind))
}

func (tr *TimestampColumnReader) readNullBlock(isNull []bool) block.Block {
	tr.verifyStreamsPresent()
	switch tr.timestampKind {
	case TIMESTAMP_MILLIS:
		return tr.readNullTimestampMillis(isNull)
	case TIMESTAMP_MICROS:
		return tr.readNullTimestampMicros(isNull)
	case TIMESTAMP_NANOS:
		return tr.readNullTimestampNanos(isNull)
	case INSTANT_MILLIS:
		return tr.readNullInstantMillis(isNull)
	case INSTANT_MICROS:
		return tr.readNullInstantMicros(isNull)
	case INSTANT_NANOS:
		return tr.readNullInstantNanos(isNull)
	}
	panic(fmt.Sprintf("Unhandled timestmap kind: %d", tr.timestampKind))
}

func (tr *TimestampColumnReader) openRowGroup() {
	pe := tr.presentStreamSource.OpenStream()
	if pe != nil {
		tr.presentStream = pe.(*BooleanInputStream)
	} else {
		tr.presentStream = nil
	}

	se := tr.secondsStreamSource.OpenStream()
	if se != nil {
		tr.secondsStream = se.(LongInputStream)
	} else {
		tr.secondsStream = nil
	}

	ne := tr.nanosStreamSource.OpenStream()
	if ne != nil {
		tr.nanosStream = ne.(LongInputStream)
	} else {
		tr.nanosStream = nil
	}

	tr.rowGroupOpen = true
}

// @Override
func (tr *TimestampColumnReader) StartStripe(fileTimeZone *time.Location, dictionaryStreamSources *InputStreamSources, encoding *metadata.ColumnMetadata[*metadata.ColumnEncoding]) {
	// ZonedDateTime.ofLocal(MOTH_EPOCH, fileTimeZone, nil).toEpochSecond()
	tr.baseTimestampInSeconds = MOTH_EPOCH.In(fileTimeZone).Unix()
	tr.fileDateTimeZone = fileTimeZone
	tr.presentStreamSource = MissingStreamSource() // [*BooleanInputStream]()
	tr.secondsStreamSource = MissingStreamSource() // [LongInputStream]()
	tr.nanosStreamSource = MissingStreamSource()   // [LongInputStream]()
	tr.readOffset = 0
	tr.nextBatchSize = 0
	tr.presentStream = nil
	tr.secondsStream = nil
	tr.nanosStream = nil
	tr.rowGroupOpen = false
}

// @Override
func (tr *TimestampColumnReader) StartRowGroup(dataStreamSources *InputStreamSources) {
	tr.presentStreamSource = GetInputStreamSource[*BooleanInputStream](dataStreamSources, tr.column, metadata.PRESENT)
	tr.secondsStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, tr.column, metadata.DATA)
	tr.nanosStreamSource = GetInputStreamSource[LongInputStream](dataStreamSources, tr.column, metadata.SECONDARY)
	tr.readOffset = 0
	tr.nextBatchSize = 0
	tr.presentStream = nil
	tr.secondsStream = nil
	tr.nanosStream = nil
	tr.rowGroupOpen = false
}

// @Override
func (tr *TimestampColumnReader) ToString() string {
	return util.NewSB().AppendString(tr.column.String()).String()
}

// @Override
func (tr *TimestampColumnReader) Close() {
	tr.memoryContext.Close()
}

// @Override
func (tr *TimestampColumnReader) GetRetainedSizeInBytes() int64 {
	return int64(TIMESTAMP_COLUMN_READER_INSTANCE_SIZE)
}

func (tr *TimestampColumnReader) isFileUtc() bool {
	return tr.fileDateTimeZone == time.UTC
}

func (tr *TimestampColumnReader) decodeNanos(serialized int64) int32 {
	// the last three bits encode the leading zeros removed minus one
	zeros := int32(serialized & 0b111)
	nanos := int32(uint64(serialized) >> 3)
	if zeros > 0 {
		nanos *= POWERS_OF_TEN[zeros+1]
	}
	if (nanos < 0) || (nanos > 999_999_999) {
		panic(fmt.Sprintf("Nanos field of timestamp is out of range: %d", nanos))
	}
	return nanos
}

func (tr *TimestampColumnReader) readNonNullTimestampMillis() block.Block {
	millis := make([]int64, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		millis[i] = tr.readTimestampMillis()
	}
	return block.NewLongArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), millis)
}

func (tr *TimestampColumnReader) readNullTimestampMillis(isNull []bool) block.Block {
	l := util.Lens(isNull)
	millis := make([]int64, l)
	for i := util.INT32_ZERO; i < l; i++ {
		if !isNull[i] {
			millis[i] = tr.readTimestampMillis()
		}
	}
	return block.NewLongArrayBlock(l, optional.Of(isNull), millis)
}

func (tr *TimestampColumnReader) readTimestampMillis() int64 {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	// MILLIS_PER_SECOND
	millis := (seconds + tr.baseTimestampInSeconds) * time.Second.Milliseconds()
	nanos := int64(tr.decodeNanos(serializedNanos))
	if nanos != 0 {
		if millis < 0 {
			millis -= time.Second.Milliseconds()
		}
		millis += block.RoundDiv(nanos, time.Millisecond.Nanoseconds()) // NANOSECONDS_PER_MILLISECOND
	}
	if !tr.isFileUtc() {
		// TODO:
		t := time.Unix(seconds, nanos)
		millis = t.In(tr.fileDateTimeZone).UnixMilli()
	}
	// MICROSECONDS_PER_MILLISECOND
	return millis * time.Millisecond.Microseconds()
}

func (tr *TimestampColumnReader) readNonNullTimestampMicros() block.Block {
	micros := make([]int64, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		micros[i] = tr.readTimestampMicros()
	}
	return block.NewLongArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), micros)
}

func (tr *TimestampColumnReader) readNullTimestampMicros(isNull []bool) block.Block {
	micros := make([]int64, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		if !isNull[i] {
			micros[i] = tr.readTimestampMicros()
		}
	}
	return block.NewLongArrayBlock(tr.nextBatchSize, optional.Of(isNull), micros)
}

func (tr *TimestampColumnReader) readTimestampMicros() int64 {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	micros := (seconds + tr.baseTimestampInSeconds) * time.Second.Microseconds() // MICROSECONDS_PER_SECOND
	nanos := int64(tr.decodeNanos(serializedNanos))
	if nanos != 0 {
		if micros < 0 {
			micros -= time.Second.Microseconds() //MICROSECONDS_PER_SECOND
		}
		micros += block.RoundDiv(nanos, time.Microsecond.Nanoseconds()) //NANOSECONDS_PER_MICROSECOND
	}
	if !tr.isFileUtc() {
		millis := maths.FloorDiv(micros, time.Millisecond.Microseconds())         //floorDiv(micros, MICROSECONDS_PER_MILLISECOND)
		microsFraction := maths.FloorMod(micros, time.Millisecond.Microseconds()) //Maths.floorMod(micros, MICROSECONDS_PER_MILLISECOND)

		// millis = fileDateTimeZone.convertUTCToLocal(millis)
		t := time.Unix(seconds, nanos)
		millis = t.In(tr.fileDateTimeZone).UnixMilli()
		micros = (millis * time.Millisecond.Microseconds()) + microsFraction
	}
	return micros
}

func (tr *TimestampColumnReader) readNonNullTimestampNanos() block.Block {
	microsValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		tr.readTimestampNanos(i, microsValues, picosFractionValues)
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), microsValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readNullTimestampNanos(isNull []bool) block.Block {
	microsValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		if !isNull[i] {
			tr.readTimestampNanos(i, microsValues, picosFractionValues)
		}
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Of(isNull), microsValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readTimestampNanos(i int32, microsValues []int64, picosFractionValues []int32) {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	micros := (seconds + tr.baseTimestampInSeconds) * time.Second.Microseconds() //MICROSECONDS_PER_SECOND
	nanos := int64(tr.decodeNanos(serializedNanos))
	picosFraction := util.INT32_ZERO
	if nanos != 0 {
		if micros < 0 {
			micros -= time.Second.Microseconds() //MICROSECONDS_PER_SECOND
		}
		micros += nanos / time.Microsecond.Nanoseconds() //NANOSECONDS_PER_MICROSECOND
		nanos %= time.Microsecond.Nanoseconds()          // NANOSECONDS_PER_MICROSECOND
		picosFraction = util.Int32Exact(nanos * 1000)    //PICOSECONDS_PER_NANOSECOND
	}
	if !tr.isFileUtc() {
		millis := maths.FloorDiv(micros, time.Millisecond.Microseconds()) //MICROSECONDS_PER_MILLISECOND
		microsFraction := maths.FloorMod(micros, time.Millisecond.Microseconds())
		// millis = fileDateTimeZone.convertUTCToLocal(millis)
		t := time.Unix(seconds, nanos)
		millis = t.In(tr.fileDateTimeZone).UnixMilli()
		micros = (millis * time.Millisecond.Microseconds()) + microsFraction
	}
	microsValues[i] = micros
	picosFractionValues[i] = picosFraction
}

func (tr *TimestampColumnReader) readNonNullInstantMillis() block.Block {
	millis := make([]int64, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		millis[i] = tr.readInstantMillis()
	}
	return block.NewLongArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), millis)
}

func (tr *TimestampColumnReader) readNullInstantMillis(isNull []bool) block.Block {
	millis := make([]int64, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		if !isNull[i] {
			millis[i] = tr.readInstantMillis()
		}
	}
	return block.NewLongArrayBlock(tr.nextBatchSize, optional.Of(isNull), millis)
}

func (tr *TimestampColumnReader) readInstantMillis() int64 {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	millis := (seconds + BASE_INSTANT_IN_SECONDS) * time.Second.Milliseconds() // MILLIS_PER_SECOND
	nanos := int64(tr.decodeNanos(serializedNanos))
	if nanos != 0 {
		if millis < 0 {
			millis -= time.Second.Milliseconds() // MILLIS_PER_SECOND
		}
		millis += block.RoundDiv(nanos, time.Millisecond.Nanoseconds()) //NANOSECONDS_PER_MILLISECOND
	}
	return block.PackDateTimeWithZone3(millis, block.UTC_KEY)
}

func (tr *TimestampColumnReader) readNonNullInstantMicros() block.Block {
	millisValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		tr.readInstantMicros(i, millisValues, picosFractionValues)
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), millisValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readNullInstantMicros(isNull []bool) block.Block {
	millisValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		if !isNull[i] {
			tr.readInstantMicros(i, millisValues, picosFractionValues)
		}
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Of(isNull), millisValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readInstantMicros(i int32, millisValues []int64, picosFractionValues []int32) {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	millis := (seconds + BASE_INSTANT_IN_SECONDS) * time.Second.Milliseconds() //MILLIS_PER_SECOND
	nanos := int64(tr.decodeNanos(serializedNanos))
	picosFraction := util.INT32_ZERO
	if nanos != 0 {
		if millis < 0 {
			millis -= time.Second.Milliseconds()
		}
		millis += nanos / time.Millisecond.Nanoseconds()                                              //NANOSECONDS_PER_MILLISECOND
		nanos %= time.Millisecond.Nanoseconds()                                                       //NANOSECONDS_PER_MILLISECOND
		picosFraction = util.Int32Exact(block.RoundDiv(nanos, time.Microsecond.Nanoseconds())) * 1000 //NANOSECONDS_PER_MICROSECOND
	}
	millisValues[i] = block.PackDateTimeWithZone3(millis, block.UTC_KEY)
	picosFractionValues[i] = picosFraction
}

func (tr *TimestampColumnReader) readNonNullInstantNanos() block.Block {
	millisValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		tr.readInstantNanos(i, millisValues, picosFractionValues)
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Empty[[]bool](), millisValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readNullInstantNanos(isNull []bool) block.Block {
	millisValues := make([]int64, tr.nextBatchSize)
	picosFractionValues := make([]int32, tr.nextBatchSize)
	for i := util.INT32_ZERO; i < tr.nextBatchSize; i++ {
		if !isNull[i] {
			tr.readInstantNanos(i, millisValues, picosFractionValues)
		}
	}
	return block.NewInt96ArrayBlock(tr.nextBatchSize, optional.Of(isNull), millisValues, picosFractionValues)
}

func (tr *TimestampColumnReader) readInstantNanos(i int32, millisValues []int64, picosFractionValues []int32) {
	seconds := tr.secondsStream.Next()
	serializedNanos := tr.nanosStream.Next()
	millis := (seconds + BASE_INSTANT_IN_SECONDS) * time.Second.Milliseconds() //MILLIS_PER_SECOND
	nanos := int64(tr.decodeNanos(serializedNanos))
	picosFraction := util.INT32_ZERO
	if nanos != 0 {
		if millis < 0 {
			millis -= time.Second.Milliseconds()
		}
		millis += nanos / time.Millisecond.Nanoseconds() // NANOSECONDS_PER_MILLISECOND
		nanos %= time.Millisecond.Nanoseconds()          //NANOSECONDS_PER_MILLISECOND
		picosFraction = util.Int32Exact(nanos * 1000)
	}
	millisValues[i] = block.PackDateTimeWithZone3(millis, block.UTC_KEY)
	picosFractionValues[i] = picosFraction
}
