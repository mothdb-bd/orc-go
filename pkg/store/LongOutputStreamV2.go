package store

import (
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LONG_OUTPUTV2_INSTANCE_SIZE           int32 = util.SizeOf(&LongOutputStreamV2{})
	LONG_OUTPUTV2_MAX_SCOPE               int32 = 512
	LONG_OUTPUTV2_MIN_REPEAT              int32 = 3
	LONG_OUTPUTV2_MAX_SHORT_REPEAT_LENGTH int32 = 10
)

// private enum EncodingType
// {
// 	SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA;

//		private int getOpCode()
//		{
//			return ordinal() << 6;
//		}
//	}
type EncodingType int8

const (
	SHORT_REPEAT EncodingType = iota
	DIRECT
	PATCHED_BASE
	DELTA
)

func (e EncodingType) ordinal() int32 {
	return int32(e)
}

func (e EncodingType) GetOpCode() int32 {
	return int32(e) << 6
}

type LongOutputStreamV2 struct {

	// 继承
	LongOutputStream

	streamKind          metadata.StreamKind
	buffer              *MothOutputBuffer
	checkpoints         *util.ArrayList[LongStreamCheckpoint]
	prevDelta           int64
	fixedRunLength      int32
	variableRunLength   int32
	literals            []int64
	signed              bool
	numLiterals         int32
	zigzagLiterals      []int64
	baseReducedLiterals []int64
	adjDeltas           []int64
	fixedDelta          int64
	zzBits90p           int32
	zzBits100p          int32
	brBits95p           int32
	brBits100p          int32
	bitsDeltaMax        int32
	patchWidth          int32
	patchGapWidth       int32
	patchLength         int32
	gapVsPatchList      []int64
	min                 int64
	isFixedDelta        bool
	utils               *SerializationUtils
	closed              bool
}

func NewLongOutputStreamV2(compression metadata.CompressionKind, bufferSize int32, signed bool, streamKind metadata.StreamKind) *LongOutputStreamV2 {
	l2 := new(LongOutputStreamV2)

	l2.checkpoints = util.NewArrayList[LongStreamCheckpoint]()
	l2.literals = make([]int64, LONG_OUTPUTV2_MAX_SCOPE)
	l2.zigzagLiterals = make([]int64, LONG_OUTPUTV2_MAX_SCOPE)
	l2.baseReducedLiterals = make([]int64, LONG_OUTPUTV2_MAX_SCOPE)
	l2.adjDeltas = make([]int64, LONG_OUTPUTV2_MAX_SCOPE)
	l2.isFixedDelta = true
	l2.utils = NewSerializationUtils()

	l2.streamKind = streamKind
	l2.buffer = NewMothOutputBuffer(compression, bufferSize)
	l2.signed = signed
	return l2
}

// @Override
func (l2 *LongOutputStreamV2) WriteLong(value int64) {
	util.CheckState(!l2.closed)
	if l2.numLiterals == 0 {
		l2.initializeLiterals(value)
		return
	}
	if l2.numLiterals == 1 {
		l2.prevDelta = value - l2.literals[0]
		l2.literals[l2.numLiterals] = value
		l2.numLiterals++
		if value == l2.literals[0] {
			l2.fixedRunLength = 2
			l2.variableRunLength = 0
		} else {
			l2.fixedRunLength = 0
			l2.variableRunLength = 2
		}
		return
	}
	if l2.prevDelta == 0 && value == l2.literals[l2.numLiterals-1] {
		l2.literals[l2.numLiterals] = value
		l2.numLiterals++
		if l2.variableRunLength > 0 {
			l2.fixedRunLength = 2
		}
		l2.fixedRunLength += 1
		if l2.fixedRunLength >= LONG_OUTPUTV2_MIN_REPEAT && l2.variableRunLength > 0 {
			l2.numLiterals -= LONG_OUTPUTV2_MIN_REPEAT
			l2.variableRunLength -= LONG_OUTPUTV2_MIN_REPEAT - 1
			tailValues := make([]int64, LONG_OUTPUTV2_MIN_REPEAT)

			util.CopyInt64s(l2.literals, l2.numLiterals, tailValues, 0, LONG_OUTPUTV2_MIN_REPEAT)
			// System.arraycopy(l2.literals, l2.numLiterals, tailValues, 0, LONG_OUTPUTV2_MIN_REPEAT)
			l2.writeValues(l2.determineEncoding())
			for _, tailValue := range tailValues {
				l2.literals[l2.numLiterals] = tailValue
				l2.numLiterals++
			}
		}
		if l2.fixedRunLength == LONG_OUTPUTV2_MAX_SCOPE {
			l2.writeValues(l2.determineEncoding())
		}
		return
	}
	if l2.fixedRunLength >= LONG_OUTPUTV2_MIN_REPEAT {
		if l2.fixedRunLength <= LONG_OUTPUTV2_MAX_SHORT_REPEAT_LENGTH {
			l2.writeValues(SHORT_REPEAT)
		} else {
			l2.isFixedDelta = true
			l2.writeValues(DELTA)
		}
	}
	if l2.fixedRunLength > 0 && l2.fixedRunLength < LONG_OUTPUTV2_MIN_REPEAT {
		if value != l2.literals[l2.numLiterals-1] {
			l2.variableRunLength = l2.fixedRunLength
			l2.fixedRunLength = 0
		}
	}
	if l2.numLiterals == 0 {
		l2.initializeLiterals(value)
	} else {
		l2.prevDelta = value - l2.literals[l2.numLiterals-1]
		l2.literals[l2.numLiterals] = value
		l2.numLiterals++
		l2.variableRunLength += 1
		if l2.variableRunLength == LONG_OUTPUTV2_MAX_SCOPE {
			l2.writeValues(l2.determineEncoding())
		}
	}
}

func (l2 *LongOutputStreamV2) initializeLiterals(val int64) {
	l2.literals[l2.numLiterals] = val
	l2.numLiterals++
	l2.fixedRunLength = 1
	l2.variableRunLength = 1
}

func (l2 *LongOutputStreamV2) determineEncoding() EncodingType {
	if l2.signed {
		for i1 := util.INT32_ZERO; i1 < l2.numLiterals; i1++ {
			l2.zigzagLiterals[i1] = zigzagEncode(l2.literals[i1])
		}
	} else {
		util.CopyInt64s(l2.literals, 0, l2.zigzagLiterals, 0, l2.numLiterals)
		// System.arraycopy(l2.literals, 0, l2.zigzagLiterals, 0, l2.numLiterals)
	}
	l2.zzBits100p = percentileBits(l2.zigzagLiterals, 0, l2.numLiterals, 1.0)
	if l2.numLiterals <= LONG_OUTPUTV2_MIN_REPEAT {
		return DIRECT
	}
	isIncreasing := true
	isDecreasing := true
	l2.isFixedDelta = true
	l2.min = l2.literals[0]
	max := l2.literals[0]
	initialDelta := l2.literals[1] - l2.literals[0]
	currDelta := initialDelta
	deltaMax := initialDelta
	l2.adjDeltas[0] = initialDelta
	for i := int32(1); i < l2.numLiterals; i++ {
		l1 := l2.literals[i]
		l0 := l2.literals[i-1]
		currDelta = l1 - l0
		l2.min = maths.Min(l2.min, l1)
		max = maths.Max(max, l1)
		isIncreasing = isIncreasing && (l0 <= l1)
		isDecreasing = isDecreasing && (l0 >= l1)
		l2.isFixedDelta = l2.isFixedDelta && (currDelta == initialDelta)
		if i > 1 {
			l2.adjDeltas[i-1] = maths.AbsInt64(currDelta)
			deltaMax = maths.Max(deltaMax, l2.adjDeltas[i-1])
		}
	}
	if !IsSafeSubtract(max, l2.min) {
		return DIRECT
	}
	if initialDelta != 0 {
		if l2.min == max {
			panic("currDelta should be zero")
		}
		if l2.isFixedDelta {
			l2.fixedDelta = currDelta
			return DELTA
		}
		l2.bitsDeltaMax = findClosestNumBits(deltaMax)
		if isIncreasing || isDecreasing {
			return DELTA
		}
	}
	l2.zzBits90p = percentileBits(l2.zigzagLiterals, 0, l2.numLiterals, 0.9)
	if l2.zzBits100p-l2.zzBits90p <= 1 {
		return DIRECT
	}
	for i := util.INT32_ZERO; i < l2.numLiterals; i++ {
		l2.baseReducedLiterals[i] = l2.literals[i] - l2.min
	}
	l2.brBits95p = percentileBits(l2.baseReducedLiterals, 0, l2.numLiterals, 0.95)
	l2.brBits100p = percentileBits(l2.baseReducedLiterals, 0, l2.numLiterals, 1.0)
	if l2.brBits100p == l2.brBits95p {
		return DIRECT
	}
	return PATCHED_BASE
}

func (l2 *LongOutputStreamV2) writeValues(encoding EncodingType) {
	if l2.numLiterals == 0 {
		return
	}
	switch encoding {
	case SHORT_REPEAT:
		l2.writeShortRepeatValues()
	case DIRECT:
		l2.writeDirectValues()
	case PATCHED_BASE:
		l2.writePatchedBaseValues()
	default:
		l2.writeDeltaValues()
	}
	l2.clearEncoder()
}

func (l2 *LongOutputStreamV2) writeShortRepeatValues() {
	var repeatVal int64
	if l2.signed {
		repeatVal = zigzagEncode(l2.literals[0])
	} else {
		repeatVal = l2.literals[0]
	}
	numBitsRepeatVal := findClosestNumBits(repeatVal)
	var numBytesRepeatVal int32
	if numBitsRepeatVal%8 == 0 {
		numBytesRepeatVal = maths.UnsignedRightShiftInt32(numBitsRepeatVal, 3)
	} else {
		numBytesRepeatVal = maths.UnsignedRightShiftInt32(numBitsRepeatVal, 3) + 1
	}
	header := SHORT_REPEAT.GetOpCode()
	header |= ((numBytesRepeatVal - 1) << 3)
	l2.fixedRunLength -= LONG_OUTPUTV2_MIN_REPEAT
	header |= l2.fixedRunLength
	l2.buffer.Write(byte(header))
	for i := numBytesRepeatVal - 1; i >= 0; i-- {
		b := ((maths.UnsignedRightShift(repeatVal, int64(i*8))) & 0xff)
		l2.buffer.Write(byte(b))
	}
	l2.fixedRunLength = 0
}

func (l2 *LongOutputStreamV2) writeDirectValues() {
	fixedBits := GetClosestAlignedFixedBits(l2.zzBits100p)
	encodeBitWidth := encodeBitWidth(fixedBits) << 1
	l2.variableRunLength -= 1
	tailBits := maths.UnsignedRightShiftInt32((l2.variableRunLength & 0x100), 8)
	headerFirstByte := DIRECT.GetOpCode() | encodeBitWidth | tailBits
	headerSecondByte := l2.variableRunLength & 0xff
	l2.buffer.Write(byte(headerFirstByte))
	l2.buffer.Write(byte(headerSecondByte))
	l2.utils.writeInts(l2.zigzagLiterals, 0, l2.numLiterals, fixedBits, l2.buffer)
	l2.variableRunLength = 0
}

func (l2 *LongOutputStreamV2) writeDeltaValues() {
	fixedBits := GetClosestAlignedFixedBits(l2.bitsDeltaMax)
	var length int32
	ebw := util.INT32_ZERO // encodeBitWidth
	if l2.isFixedDelta {
		if l2.fixedRunLength > LONG_OUTPUTV2_MIN_REPEAT {
			length = l2.fixedRunLength - 1
			l2.fixedRunLength = 0
		} else {
			length = l2.variableRunLength - 1
			l2.variableRunLength = 0
		}
	} else {
		if fixedBits == 1 {
			fixedBits = 2
		}
		ebw = encodeBitWidth(fixedBits) << 1
		length = l2.variableRunLength - 1
		l2.variableRunLength = 0
	}
	tailBits := maths.UnsignedRightShiftInt32((length & 0x100), 8)
	headerFirstByte := DELTA.GetOpCode() | ebw | tailBits
	headerSecondByte := length & 0xff
	l2.buffer.Write(byte(headerFirstByte))
	l2.buffer.Write(byte(headerSecondByte))
	if l2.signed {
		writeVslong(l2.buffer, l2.literals[0])
	} else {
		writeVulong(l2.buffer, l2.literals[0])
	}
	if l2.isFixedDelta {
		writeVslong(l2.buffer, l2.fixedDelta)
	} else {
		writeVslong(l2.buffer, l2.adjDeltas[0])
		l2.utils.writeInts(l2.adjDeltas, 1, l2.numLiterals-2, fixedBits, l2.buffer)
	}
}

func (l2 *LongOutputStreamV2) writePatchedBaseValues() {
	l2.preparePatchedBlob()
	fb := l2.brBits95p
	efb := encodeBitWidth(fb) << 1
	l2.variableRunLength -= 1
	tailBits := maths.UnsignedRightShiftInt32((l2.variableRunLength & 0x100), 8)
	headerFirstByte := PATCHED_BASE.GetOpCode() | efb | tailBits
	headerSecondByte := l2.variableRunLength & 0xff
	isNegative := l2.min < 0
	if isNegative {
		l2.min = -l2.min
	}
	baseWidth := findClosestNumBits(l2.min) + 1
	baseBytes := util.Ternary(baseWidth%8 == 0, baseWidth/8, (baseWidth/8)+1)
	bb := (baseBytes - 1) << 5
	if isNegative {
		l2.min |= (int64(1) << ((baseBytes * 8) - 1))
	}
	headerThirdByte := bb | encodeBitWidth(l2.patchWidth)
	headerFourthByte := (l2.patchGapWidth-1)<<5 | l2.patchLength
	l2.buffer.Write(byte(headerFirstByte))
	l2.buffer.Write(byte(headerSecondByte))
	l2.buffer.Write(byte(headerThirdByte))
	l2.buffer.Write(byte(headerFourthByte))
	for i := baseBytes - 1; i >= 0; i-- {
		b := byte((maths.UnsignedRightShift(l2.min, int64(i*8))) & 0xff)
		l2.buffer.Write(b)
	}
	closestFixedBits := getClosestFixedBits(fb)
	l2.utils.writeInts(l2.baseReducedLiterals, 0, l2.numLiterals, closestFixedBits, l2.buffer)
	closestFixedBits = getClosestFixedBits(l2.patchGapWidth + l2.patchWidth)
	l2.utils.writeInts(l2.gapVsPatchList, 0, util.Lens(l2.gapVsPatchList), closestFixedBits, l2.buffer)
	l2.variableRunLength = 0
}

func (l2 *LongOutputStreamV2) preparePatchedBlob() {
	mask := (int64(1) << l2.brBits95p) - 1
	l2.patchLength = int32(math.Ceil(float64(l2.numLiterals) * 0.05))
	gapList := make([]int32, l2.patchLength)
	patchList := make([]int64, l2.patchLength)
	l2.patchWidth = l2.brBits100p - l2.brBits95p
	l2.patchWidth = getClosestFixedBits(l2.patchWidth)
	if l2.patchWidth == 64 {
		l2.patchWidth = 56
		l2.brBits95p = 8
		mask = (int64(1) << l2.brBits95p) - 1
	}
	gapIdx := util.INT32_ZERO
	patchIdx := util.INT32_ZERO
	prev := util.INT32_ZERO
	var gap int32
	maxGap := util.INT32_ZERO
	for i := util.INT32_ZERO; i < l2.numLiterals; i++ {
		if l2.baseReducedLiterals[i] > mask {
			gap = i - prev
			if gap > maxGap {
				maxGap = gap
			}
			prev = i
			gapList[gapIdx] = gap
			gapIdx++

			patch := maths.UnsignedRightShift(l2.baseReducedLiterals[i], int64(l2.brBits95p))
			patchList[patchIdx] = patch
			patchIdx++
			l2.baseReducedLiterals[i] &= mask
		}
	}
	l2.patchLength = gapIdx
	if maxGap == 0 && l2.patchLength != 0 {
		l2.patchGapWidth = 1
	} else {
		l2.patchGapWidth = findClosestNumBits(int64(maxGap))
	}
	if l2.patchGapWidth > 8 {
		l2.patchGapWidth = 8
		if maxGap == 511 {
			l2.patchLength += 2
		} else {
			l2.patchLength += 1
		}
	}
	gapIdx = 0
	patchIdx = 0
	l2.gapVsPatchList = make([]int64, l2.patchLength)
	for i := util.INT32_ZERO; i < l2.patchLength; i++ {
		g := gapList[gapIdx]
		gapIdx++
		p := patchList[patchIdx]
		patchIdx++
		for g > 255 {
			l2.gapVsPatchList[i] = (int64(255) << l2.patchWidth)
			i++
			g -= 255
		}
		l2.gapVsPatchList[i] = int64(g<<l2.patchWidth) | p
	}
}

func (l2 *LongOutputStreamV2) clearEncoder() {
	l2.numLiterals = 0
	l2.prevDelta = 0
	l2.fixedDelta = 0
	l2.zzBits90p = 0
	l2.zzBits100p = 0
	l2.brBits95p = 0
	l2.brBits100p = 0
	l2.bitsDeltaMax = 0
	l2.patchGapWidth = 0
	l2.patchLength = 0
	l2.patchWidth = 0
	l2.gapVsPatchList = nil
	l2.min = 0
	l2.isFixedDelta = true
}

func (l2 *LongOutputStreamV2) flush() {
	if l2.numLiterals == 0 {
		return
	}
	if l2.variableRunLength != 0 {
		l2.writeValues(l2.determineEncoding())
		return
	}
	if l2.fixedRunLength == 0 {
		panic("literals does not agree with run length counters")
	}
	if l2.fixedRunLength < LONG_OUTPUTV2_MIN_REPEAT {
		l2.variableRunLength = l2.fixedRunLength
		l2.fixedRunLength = 0
		l2.writeValues(l2.determineEncoding())
		return
	}
	if l2.fixedRunLength <= LONG_OUTPUTV2_MAX_SHORT_REPEAT_LENGTH {
		l2.writeValues(SHORT_REPEAT)
		return
	}
	l2.isFixedDelta = true
	l2.writeValues(DELTA)
}

// @Override
func (l2 *LongOutputStreamV2) RecordCheckpoint() {
	util.CheckState(!l2.closed)
	l2.checkpoints.Add(NewLongStreamV2Checkpoint(l2.numLiterals, l2.buffer.GetCheckpoint()))
}

// @Override
func (l2 *LongOutputStreamV2) Close() {
	l2.closed = true
	l2.flush()
	l2.buffer.Close()
}

// @Override
func (l2 *LongOutputStreamV2) GetCheckpoints() *util.ArrayList[LongStreamCheckpoint] {
	util.CheckState(l2.closed)
	return l2.checkpoints
}

// @Override
func (l2 *LongOutputStreamV2) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(l2.buffer.WriteDataTo, metadata.NewStream(columnId, l2.streamKind, util.Int32Exact(l2.buffer.GetOutputDataSize()), true))
}

// @Override
func (l2 *LongOutputStreamV2) GetBufferedBytes() int64 {
	return l2.buffer.EstimateOutputDataSize() + int64(util.INT64_BYTES*l2.numLiterals)
}

// @Override
func (l2 *LongOutputStreamV2) GetRetainedBytes() int64 {
	return int64(LONG_OUTPUTV2_INSTANCE_SIZE) + l2.buffer.GetRetainedSize() + util.SizeOfInt64(l2.literals) + util.SizeOfInt64(l2.zigzagLiterals) + util.SizeOfInt64(l2.baseReducedLiterals) + util.SizeOfInt64(l2.adjDeltas) + util.SizeOfInt64(l2.gapVsPatchList)
}

// @Override
func (l2 *LongOutputStreamV2) Reset() {
	l2.clearEncoder()
	l2.closed = false
	l2.buffer.Reset()
	l2.checkpoints.Clear()
}

// SerializationUtils 类
var SERIALIZATIONUTILS_BUFFER_SIZE int32 = 64

type SerializationUtils struct {
	writeBuffer []byte
}

func NewSerializationUtils() *SerializationUtils {
	ss := new(SerializationUtils)
	ss.writeBuffer = make([]byte, SERIALIZATIONUTILS_BUFFER_SIZE)
	return ss
}

func writeVulong(output slice.SliceOutput, value int64) {
	for true {
		if (value & ^0x7f) == 0 {
			output.Write(byte(value))
			return
		} else {
			output.Write(byte(0x80 | (value & 0x7f)))
			value = maths.UnsignedRightShift(value, 7)
		}
	}
}

func writeVslong(output slice.SliceOutput, value int64) {
	writeVulong(output, (value<<1)^(value>>63))
}

func findClosestNumBits(value int64) int32 {
	count := util.INT32_ZERO
	for value != 0 {
		count++
		value = maths.UnsignedRightShift(value, 1)
	}
	return getClosestFixedBits(count)
}

func percentileBits(data []int64, offset int32, length int32, percentile float64) int32 {
	util.CheckArgument(percentile <= 1.0 && percentile > 0.0)
	hist := make([]int32, 32)
	for i := offset; i < (offset + length); i++ {
		idx := encodeBitWidth(findClosestNumBits(data[i]))
		hist[idx] += 1
	}
	perLen := (length * int32(1.0-percentile))
	for i := util.Lens(hist) - 1; i >= 0; i-- {
		perLen -= hist[i]
		if perLen < 0 {
			return ss_decodeBitWidth(i)
		}
	}
	return 0
}

func getClosestFixedBits(n int32) int32 {
	if n == 0 {
		return 1
	}
	if n >= 1 && n <= 24 {
		return n
	} else if n > 24 && n <= 26 {
		return 26
	} else if n > 26 && n <= 28 {
		return 28
	} else if n > 28 && n <= 30 {
		return 30
	} else if n > 30 && n <= 32 {
		return 32
	} else if n > 32 && n <= 40 {
		return 40
	} else if n > 40 && n <= 48 {
		return 48
	} else if n > 48 && n <= 56 {
		return 56
	} else {
		return 64
	}
}

func GetClosestAlignedFixedBits(n int32) int32 {
	if n == 0 || n == 1 {
		return 1
	} else if n > 1 && n <= 2 {
		return 2
	} else if n > 2 && n <= 4 {
		return 4
	} else if n > 4 && n <= 8 {
		return 8
	} else if n > 8 && n <= 16 {
		return 16
	} else if n > 16 && n <= 24 {
		return 24
	} else if n > 24 && n <= 32 {
		return 32
	} else if n > 32 && n <= 40 {
		return 40
	} else if n > 40 && n <= 48 {
		return 48
	} else if n > 48 && n <= 56 {
		return 56
	} else {
		return 64
	}
}

type FixedBitSizes int8

const (
	ONE FixedBitSizes = iota
	TWO
	THREE
	FOUR
	FIVE
	SIX
	SEVEN
	EIGHT
	NINE
	TEN
	ELEVEN
	TWELVE
	THIRTEEN
	FOURTEEN
	FIFTEEN
	SIXTEEN
	SEVENTEEN
	EIGHTEEN
	NINETEEN
	TWENTY
	TWENTY_ONE
	TWENTY_TWO
	TWENTY_THREE
	TWENTY_FOUR
	TWENTY_SIX
	TWENTY_EIGHT
	THIRTY
	THIRTY_TWO
	FORTY
	FORTY_EIGHT
	FIFTY_SIX
	SIXTY_FOUR
)

func (fs FixedBitSizes) ordinal() int32 {
	return int32(fs)
}

func zigzagEncode(value int64) int64 {
	return (value << 1) ^ (value >> 63)
}

/**
* Decodes the ordinal fixed bit value to actual fixed bit width value.
 */
func decodeBitWidth(n FixedBitSizes) int32 {
	if n >= ONE && n <= TWENTY_FOUR {
		return int32(n) + 1
	} else if n == TWENTY_SIX {
		return 26
	} else if n == TWENTY_EIGHT {
		return 28
	} else if n == THIRTY {
		return 30
	} else if n == THIRTY_TWO {
		return 32
	} else if n == FORTY {
		return 40
	} else if n == FORTY_EIGHT {
		return 48
	} else if n == FIFTY_SIX {
		return 56
	} else {
		return 64
	}
}

func encodeBitWidth(n int32) int32 {
	n = getClosestFixedBits(n)
	if n >= 1 && n <= 24 {
		return n - 1
	} else if n > 24 && n <= 26 {
		return TWENTY_SIX.ordinal()
	} else if n > 26 && n <= 28 {
		return TWENTY_EIGHT.ordinal()
	} else if n > 28 && n <= 30 {
		return THIRTY.ordinal()
	} else if n > 30 && n <= 32 {
		return THIRTY_TWO.ordinal()
	} else if n > 32 && n <= 40 {
		return FORTY.ordinal()
	} else if n > 40 && n <= 48 {
		return FORTY_EIGHT.ordinal()
	} else if n > 48 && n <= 56 {
		return FIFTY_SIX.ordinal()
	} else {
		return SIXTY_FOUR.ordinal()
	}
}

func ss_decodeBitWidth(n int32) int32 {
	if n >= ONE.ordinal() && n <= TWENTY_FOUR.ordinal() {
		return n + 1
	} else if n == TWENTY_SIX.ordinal() {
		return 26
	} else if n == TWENTY_EIGHT.ordinal() {
		return 28
	} else if n == THIRTY.ordinal() {
		return 30
	} else if n == THIRTY_TWO.ordinal() {
		return 32
	} else if n == FORTY.ordinal() {
		return 40
	} else if n == FORTY_EIGHT.ordinal() {
		return 48
	} else if n == FIFTY_SIX.ordinal() {
		return 56
	} else {
		return 64
	}
}

func (ss *SerializationUtils) writeInts(input []int64, offset int32, length int32, bitSize int32, output slice.SliceOutput) {
	util.CheckArgument(util.Lens(input) != 0)
	util.CheckArgument(offset >= 0)
	util.CheckArgument(length >= 1)
	util.CheckArgument(bitSize >= 1)
	switch bitSize {
	case 1:
		unrolledBitPack1(input, offset, length, output)
		return
	case 2:
		unrolledBitPack2(input, offset, length, output)
		return
	case 4:
		unrolledBitPack4(input, offset, length, output)
		return
	case 8:
		ss.unrolledBitPack8(input, offset, length, output)
		return
	case 16:
		ss.unrolledBitPack16(input, offset, length, output)
		return
	case 24:
		ss.unrolledBitPack24(input, offset, length, output)
		return
	case 32:
		ss.unrolledBitPack32(input, offset, length, output)
		return
	case 40:
		ss.unrolledBitPack40(input, offset, length, output)
		return
	case 48:
		ss.unrolledBitPack48(input, offset, length, output)
		return
	case 56:
		ss.unrolledBitPack56(input, offset, length, output)
		return
	case 64:
		ss.unrolledBitPack64(input, offset, length, output)
		return
	}
	bitsLeft := int32(8)
	current := util.INT64_ZERO
	for i := offset; i < (offset + length); i++ {
		value := input[i]
		bitsToWrite := bitSize
		for bitsToWrite > bitsLeft {
			current |= maths.UnsignedRightShift(value, int64(bitsToWrite-bitsLeft))
			bitsToWrite -= bitsLeft
			value &= (int64(1) << bitsToWrite) - 1
			output.Write(byte(current))
			current = 0
			bitsLeft = 8
		}
		bitsLeft -= bitsToWrite
		current |= value << bitsLeft
		if bitsLeft == 0 {
			output.Write(byte(current))
			current = 0
			bitsLeft = 8
		}
	}
	if bitsLeft != 8 {
		output.Write(byte(current))
	}
}

func unrolledBitPack1(input []int64, offset int32, len int32, output slice.SliceOutput) {
	numHops := int32(8)
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := util.INT64_ZERO
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | ((input[i] & 1) << 7) | ((input[i+1] & 1) << 6) | ((input[i+2] & 1) << 5) | ((input[i+3] & 1) << 4) | ((input[i+4] & 1) << 3) | ((input[i+5] & 1) << 2) | ((input[i+6] & 1) << 1) | (input[i+7])&1)
		output.Write(byte(val))
		val = 0
	}
	if remainder > 0 {
		startShift := 7
		for i := endUnroll; i < endOffset; i++ {
			val = (val | (input[i]&1)<<startShift)
			startShift -= 1
		}
		output.Write(byte(val))
	}
}

func unrolledBitPack2(input []int64, offset int32, len int32, output slice.SliceOutput) {
	numHops := int32(4)
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := util.INT64_ZERO
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | ((input[i] & 3) << 6) | ((input[i+1] & 3) << 4) | ((input[i+2] & 3) << 2) | (input[i+3])&3)
		output.Write(byte(val))
		val = 0
	}

	if remainder > 0 {
		startShift := 6
		for i := endUnroll; i < endOffset; i++ {
			val = (val | (input[i]&3)<<startShift)
			startShift -= 2
		}
		output.Write(byte(val))
	}
}

func unrolledBitPack4(input []int64, offset int32, len int32, output slice.SliceOutput) {
	numHops := int32(2)
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := util.INT64_ZERO
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | ((input[i] & 15) << 4) | (input[i+1])&15)
		output.Write(byte(val))
		val = 0
	}
	if remainder > 0 {
		startShift := 4
		for i := endUnroll; i < endOffset; i++ {
			val = (val | (input[i]&15)<<startShift)
			startShift -= 4
		}
		output.Write(byte(val))
	}
}

func (ss *SerializationUtils) unrolledBitPack8(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 1)
}

func (ss *SerializationUtils) unrolledBitPack16(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 2)
}

func (ss *SerializationUtils) unrolledBitPack24(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 3)
}

func (ss *SerializationUtils) unrolledBitPack32(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 4)
}

func (ss *SerializationUtils) unrolledBitPack40(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 5)
}

func (ss *SerializationUtils) unrolledBitPack48(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 6)
}

func (ss *SerializationUtils) unrolledBitPack56(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 7)
}

func (ss *SerializationUtils) unrolledBitPack64(input []int64, offset int32, len int32, output slice.SliceOutput) {
	ss.unrolledBitPackBytes(input, offset, len, output, 8)
}

func (ss *SerializationUtils) unrolledBitPackBytes(input []int64, offset int32, len int32, output slice.SliceOutput, numBytes int32) {
	numHops := int32(8)
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	i := offset
	for ; i < endUnroll; i = i + numHops {
		ss.writeLongBE(output, input, i, numHops, numBytes)
	}
	if remainder > 0 {
		ss.writeRemainingLongs(output, i, input, remainder, numBytes)
	}
}

func (ss *SerializationUtils) writeRemainingLongs(output slice.SliceOutput, offset int32, input []int64, remainder int32, numBytes int32) {
	numHops := remainder
	idx := util.INT32_ZERO
	switch numBytes {
	case 1:
		for remainder > 0 {
			ss.writeBuffer[idx] = byte(input[offset+idx] & 255)
			remainder--
			idx++
		}
	case 2:
		for remainder > 0 {
			ss.writeLongBE2(input[offset+idx], idx*2)
			remainder--
			idx++
		}
	case 3:
		for remainder > 0 {
			ss.writeLongBE3(input[offset+idx], idx*3)
			remainder--
			idx++
		}
	case 4:
		for remainder > 0 {
			ss.writeLongBE4(input[offset+idx], idx*4)
			remainder--
			idx++
		}
	case 5:
		for remainder > 0 {
			ss.writeLongBE5(input[offset+idx], idx*5)
			remainder--
			idx++
		}
	case 6:
		for remainder > 0 {
			ss.writeLongBE6(input[offset+idx], idx*6)
			remainder--
			idx++
		}
	case 7:
		for remainder > 0 {
			ss.writeLongBE7(input[offset+idx], idx*7)
			remainder--
			idx++
		}
	case 8:
		for remainder > 0 {
			ss.writeLongBE8(input[offset+idx], idx*8)
			remainder--
			idx++
		}
	default:
	}
	toWrite := numHops * numBytes
	output.WriteBS2(ss.writeBuffer, 0, toWrite)
}

func (ss *SerializationUtils) writeLongBE(output slice.SliceOutput, input []int64, offset int32, numHops int32, numBytes int32) {
	switch numBytes {
	case 1:
		ss.writeBuffer[0] = byte(input[offset+0] & 255)
		ss.writeBuffer[1] = byte(input[offset+1] & 255)
		ss.writeBuffer[2] = byte(input[offset+2] & 255)
		ss.writeBuffer[3] = byte(input[offset+3] & 255)
		ss.writeBuffer[4] = byte(input[offset+4] & 255)
		ss.writeBuffer[5] = byte(input[offset+5] & 255)
		ss.writeBuffer[6] = byte(input[offset+6] & 255)
		ss.writeBuffer[7] = byte(input[offset+7] & 255)
	case 2:
		ss.writeLongBE2(input[offset+0], 0)
		ss.writeLongBE2(input[offset+1], 2)
		ss.writeLongBE2(input[offset+2], 4)
		ss.writeLongBE2(input[offset+3], 6)
		ss.writeLongBE2(input[offset+4], 8)
		ss.writeLongBE2(input[offset+5], 10)
		ss.writeLongBE2(input[offset+6], 12)
		ss.writeLongBE2(input[offset+7], 14)
	case 3:
		ss.writeLongBE3(input[offset+0], 0)
		ss.writeLongBE3(input[offset+1], 3)
		ss.writeLongBE3(input[offset+2], 6)
		ss.writeLongBE3(input[offset+3], 9)
		ss.writeLongBE3(input[offset+4], 12)
		ss.writeLongBE3(input[offset+5], 15)
		ss.writeLongBE3(input[offset+6], 18)
		ss.writeLongBE3(input[offset+7], 21)
	case 4:
		ss.writeLongBE4(input[offset+0], 0)
		ss.writeLongBE4(input[offset+1], 4)
		ss.writeLongBE4(input[offset+2], 8)
		ss.writeLongBE4(input[offset+3], 12)
		ss.writeLongBE4(input[offset+4], 16)
		ss.writeLongBE4(input[offset+5], 20)
		ss.writeLongBE4(input[offset+6], 24)
		ss.writeLongBE4(input[offset+7], 28)
	case 5:
		ss.writeLongBE5(input[offset+0], 0)
		ss.writeLongBE5(input[offset+1], 5)
		ss.writeLongBE5(input[offset+2], 10)
		ss.writeLongBE5(input[offset+3], 15)
		ss.writeLongBE5(input[offset+4], 20)
		ss.writeLongBE5(input[offset+5], 25)
		ss.writeLongBE5(input[offset+6], 30)
		ss.writeLongBE5(input[offset+7], 35)
	case 6:
		ss.writeLongBE6(input[offset+0], 0)
		ss.writeLongBE6(input[offset+1], 6)
		ss.writeLongBE6(input[offset+2], 12)
		ss.writeLongBE6(input[offset+3], 18)
		ss.writeLongBE6(input[offset+4], 24)
		ss.writeLongBE6(input[offset+5], 30)
		ss.writeLongBE6(input[offset+6], 36)
		ss.writeLongBE6(input[offset+7], 42)
	case 7:
		ss.writeLongBE7(input[offset+0], 0)
		ss.writeLongBE7(input[offset+1], 7)
		ss.writeLongBE7(input[offset+2], 14)
		ss.writeLongBE7(input[offset+3], 21)
		ss.writeLongBE7(input[offset+4], 28)
		ss.writeLongBE7(input[offset+5], 35)
		ss.writeLongBE7(input[offset+6], 42)
		ss.writeLongBE7(input[offset+7], 49)
	case 8:
		ss.writeLongBE8(input[offset+0], 0)
		ss.writeLongBE8(input[offset+1], 8)
		ss.writeLongBE8(input[offset+2], 16)
		ss.writeLongBE8(input[offset+3], 24)
		ss.writeLongBE8(input[offset+4], 32)
		ss.writeLongBE8(input[offset+5], 40)
		ss.writeLongBE8(input[offset+6], 48)
		ss.writeLongBE8(input[offset+7], 56)
	default:
	}
	toWrite := numHops * numBytes
	output.WriteBS2(ss.writeBuffer, 0, toWrite)
}

func (ss *SerializationUtils) writeLongBE2(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE3(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE4(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 24))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+3] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE5(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 32))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 24))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+3] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+4] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE6(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 40))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 32))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 24))
	ss.writeBuffer[wbOffset+3] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+4] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+5] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE7(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 48))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 40))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 32))
	ss.writeBuffer[wbOffset+3] = byte(maths.UnsignedRightShift(val, 24))
	ss.writeBuffer[wbOffset+4] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+5] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+6] = byte(maths.UnsignedRightShift(val, 0))
}

func (ss *SerializationUtils) writeLongBE8(val int64, wbOffset int32) {
	ss.writeBuffer[wbOffset+0] = byte(maths.UnsignedRightShift(val, 56))
	ss.writeBuffer[wbOffset+1] = byte(maths.UnsignedRightShift(val, 48))
	ss.writeBuffer[wbOffset+2] = byte(maths.UnsignedRightShift(val, 40))
	ss.writeBuffer[wbOffset+3] = byte(maths.UnsignedRightShift(val, 32))
	ss.writeBuffer[wbOffset+4] = byte(maths.UnsignedRightShift(val, 24))
	ss.writeBuffer[wbOffset+5] = byte(maths.UnsignedRightShift(val, 16))
	ss.writeBuffer[wbOffset+6] = byte(maths.UnsignedRightShift(val, 8))
	ss.writeBuffer[wbOffset+7] = byte(maths.UnsignedRightShift(val, 0))
}

func IsSafeSubtract(left int64, right int64) bool {
	return ((left ^ right) >= 0) || ((left ^ (left - right)) >= 0)
}
