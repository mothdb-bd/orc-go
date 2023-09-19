package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LONGINPUTV2_MIN_REPEAT_SIZE  int32 = 3
	LONGINPUTV2_MAX_LITERAL_SIZE int32 = 512
)

// type EncodingType int8

// const (
// 	SHORT_REPEAT EncodingType = iota
// 	DIRECT
// 	PATCHED_BASE
// 	DELTA
// )

type LongInputStreamV2 struct {
	// 继承
	LongInputStream

	packer                  *LongBitPacker
	input                   *MothInputStream
	signed                  bool
	literals                []int64
	numLiterals             int32
	used                    int32
	skipCorrupt             bool
	lastReadInputCheckpoint int64
}

func NewLongInputStreamV2(input *MothInputStream, signed bool, skipCorrupt bool) *LongInputStreamV2 {
	l2 := new(LongInputStreamV2)
	l2.packer = NewLongBitPacker()
	l2.literals = make([]int64, LONGINPUTV2_MAX_LITERAL_SIZE)
	l2.input = input
	l2.signed = signed
	l2.skipCorrupt = skipCorrupt
	l2.lastReadInputCheckpoint = input.GetCheckpoint()
	return l2
}

func (l2 *LongInputStreamV2) readValues() {
	l2.lastReadInputCheckpoint = l2.input.GetCheckpoint()
	firstByte, err := l2.input.ReadBS()
	firstInt := int32(firstByte)
	if err != nil {
		panic("Read past end of RLE integer")
	}
	enc := (maths.UnsignedRightShiftInt32(firstInt, 6)) & 0x03
	if SHORT_REPEAT.ordinal() == enc {
		l2.readShortRepeatValues(firstInt)
	} else if DIRECT.ordinal() == enc {
		l2.readDirectValues(firstInt)
	} else if PATCHED_BASE.ordinal() == enc {
		l2.readPatchedBaseValues(firstInt)
	} else {
		l2.readDeltaValues(firstInt)
	}
}

func (l2 *LongInputStreamV2) readDeltaValues(firstByte int32) {
	fixedBits := (maths.UnsignedRightShiftInt32(firstByte, 1)) & 0x1f
	if fixedBits != 0 {
		fixedBits = DecodeBitWidth(FixedBitSizes_V1(fixedBits))
	}
	length := (firstByte & 0x01) << 8

	b, _ := l2.input.ReadBS()
	length |= int32(b)
	firstVal := ReadVInt(l2.signed, l2.input)
	l2.literals[l2.numLiterals] = firstVal
	l2.numLiterals++
	var prevVal int64
	if fixedBits == 0 {
		fixedDelta := ReadSignedVInt(l2.input)
		for i := util.INT32_ZERO; i < length; i++ {
			l2.literals[l2.numLiterals] = l2.literals[(l2.numLiterals+1)-2] + fixedDelta
			l2.numLiterals++
		}
	} else {
		deltaBase := ReadSignedVInt(l2.input)
		l2.literals[l2.numLiterals] = firstVal + deltaBase
		l2.numLiterals++
		prevVal = l2.literals[l2.numLiterals-1]
		length -= 1
		l2.packer.Unpack(l2.literals, l2.numLiterals, length, fixedBits, l2.input)
		for length > 0 {
			if deltaBase < 0 {
				l2.literals[l2.numLiterals] = prevVal - l2.literals[l2.numLiterals]
			} else {
				l2.literals[l2.numLiterals] = prevVal + l2.literals[l2.numLiterals]
			}
			prevVal = l2.literals[l2.numLiterals]
			length--
			l2.numLiterals++
		}
	}
}

func bytesToLongBE(input mothio.InputStream, n int32) int64 {
	out := util.INT64_ZERO
	var val int64
	for n > 0 {
		n--
		b, _ := input.ReadBS()
		val = int64(b)
		out |= (val << (n * 8))
	}
	return out
}

// @Override
func (l2 *LongInputStreamV2) Next() int64 {
	if l2.used == l2.numLiterals {
		l2.numLiterals = 0
		l2.used = 0
		l2.readValues()
	}
	re := l2.literals[l2.used]
	l2.used++
	return re
}

// @Override
func (l2 *LongInputStreamV2) Next2(values []int64, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l2.used == l2.numLiterals {
			l2.numLiterals = 0
			l2.used = 0
			l2.readValues()
		}
		chunkSize := maths.MinInt32(l2.numLiterals-l2.used, items)

		util.CopyInt64s(l2.literals, l2.used, values, offset, chunkSize)
		l2.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l2 *LongInputStreamV2) Next3(values []int32, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l2.used == l2.numLiterals {
			l2.numLiterals = 0
			l2.used = 0
			l2.readValues()
		}
		chunkSize := maths.MinInt32(l2.numLiterals-l2.used, items)
		for i := util.INT32_ZERO; i < chunkSize; i++ {
			literal := l2.literals[l2.used+i]
			var value int32 = int32(literal)
			if literal != int64(value) {
				panic("Decoded value out of range for a 32bit number")
			}
			values[offset+i] = value
		}
		l2.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l2 *LongInputStreamV2) Next4(values []int16, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l2.used == l2.numLiterals {
			l2.numLiterals = 0
			l2.used = 0
			l2.readValues()
		}
		chunkSize := maths.MinInt32(l2.numLiterals-l2.used, items)
		for i := util.INT32_ZERO; i < chunkSize; i++ {
			literal := l2.literals[l2.used+i]
			value := int16(literal)
			if literal != int64(value) {
				panic("Decoded value out of range for a 16bit number")
			}
			values[offset+i] = value
		}
		l2.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l2 *LongInputStreamV2) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	v2Checkpoint := checkpoint.(*LongStreamV2Checkpoint)
	if l2.lastReadInputCheckpoint == v2Checkpoint.GetInputStreamCheckpoint() && v2Checkpoint.GetOffset() <= l2.numLiterals {
		l2.used = v2Checkpoint.GetOffset()
	} else {
		l2.input.SeekToCheckpoint(v2Checkpoint.GetInputStreamCheckpoint())
		l2.numLiterals = 0
		l2.used = 0
		l2.Skip(int64(v2Checkpoint.GetOffset()))
	}
}

// @Override
func (l2 *LongInputStreamV2) Skip(items int64) {
	for items > 0 {
		if l2.used == l2.numLiterals {
			l2.numLiterals = 0
			l2.used = 0
			l2.readValues()
		}
		consume := maths.MinInt32(int32(items), l2.numLiterals-l2.used)
		l2.used += consume
		items -= int64(consume)
	}
}

func (l2 *LongInputStreamV2) readShortRepeatValues(firstByte int32) {
	size := (maths.UnsignedRightShiftInt32(firstByte, 3)) & 0b0111
	size += 1
	length := firstByte & 0x07
	length += MIN_REPEAT_SIZE
	val := bytesToLongBE(l2.input, size)
	if l2.signed {
		val = ZigzagDecode(val)
	}
	for i := util.INT32_ZERO; i < length; i++ {
		l2.literals[l2.numLiterals] = val
		l2.numLiterals++
	}
}

func (l2 *LongInputStreamV2) readDirectValues(firstByte int32) {
	fixedBits := DecodeBitWidth(FixedBitSizes_V1(maths.UnsignedRightShiftInt32(firstByte, 1)) & 0b1_1111)
	length := (firstByte & 0b1) << 8
	b, _ := l2.input.ReadBS()
	length |= int32(b)
	length += 1
	l2.packer.Unpack(l2.literals, l2.numLiterals, length, fixedBits, l2.input)
	if l2.signed {
		for i := util.INT32_ZERO; i < length; i++ {
			l2.literals[l2.numLiterals] = ZigzagDecode(l2.literals[l2.numLiterals])
			l2.numLiterals++
		}
	} else {
		l2.numLiterals += length
	}
}

func (l2 *LongInputStreamV2) readPatchedBaseValues(firstByte int32) {
	fb := DecodeBitWidth(FixedBitSizes_V1(maths.UnsignedRightShift(int64(firstByte), 1)) & 0b1_1111)
	length := (firstByte & 0b1) << 8
	b, _ := l2.input.ReadBS()
	length |= int32(b)
	length += 1
	b, _ = l2.input.ReadBS()
	thirdByte := int32(b)
	baseWidth := (maths.UnsignedRightShiftInt32(thirdByte, 5)) & 0b0111
	baseWidth += 1
	patchWidth := DecodeBitWidth(FixedBitSizes_V1(thirdByte) & 0b1_1111)
	b, _ = l2.input.ReadBS()
	fourthByte := int32(b)
	patchGapWidth := (maths.UnsignedRightShiftInt32(fourthByte, 5)) & 0b0111
	patchGapWidth += 1
	patchListLength := fourthByte & 0b1_1111
	base := bytesToLongBE(l2.input, baseWidth)
	mask := (int64(1) << ((baseWidth * 8) - 1))
	if (base & mask) != 0 {
		base = base & ^mask
		base = -base
	}
	unpacked := make([]int64, length)
	l2.packer.Unpack(unpacked, 0, length, fb, l2.input)
	unpackedPatch := make([]int64, patchListLength)
	if patchWidth+patchGapWidth > 64 && !l2.skipCorrupt {
		panic("Invalid RLEv2 encoded stream")
	}
	bitSize := GetClosestFixedBits(patchWidth + patchGapWidth)
	l2.packer.Unpack(unpackedPatch, 0, patchListLength, bitSize, l2.input)
	patchIndex := util.INT32_ZERO
	var currentGap int64
	var currentPatch int64
	patchMask := ((int64(1) << patchWidth) - 1)
	currentGap = maths.UnsignedRightShift(unpackedPatch[patchIndex], int64(patchWidth))
	currentPatch = unpackedPatch[patchIndex] & patchMask
	actualGap := util.INT64_ZERO
	for currentGap == 255 && currentPatch == 0 {
		actualGap += 255
		patchIndex++
		currentGap = maths.UnsignedRightShift(unpackedPatch[patchIndex], int64(patchWidth))
		currentPatch = unpackedPatch[patchIndex] & patchMask
	}
	actualGap += currentGap
	for i := util.INT32_ZERO; i < util.Lens(unpacked); i++ {
		if int64(i) == actualGap {
			patchedValue := unpacked[i] | (currentPatch << fb)
			l2.literals[l2.numLiterals] = base + patchedValue
			l2.numLiterals++
			patchIndex++
			if patchIndex < patchListLength {
				currentGap = maths.UnsignedRightShift(unpackedPatch[patchIndex], int64(patchWidth))
				currentPatch = unpackedPatch[patchIndex] & patchMask
				actualGap = 0
				for currentGap == 255 && currentPatch == 0 {
					actualGap += 255
					patchIndex++
					currentGap = maths.UnsignedRightShift(unpackedPatch[patchIndex], int64(patchWidth))
					currentPatch = unpackedPatch[patchIndex] & patchMask
				}
				actualGap += currentGap
				actualGap += int64(i)
			}
		} else {
			l2.literals[l2.numLiterals] = base + unpacked[i]
			l2.numLiterals++
		}
	}
}

func (l2 *LongInputStreamV2) String() string {
	return "LongInputStreamV2"
}
