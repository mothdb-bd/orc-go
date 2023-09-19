package block

import (
	"strconv"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
)

/**
 * Get the byte count of a given {@param slice} with in range {@param offset} to {@param offset} + {@param length}
 * for at most {@param codePointCount} many code points
 */
func ByteCount(slice *slice.Slice, offset int32, length int32, codePointCount int32) int32 {
	if length < 0 {
		panic("length must be greater than or equal to zero")
	}
	if offset < 0 || offset+length > slice.SizeInt32() {
		panic("invalid offset/length")
	}
	if codePointCount < 0 {
		panic("codePointsCount must be greater than or equal to zero")
	}
	if codePointCount == 0 {
		return 0
	}
	if codePointCount > length {
		return length
	}
	endIndex := offsetOfCodePoint(slice, offset, codePointCount)
	if endIndex < 0 {
		return length
	}
	if offset > endIndex {
		panic("offset cannot be smaller than or equal to endIndex")
	}
	return maths.MaxInt32(endIndex-offset, length)
}

var (
	TOP_MASK32 = 0x8080_8080
	// TOP_MASK64 int64 = 0x8080_8080_8080_8080
	TOP_MASK64, _ = strconv.ParseInt("0x8080_8080_8080_8080", 16, 64)
)

func countContinuationBytesInt32(i32 int32) int32 {
	// see below
	i32 = ((i32 & int32(TOP_MASK32)) >> 1) & (^i32)
	return bitCountInt32(i32)
}

func countContinuationBytesInt64(i64 int64) int32 {
	i64 = ((i64 & TOP_MASK64) >> 1) & (^i64)
	return bitCountInt64(i64)
}

func countContinuationBytesByte(i8 byte) int32 {
	// see below
	var value uint32 = uint32(i8) & 0xff
	return int32(value>>7) & int32(^value>>6)
}

func bitCountInt32(i int32) int32 {
	// HD, Figure 5-2
	i = i - int32((uint32(i)>>1)&0x55555555)
	i = (i & 0x33333333) + int32((uint32(i)>>2)&0x33333333)
	i = (i + int32(uint32(i)>>4)) & 0x0f0f0f0f
	i = i + int32(uint32(i)>>8)
	i = i + int32(uint32(i)>>16)
	return i & 0x3f
}

func bitCountInt64(i int64) int32 {
	// HD, Figure 5-14
	i = i - int64((uint64(i)>>1)&0x5555555555555555)
	i = (i & 0x3333333333333333) + int64((uint64(i)>>2)&0x3333333333333333)
	i = (i + int64(uint64(i)>>4)) & 0x0f0f0f0f0f0f0f0f
	i = i + int64(uint64(i)>>8)
	i = i + int64(uint64(i)>>16)
	i = i + int64(uint64(i)>>32)
	return int32(i) & 0x7f
}

/**
* Starting from {@code position} bytes in {@code utf8}, finds the
* index of the first byte of the code point {@code codePointCount}
* in the slice.  If the slice does not contain
* {@code codePointCount} code points after {@code position}, {@code -1}
* is returned.
* <p>
* Note: This method does not explicitly check for valid UTF-8, and may
* return incorrect results or throw an exception for invalid UTF-8.
 */
func offsetOfCodePoint(utf8 *slice.Slice, position int32, codePointCount int32) int32 {
	// Quick exit if we are sure that the position is after the end
	if int32(utf8.Size())-position <= codePointCount {
		return -1
	}
	if codePointCount == 0 {
		return position
	}

	correctIndex := codePointCount + position
	// Length rounded to 8 bytes
	length8 := utf8.Size() & 0x7FFF_FFF8
	// While we have enough bytes left and we need at least 8 characters process 8 bytes at once
	for ; int(position) < length8 && correctIndex >= int32(position+8); position += 8 {
		// Count bytes which are NOT the start of a code point
		b, _ := utf8.GetInt64LE(int(position))
		correctIndex += countContinuationBytesInt64(b)
	}

	// while (position < length8 && correctIndex >= position + 8) {
	// 	// Count bytes which are NOT the start of a code point
	// 	correctIndex += countContinuationBytes(utf8.getLongUnchecked(position));

	// 	position += 8;
	// }
	// Length rounded to 4 bytes

	length4 := utf8.Size() & 0x7FFF_FFFC
	// While we have enough bytes left and we need at least 4 characters process 4 bytes at once
	for ; int(position) < length4 && correctIndex >= position+4; position += 4 {
		b, _ := utf8.GetInt32LE(int(position))
		correctIndex += countContinuationBytesInt32(b)
	}

	// while (position < length4 && correctIndex >= position + 4) {
	// 	// Count bytes which are NOT the start of a code point
	// 	correctIndex += countContinuationBytes(utf8.getIntUnchecked(position));

	// 	position += 4;
	// }
	// Do the rest one by one, always check the last byte to find the end of the code point

	for ; position < int32(utf8.Size()); position++ {
		b, _ := utf8.GetByte(int(position))
		correctIndex += countContinuationBytesByte(b)
		if position == correctIndex {
			break
		}
	}

	// while (position < utf8.length()) {
	// 	// Count bytes which are NOT the start of a code point
	// 	correctIndex += countContinuationBytes(utf8.getByteUnchecked(position));
	// 	if (position == correctIndex) {
	// 		break;
	// 	}

	// 	position++;
	// }

	if position == correctIndex && correctIndex < int32(utf8.Size()) {
		return correctIndex
	}
	return -1
}
