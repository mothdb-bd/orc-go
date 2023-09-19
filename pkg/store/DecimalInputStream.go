package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LONG_MASK uint64 = 0x80_80_80_80_80_80_80_80
	INT_MASK  uint32 = 0x80_80_80_80
)

type DecimalInputStream struct {
	// 继承
	ValueInputStream[*DecimalStreamCheckpoint]

	chunkLoader    MothChunkLoader
	block          *slice.Slice
	blockOffset    int32
	lastCheckpoint int64
}

func NewDecimalInputStream(chunkLoader MothChunkLoader) *DecimalInputStream {
	dm := new(DecimalInputStream)
	dm.block = slice.EMPTY_SLICE

	dm.chunkLoader = chunkLoader
	return dm
}

// @Override
func (dm *DecimalInputStream) SeekToCheckpoint(ct StreamCheckpoint) {
	dt := ct.(*DecimalStreamCheckpoint)
	newCheckpoint := dt.GetInputStreamCheckpoint()
	if dm.block.Size() > 0 && DecodeCompressedBlockOffset(newCheckpoint) == DecodeCompressedBlockOffset(dm.lastCheckpoint) {
		blockOffset := DecodeDecompressedOffset(newCheckpoint) - DecodeDecompressedOffset(dm.lastCheckpoint)
		if blockOffset >= 0 && blockOffset < int32(dm.block.Size()) {
			dm.blockOffset = blockOffset
			return
		}
	}
	dm.chunkLoader.SeekToCheckpoint(newCheckpoint)
	dm.lastCheckpoint = newCheckpoint
	dm.block = slice.EMPTY_SLICE
	dm.blockOffset = 0
}

// @SuppressWarnings("PointlessBitwiseExpression")
func (dm *DecimalInputStream) NextLongDecimal(result []int64, batchSize int32) {
	count := util.INT32_ZERO
	for count < batchSize {
		if dm.blockOffset == int32(dm.block.Size()) {
			dm.advance()
		}
		for dm.blockOffset <= int32(dm.block.Size())-20 {
			var low int64
			middle := util.INT64_ZERO
			high := util.INT64_ZERO
			current, _ := dm.block.GetInt64LE(int(dm.blockOffset))

			zeros := maths.NumberOfTrailingZeros(^current & int64(LONG_MASK))
			end := (zeros + 1) / 8
			dm.blockOffset += end
			negative := (current & 1) == 1
			low = maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00_00), 7)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00), 6)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00), 5)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00), 4)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00), 3)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00), 2)
			low |= maths.UnsignedRightShift((current & 0x7F_00), 1)
			low |= maths.UnsignedRightShift((current & 0x7F), 0)
			low = low & ((int64(1) << (end * 7)) - 1)
			if zeros == 64 {
				current, _ = dm.block.GetInt64LE(int(dm.blockOffset))
				zeros = maths.NumberOfTrailingZeros(int64(uint64(^current) & LONG_MASK))
				end = (zeros + 1) / 8
				dm.blockOffset += end
				middle = maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00_00), 7)
				middle |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00), 6)
				middle |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00), 5)
				middle |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00), 4)
				middle |= maths.UnsignedRightShift((current & 0x7F_00_00_00), 3)
				middle |= maths.UnsignedRightShift((current & 0x7F_00_00), 2)
				middle |= maths.UnsignedRightShift((current & 0x7F_00), 1)
				middle |= maths.UnsignedRightShift((current & 0x7F), 0)
				middle = middle & ((int64(1) << (end * 7)) - 1)
				if zeros == 64 {
					last, _ := dm.block.GetInt32LE(int(dm.blockOffset))
					zeros = maths.NumberOfTrailingZerosInt32(int32(uint32(^last) & INT_MASK))
					end = (zeros + 1) / 8
					dm.blockOffset += end
					high = int64(maths.UnsignedRightShiftInt32((last & 0x7F_00_00), 2))
					high |= int64(maths.UnsignedRightShiftInt32((last & 0x7F_00), 1))
					high |= int64(maths.UnsignedRightShiftInt32((last & 0x7F), 0))
					high = high & ((1 << (end * 7)) - 1)
					if end == 4 || high > 0xFF_FF {
						panic("Decimal exceeds 128 bits")
					}
				}
			}
			emitLongDecimal(result, count, low, middle, high, negative)
			count++
			if count == batchSize {
				return
			}
		}
		count = dm.decodeLongDecimalTail(result, count, batchSize)
	}
}

func (dm *DecimalInputStream) decodeLongDecimalTail(result []int64, count int32, batchSize int32) int32 {
	negative := false
	low := util.INT64_ZERO
	middle := util.INT64_ZERO
	high := util.INT64_ZERO
	var value int64
	last := false
	if dm.blockOffset == int32(dm.block.Size()) {
		dm.advance()
	}
	offset := 0
	for true {
		tmpValue, _ := dm.block.GetByte(int(dm.blockOffset))
		value = int64(tmpValue)
		dm.blockOffset++
		if offset == 0 {
			negative = (value & 1) == 1
			low |= (value & 0x7F)
		} else if offset < 8 {
			low |= (value & 0x7F) << (offset * 7)
		} else if offset < 16 {
			middle |= (value & 0x7F) << ((offset - 8) * 7)
		} else if offset < 19 {
			high |= (value & 0x7F) << ((offset - 16) * 7)
		} else {
			panic("Decimal exceeds 128 bits")
		}
		offset++
		if (value & 0x80) == 0 {
			if high > 0xFF_FF {
				panic("Decimal exceeds 128 bits")
			}
			emitLongDecimal(result, count, low, middle, high, negative)
			count++
			low = 0
			middle = 0
			high = 0
			offset = 0
			if dm.blockOffset == int32(dm.block.Size()) {
				break
			}
			if last || count == batchSize {
				break
			}
		} else if dm.blockOffset == int32(dm.block.Size()) {
			last = true
			dm.advance()
		}
	}
	return count
}

func emitLongDecimal(result []int64, offset int32, low int64, middle int64, high int64, negative bool) {
	lower := int64(uint64(low)>>1) | (middle << 55)
	upper := int64(uint64(middle)>>9) | (high << 47)
	if negative {
		// MOTH encodes decimals using a zig-zag vint strategy
		// For negative values, the encoded value is given by:
		//     encoded = -value * 2 - 1
		//
		// Therefore,
		//     value = -(encoded + 1) / 2
		//           = -encoded / 2 - 1/2
		//
		// Given the identity -v = ~v + 1 for negating a value using
		// two's complement representation,
		//
		//     value = (~encoded + 1) / 2 - 1/2
		//           = ~encoded / 2 + 1/2 - 1/2
		//           = ~encoded / 2
		//
		// The shift is performed above as the bits are assembled. The negation
		// is performed here.
		lower = ^lower
		upper = ^upper
	}
	result[2*offset] = upper
	result[2*offset+1] = lower
}

// @SuppressWarnings("PointlessBitwiseExpression")
func (dm *DecimalInputStream) NextShortDecimal(result []int64, batchSize int32) {
	count := util.INT32_ZERO
	for count < batchSize {
		if dm.blockOffset == dm.block.SizeInt32() {
			dm.advance()
		}
		for dm.blockOffset <= dm.block.SizeInt32()-12 {
			var low int64
			high := util.INT64_ZERO
			current, _ := dm.block.GetInt64LE(int(dm.blockOffset))
			zeros := maths.NumberOfTrailingZeros(int64(uint64(^current) & LONG_MASK))
			end := (zeros + 1) / 8
			dm.blockOffset += end
			low = maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00_00), 7)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00_00), 6)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00_00), 5)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00_00), 4)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00_00), 3)
			low |= maths.UnsignedRightShift((current & 0x7F_00_00), 2)
			low |= maths.UnsignedRightShift((current & 0x7F_00), 1)
			low |= maths.UnsignedRightShift((current & 0x7F), 0)
			low = low & ((int64(1) << (end * 7)) - 1)
			if zeros == 64 {
				last, _ := dm.block.GetInt32LE(int(dm.blockOffset))
				zeros = maths.NumberOfTrailingZerosInt32(int32(uint32(^last) & INT_MASK))
				end = (zeros + 1) / 8
				dm.blockOffset += end
				high = int64(maths.UnsignedRightShiftInt32(int32(uint32(last)&0x7F_00), 1))
				high |= int64(maths.UnsignedRightShiftInt32(int32(uint32(last)&0x7F), 0))
				high = high & ((1 << (end * 7)) - 1)
				if end >= 3 || high > 0xFF {
					panic("Decimal does not fit long (invalid table schema?)")
				}
			}
			emitShortDecimal(result, count, low, high)
			count++
			if count == batchSize {
				return
			}
		}
		count = dm.decodeShortDecimalTail(result, count, batchSize)
	}
}

func (dm *DecimalInputStream) decodeShortDecimalTail(result []int64, count int32, batchSize int32) int32 {
	low := util.INT64_ZERO
	high := util.INT64_ZERO
	var value int64
	last := false
	offset := 0
	if dm.blockOffset == int32(dm.block.Size()) {
		dm.advance()
	}
	for true {
		tmpValue, _ := dm.block.GetByte(int(dm.blockOffset))
		value = int64(tmpValue)

		dm.blockOffset++
		if offset == 0 {
			low |= (value & 0x7F)
		} else if offset < 8 {
			low |= (value & 0x7F) << (offset * 7)
		} else if offset < 11 {
			high |= (value & 0x7F) << ((offset - 8) * 7)
		} else {
			panic("Decimal does not fit long (invalid table schema?)")
		}
		offset++
		if (value & 0x80) == 0 {
			if high > 0xFF {
				panic("Decimal does not fit long (invalid table schema?)")
			}
			emitShortDecimal(result, count, low, high)
			count++
			low = 0
			high = 0
			offset = 0
			if dm.blockOffset == int32(dm.block.Size()) {
				break
			}
			if last || count == batchSize {
				break
			}
		} else if dm.blockOffset == int32(dm.block.Size()) {
			last = true
			dm.advance()
		}
	}
	return count
}

func emitShortDecimal(result []int64, offset int32, low int64, high int64) {
	negative := (low & 1) == 1
	value := int64(uint64(low)>>1) | (high << 55)
	if negative {
		value = ^value
	}
	result[offset] = value
}

// @Override
func (dm *DecimalInputStream) Skip(items int64) {
	if items == 0 {
		return
	}
	if dm.blockOffset == int32(dm.block.Size()) {
		dm.advance()
	}
	count := util.INT32_ZERO
	for true {
		for dm.blockOffset <= int32(dm.block.Size())-util.INT64_BYTES {
			current, _ := dm.block.GetInt64LE(int(dm.blockOffset))

			increment := maths.BitCount(int64(uint64(^current) & LONG_MASK))
			if int64(count+increment) >= items {
				break
			}
			count += increment
			dm.blockOffset += util.INT64_BYTES
		}
		for dm.blockOffset < int32(dm.block.Size()) {
			current, _ := dm.block.GetByte(int(dm.blockOffset))
			dm.blockOffset++
			if (current & 0x80) == 0 {
				count++
				if int64(count) == items {
					return
				}
			}
		}
		dm.advance()
	}
}

func (dm *DecimalInputStream) advance() {
	dm.block = dm.chunkLoader.NextChunk()
	dm.lastCheckpoint = dm.chunkLoader.GetLastCheckpoint()
	dm.blockOffset = 0
}
