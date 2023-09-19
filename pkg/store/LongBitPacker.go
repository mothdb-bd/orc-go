package store

import (
	"io"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var MAX_BUFFERED_POSITIONS int32 = 512

type LongBitPacker struct {
	// tmp   []byte
	slice *slice.Slice
}

func NewLongBitPacker() *LongBitPacker {
	lr := new(LongBitPacker)
	tmp := make([]byte, util.INT64_BYTES*MAX_BUFFERED_POSITIONS)
	lr.slice = slice.NewWithBuf(tmp)
	return lr
}

func (lr *LongBitPacker) Unpack(buffer []int64, offset int32, len int32, bitSize int32, input mothio.InputStream) {
	util.CheckArgument2(len <= MAX_BUFFERED_POSITIONS, "Expected MOTH files to have runs of at most 512 bit packed longs")
	switch bitSize {
	case 1:
		lr.unpack1(buffer, offset, len, input)
	case 2:
		lr.unpack2(buffer, offset, len, input)
	case 4:
		lr.unpack4(buffer, offset, len, input)
	case 8:
		lr.unpack8(buffer, offset, len, input)
	case 16:
		lr.unpack16(buffer, offset, len, input)
	case 24:
		lr.unpack24(buffer, offset, len, input)
	case 32:
		lr.unpack32(buffer, offset, len, input)
	case 40:
		lr.unpack40(buffer, offset, len, input)
	case 48:
		lr.unpack48(buffer, offset, len, input)
	case 56:
		lr.unpack56(buffer, offset, len, input)
	case 64:
		lr.unpack64(buffer, offset, len, input)
	default:
		unpackGeneric(buffer, offset, len, bitSize, input)
	}
}

func unpackGeneric(buffer []int64, offset int32, len int32, bitSize int32, input mothio.InputStream) {
	bitsLeft := util.INT32_ZERO
	current := util.INT32_ZERO
	for i := offset; i < (offset + len); i++ {
		result := util.INT32_ZERO
		bitsLeftToRead := bitSize
		for bitsLeftToRead > bitsLeft {
			result <<= bitsLeft
			result |= current & ((1 << bitsLeft) - 1)
			bitsLeftToRead -= bitsLeft
			b, _ := input.ReadBS()
			current = int32(b)
			bitsLeft = 8
		}
		if bitsLeftToRead > 0 {
			result <<= bitsLeftToRead
			bitsLeft -= bitsLeftToRead
			result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1)
		}
		buffer[i] = int64(result)
	}
}

func (lr *LongBitPacker) GetTmp() []byte {
	return make([]byte, util.INT64_BYTES*MAX_BUFFERED_POSITIONS)
}

func (lr *LongBitPacker) unpack1(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	if len != 0 && len < 8 {
		b, _ := input.ReadBS()
		unpack1Unaligned(buffer, offset, len, int32(b))
		return
	}
	blockReadableBytes := (len + 7) / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	outputIndex := offset
	end := offset + len
	tmpIndex := 0
	for ; outputIndex+7 < end; outputIndex += 8 {
		var value int64 = int64(tmp[tmpIndex])
		tmpIndex++
		buffer[outputIndex] = maths.UnsignedRightShift((0b1000_0000 & value), 7)
		buffer[outputIndex+1] = maths.UnsignedRightShift((0b0100_0000 & value), 6)
		buffer[outputIndex+2] = maths.UnsignedRightShift((0b0010_0000 & value), 5)
		buffer[outputIndex+3] = maths.UnsignedRightShift((0b0001_0000 & value), 4)
		buffer[outputIndex+4] = maths.UnsignedRightShift((0b0000_1000 & value), 3)
		buffer[outputIndex+5] = maths.UnsignedRightShift((0b0000_0100 & value), 2)
		buffer[outputIndex+6] = maths.UnsignedRightShift((0b0000_0010 & value), 1)
		buffer[outputIndex+7] = 0b0000_0001 & value
	}
	if outputIndex < end {
		unpack1Unaligned(buffer, outputIndex, end-outputIndex, int32(tmp[blockReadableBytes-1]))
	}
}

func unpack1Unaligned(buffer []int64, outputIndex int32, length int32, value int32) {
	switch length {
	case 7:
		buffer[outputIndex+6] = maths.UnsignedRightShift(int64(0b0000_0010&value), 1)
		//noinspection fallthrough
	case 6:
		buffer[outputIndex+5] = maths.UnsignedRightShift(int64(0b0000_0100&value), 2)
		//noinspection fallthrough
	case 5:
		buffer[outputIndex+4] = maths.UnsignedRightShift(int64(0b0000_1000&value), 3)
		//noinspection fallthrough
	case 4:
		buffer[outputIndex+3] = maths.UnsignedRightShift(int64(0b0001_0000&value), 4)
		//noinspection fallthrough
	case 3:
		buffer[outputIndex+2] = maths.UnsignedRightShift(int64(0b0010_0000&value), 5)
		//noinspection fallthrough
	case 2:
		buffer[outputIndex+1] = maths.UnsignedRightShift(int64(0b0100_0000&value), 6)
		//noinspection fallthrough
	case 1:
		buffer[outputIndex] = maths.UnsignedRightShift(int64(0b1000_0000&value), 7)
	}
}

func (lr *LongBitPacker) unpack2(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	if len != 0 && len < 4 {
		b, _ := input.ReadBS()
		unpack2Unaligned(buffer, offset, len, int32(b))
		return
	}
	blockReadableBytes := (2*len + 7) / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	outputIndex := offset
	end := offset + len
	tmpIndex := 0
	for ; outputIndex+3 < end; outputIndex += 4 {
		var value int64
		value = int64(tmp[tmpIndex])
		tmpIndex++
		buffer[outputIndex] = maths.UnsignedRightShift((0b1100_0000 & value), 6)
		buffer[outputIndex+1] = maths.UnsignedRightShift((0b0011_0000 & value), 4)
		buffer[outputIndex+2] = maths.UnsignedRightShift((0b0000_1100 & value), 2)
		buffer[outputIndex+3] = 0b0000_0011 & value
	}
	if outputIndex < end {
		unpack2Unaligned(buffer, outputIndex, end-outputIndex, int32(tmp[blockReadableBytes-1]))
	}
}

func unpack2Unaligned(buffer []int64, outputIndex int32, length int32, value int32) {
	switch length {
	case 3:
		buffer[outputIndex+2] = maths.UnsignedRightShift(int64(0b0000_1100&value), 2)
		//noinspection fallthrough
	case 2:
		buffer[outputIndex+1] = maths.UnsignedRightShift(int64(0b0011_0000&value), 4)
		//noinspection fallthrough
	case 1:
		buffer[outputIndex] = maths.UnsignedRightShift(int64(0b1100_0000&value), 6)
	}
}

func (lr *LongBitPacker) unpack4(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	if len != 0 && len < 3 {
		b, _ := input.ReadBS()
		value := int64(b)
		buffer[offset] = maths.UnsignedRightShift(0b1111_0000&value, 4)
		if len == 2 {
			buffer[offset+1] = 0b0000_1111 & value
		}
		return
	}
	blockReadableBytes := (4*len + 7) / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	outputIndex := offset
	end := offset + len
	tmpIndex := 0
	for ; outputIndex+1 < end; outputIndex += 2 {
		var value int64
		value = int64(tmp[tmpIndex])
		tmpIndex++
		buffer[outputIndex] = maths.UnsignedRightShift((0b1111_0000 & value), 4)
		buffer[outputIndex+1] = 0b0000_1111 & value
	}
	if outputIndex != end {
		buffer[outputIndex] = maths.UnsignedRightShift((0b1111_0000 & int64(tmp[blockReadableBytes-1])), 4)
	}
}

func (lr *LongBitPacker) unpack8(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	tmp := lr.GetTmp()
	for i := 0; i < int(len); {
		t, err := input.ReadBS3(tmp, i, int(len)-i)
		if err == nil || err == io.EOF {
			i += t
		} else {
			panic(err)
		}
	}
	for i := util.INT32_ZERO; i < len; i++ {
		buffer[offset+i] = 0xFF & int64(tmp[i])
	}
}

func reverseBytesInt16(i int16) int16 {
	return int16(((int32(i) & 0xFF00) >> 8) | (int32(i) << 8))
}

func (lr *LongBitPacker) unpack16(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 16 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)

	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt16LE(2 * i)
		buffer[offset+int32(i)] = 0xFFFF & int64(reverseBytesInt16(t))
	}
}

func reverseBytesInt32(i int32) int32 {
	return maths.UnsignedRightShiftInt32(i, 24) |
		((i >> 8) & 0xFF00) |
		((i << 8) & 0xFF0000) |
		(i << 24)
}

func (lr *LongBitPacker) unpack24(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 24 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)

	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt32LE(3 * i)
		buffer[offset+int32(i)] = 0xFF_FFFF & (maths.UnsignedRightShift(int64(reverseBytesInt32(t)), 8))
	}
}

func (lr *LongBitPacker) unpack32(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 32 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)

	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt32LE(4 * i)
		buffer[offset+int32(i)] = 0xFFFF_FFFF & int64(reverseBytesInt32(t))
	}
}

func reverseBytesInt64(i int64) int64 {
	i = (i&0x00ff00ff00ff00ff)<<8 | maths.UnsignedRightShift(i, 8)&0x00ff00ff00ff00ff
	return (i << 48) | ((i & 0xffff0000) << 16) |
		(maths.UnsignedRightShift(i, 16) & 0xffff0000) | maths.UnsignedRightShift(i, 48)
}

func (lr *LongBitPacker) unpack40(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 40 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)
	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt64LE(5 * i)
		buffer[offset+int32(i)] = maths.UnsignedRightShift(reverseBytesInt64(t), 24)
	}
}

func (lr *LongBitPacker) unpack48(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 48 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)
	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt64LE(6 * i)
		buffer[offset+int32(i)] = maths.UnsignedRightShift(reverseBytesInt64(t), 16)
	}
}

func (lr *LongBitPacker) unpack56(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 56 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)
	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt64LE(7 * i)
		buffer[offset+int32(i)] = maths.UnsignedRightShift(reverseBytesInt64(t), 8)
	}
}

func (lr *LongBitPacker) unpack64(buffer []int64, offset int32, len int32, input mothio.InputStream) {
	blockReadableBytes := len * 64 / 8
	tmp := lr.GetTmp()
	for i := 0; i < int(blockReadableBytes); {
		t, _ := input.ReadBS3(tmp, i, int(blockReadableBytes)-i)
		i += t
	}
	lr.slice = slice.NewWithBuf(tmp)
	for i := 0; i < int(len); i++ {
		t, _ := lr.slice.GetInt64LE(8 * i)
		buffer[offset+int32(i)] = reverseBytesInt64(t)
	}
}
