package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

// private static final int HIGH_BIT_MASK = 0b1000_0000;
var HIGH_BIT_MASK int32 = 0b1000_0000

type BooleanInputStream struct {
	// 继承
	ValueInputStream[*BooleanStreamCheckpoint]

	// private final ByteInputStream byteStream;
	byteStream *ByteInputStream
	// private byte data;
	data byte
	// private int bitsInData;
	bitsInData int32
}

// public NEWBooleanInputStream(MothInputStream byteStream)
//
//	{
//	    this.byteStream = new ByteInputStream(byteStream);
//	}
func NewBooleanInputStream(byteStream *MothInputStream) *BooleanInputStream {
	bi := new(BooleanInputStream)
	bi.byteStream = NewByteInputStream(byteStream)
	return bi
}

func (bi *BooleanInputStream) ReadByte() {
	bi.data = bi.byteStream.Next()
	bi.bitsInData = 8
}

func (bi *BooleanInputStream) NextBit() bool {
	if bi.bitsInData == 0 {
		bi.ReadByte()
	}

	// read bit
	result := (int32(bi.data) & HIGH_BIT_MASK) != 0

	// mark bit consumed
	bi.data <<= 1
	bi.bitsInData--

	return result
}

// @Override
// public void seekToCheckpoint(BooleanStreamCheckpoint checkpoint)
//         throws IOException
// {
//     byteStream.seekToCheckpoint(checkpoint.getByteStreamCheckpoint());
//     bitsInData = 0;
//     skip(checkpoint.getOffset());
// }

func (bi *BooleanInputStream) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	bt := checkpoint.(*BooleanStreamCheckpoint)
	bi.byteStream.SeekToCheckpoint(bt.GetByteStreamCheckpoint())
	bi.bitsInData = 0
	bi.Skip(int64(bt.GetOffset()))
}

// @Override
// public void skip(long items)
// 		throws IOException
// {
// 	if (bitsInData >= items) {
// 		data <<= items;
// 		bitsInData -= items;
// 	}
// 	else {
// 		items -= bitsInData;
// 		bitsInData = 0;

// 		byteStream.skip(items >>> 3);
// 		items &= 0b111;

//			if (items != 0) {
//				readByte();
//				data <<= items;
//				bitsInData -= items;
//			}
//		}
//	}
func (bi *BooleanInputStream) SkipInt32(items int32) {
	bi.Skip(int64(items))
}

func (bi *BooleanInputStream) Skip(items int64) {
	iInt32 := int32(items)
	if bi.bitsInData >= iInt32 {
		bi.data <<= items
		bi.bitsInData -= iInt32
	} else {
		items -= int64(bi.bitsInData)
		bi.bitsInData = 0

		bi.byteStream.Skip(int64(items) >> 3)
		items &= 0b111

		if items != 0 {
			bi.ReadByte()
			bi.data <<= items
			bi.bitsInData -= int32(items)
		}
	}
}

// public int countBitsSet(int items)
// 		throws IOException
// {
// 	int count = 0;

// 	// count buffered data
// 	if (items > bitsInData && bitsInData > 0) {
// 		count += bitCount(data);
// 		items -= bitsInData;
// 		bitsInData = 0;
// 	}

// 	// count whole bytes
// 	while (items > 8) {
// 		count += bitCount(byteStream.next());
// 		items -= 8;
// 	}

// 	// count remaining bits
// 	for (int i = 0; i < items; i++) {
// 		count += nextBit() ? 1 : 0;
// 	}

//		return count;
//	}
func (bi *BooleanInputStream) CountBitsSet(items int32) int32 {
	count := util.INT32_ZERO

	// count buffered data
	if items > bi.bitsInData && bi.bitsInData > 0 {
		count += bitCount(bi.data)
		items -= bi.bitsInData
		bi.bitsInData = 0
	}

	// count whole bytes
	for ; items > 8; items -= 8 {
		count += bitCount(bi.byteStream.Next())
	}

	// count remaining bits
	for i := util.INT32_ZERO; i < items; i++ {
		if bi.NextBit() {
			count += 1
		} else {
			count += 0
		}
	}

	return count
}

// /**
// 	* Gets a vector of bytes set to 1 if the bit is set.
// 	*/
// public byte[] getSetBits(int batchSize)
// 		throws IOException
// {
// 	byte[] vector = new byte[batchSize];
// 	getSetBits(vector, batchSize);
// 	return vector;
// }

func (bi *BooleanInputStream) GetSetBits(batchSize int32) []byte {
	vector := make([]byte, batchSize)
	bi.GetSetBits2(vector, batchSize)
	return vector
}

// /**
// 	* Sets the vector element to 1 if the bit is set.
// 	*/
// @SuppressWarnings({"PointlessBitwiseExpression", "PointlessArithmeticExpression", "UnusedAssignment"})
// public void getSetBits(byte[] vector, int batchSize)
// 		throws IOException
// {
// 	int offset = 0;

// 	// handle the head
// 	int count = Math.min(batchSize, bitsInData);
// 	if (count != 0) {
// 		int value = data >>> (8 - count);
// 		switch (count) {
// 			case 7:
// 				vector[offset++] = (byte) ((value & 64) >>> 6);
// 				// fall through
// 			case 6:
// 				vector[offset++] = (byte) ((value & 32) >>> 5);
// 				// fall through
// 			case 5:
// 				vector[offset++] = (byte) ((value & 16) >>> 4);
// 				// fall through
// 			case 4:
// 				vector[offset++] = (byte) ((value & 8) >>> 3);
// 				// fall through
// 			case 3:
// 				vector[offset++] = (byte) ((value & 4) >>> 2);
// 				// fall through
// 			case 2:
// 				vector[offset++] = (byte) ((value & 2) >>> 1);
// 				// fall through
// 			case 1:
// 				vector[offset++] = (byte) ((value & 1) >>> 0);
// 		}
// 		data <<= count;
// 		bitsInData -= count;

// 		if (count == batchSize) {
// 			return;
// 		}
// 	}
// 	// the middle part
// 	while (offset < batchSize - 7) {
// 		byte value = byteStream.next();
// 		vector[offset + 0] = (byte) ((value & 128) >>> 7);
// 		vector[offset + 1] = (byte) ((value & 64) >>> 6);
// 		vector[offset + 2] = (byte) ((value & 32) >>> 5);
// 		vector[offset + 3] = (byte) ((value & 16) >>> 4);
// 		vector[offset + 4] = (byte) ((value & 8) >>> 3);
// 		vector[offset + 5] = (byte) ((value & 4) >>> 2);
// 		vector[offset + 6] = (byte) ((value & 2) >>> 1);
// 		vector[offset + 7] = (byte) ((value & 1));
// 		offset += 8;
// 	}

//		// the tail
//		int remaining = batchSize - offset;
//		if (remaining > 0) {
//			byte data = byteStream.next();
//			int value = data >>> (8 - remaining);
//			switch (remaining) {
//				case 7:
//					vector[offset++] = (byte) ((value & 64) >>> 6);
//					// fall through
//				case 6:
//					vector[offset++] = (byte) ((value & 32) >>> 5);
//					// fall through
//				case 5:
//					vector[offset++] = (byte) ((value & 16) >>> 4);
//					// fall through
//				case 4:
//					vector[offset++] = (byte) ((value & 8) >>> 3);
//					// fall through
//				case 3:
//					vector[offset++] = (byte) ((value & 4) >>> 2);
//					// fall through
//				case 2:
//					vector[offset++] = (byte) ((value & 2) >>> 1);
//					// fall through
//				case 1:
//					vector[offset++] = (byte) ((value & 1) >>> 0);
//			}
//			this.data = (byte) (data << remaining);
//			bitsInData = 8 - remaining;
//		}
//	}
func (bi *BooleanInputStream) GetSetBits2(vector []byte, batchSize int32) {
	offset := util.INT32_ZERO

	// handle the head
	count := maths.MinInt32(batchSize, bi.bitsInData)
	if count != 0 {
		value := uint8(bi.data) >> (8 - count)
		switch count {
		case 7:
			vector[offset] = byte((value & 64) >> 6)
			offset++
			// fall through
		case 6:
			vector[offset] = byte((value & 32) >> 5)
			offset++
			// fall through
		case 5:
			vector[offset] = byte((value & 16) >> 4)
			offset++
			// fall through
		case 4:
			vector[offset] = byte((value & 8) >> 3)
			offset++
			// fall through
		case 3:
			vector[offset] = byte((value & 4) >> 2)
			offset++
			// fall through
		case 2:
			vector[offset] = byte((value & 2) >> 1)
			offset++
			// fall through
		case 1:
			vector[offset] = byte((value & 1) >> 0)
			offset++
		}
		bi.data <<= count
		bi.bitsInData -= count

		if count == batchSize {
			return
		}
	}

	// 	// the middle part
	for ; offset < batchSize-7; offset += 8 {
		value := bi.byteStream.Next()
		vector[offset+0] = byte((value & 128) >> 7)
		vector[offset+1] = byte((value & 64) >> 6)
		vector[offset+2] = byte((value & 32) >> 5)
		vector[offset+3] = byte((value & 16) >> 4)
		vector[offset+4] = byte((value & 8) >> 3)
		vector[offset+5] = byte((value & 4) >> 2)
		vector[offset+6] = byte((value & 2) >> 1)
		vector[offset+7] = byte((value & 1))
		offset += 8
	}

	// the tail
	remaining := batchSize - offset
	if remaining > 0 {
		data := bi.byteStream.Next()
		value := uint8(data) >> (8 - remaining)
		switch remaining {
		case 7:
			vector[offset] = byte((value & 64) >> 6)
			offset++
			// fall through
		case 6:
			vector[offset] = byte((value & 32) >> 5)
			offset++
			// fall through
		case 5:
			vector[offset] = byte((value & 16) >> 4)
			offset++
			// fall through
		case 4:
			vector[offset] = byte((value & 8) >> 3)
			offset++
			// fall through
		case 3:
			vector[offset] = byte((value & 4) >> 2)
			offset++
			// fall through
		case 2:
			vector[offset] = byte((value & 2) >> 1)
			offset++
			// fall through
		case 1:
			vector[offset] = byte((value & 1) >> 0)
			offset++
		}
		bi.data = byte(data << remaining)
		bi.bitsInData = 8 - remaining
	}
}

// /**
// 	* Sets the vector element to true if the bit is not set.
// 	*/
// @SuppressWarnings({"PointlessArithmeticExpression", "UnusedAssignment"})
// public int getUnsetBits(int batchSize, boolean[] vector)
// 		throws IOException
// {
// 	int unsetCount = 0;
// 	int offset = 0;

// 	// handle the head
// 	int count = Math.min(batchSize, bitsInData);
// 	if (count != 0) {
// 		int value = (data & 0xFF) >>> (8 - count);
// 		unsetCount += (count - Integer.bitCount(value));
// 		switch (count) {
// 			case 7:
// 				vector[offset++] = (value & 64) == 0;
// 				// fall through
// 			case 6:
// 				vector[offset++] = (value & 32) == 0;
// 				// fall through
// 			case 5:
// 				vector[offset++] = (value & 16) == 0;
// 				// fall through
// 			case 4:
// 				vector[offset++] = (value & 8) == 0;
// 				// fall through
// 			case 3:
// 				vector[offset++] = (value & 4) == 0;
// 				// fall through
// 			case 2:
// 				vector[offset++] = (value & 2) == 0;
// 				// fall through
// 			case 1:
// 				vector[offset++] = (value & 1) == 0;
// 		}
// 		data <<= count;
// 		bitsInData -= count;

// 		if (count == batchSize) {
// 			return unsetCount;
// 		}
// 	}

// 	// the middle part
// 	while (offset < batchSize - 7) {
// 		byte value = byteStream.next();
// 		unsetCount += (8 - Integer.bitCount(value & 0xFF));
// 		vector[offset + 0] = (value & 128) == 0;
// 		vector[offset + 1] = (value & 64) == 0;
// 		vector[offset + 2] = (value & 32) == 0;
// 		vector[offset + 3] = (value & 16) == 0;
// 		vector[offset + 4] = (value & 8) == 0;
// 		vector[offset + 5] = (value & 4) == 0;
// 		vector[offset + 6] = (value & 2) == 0;
// 		vector[offset + 7] = (value & 1) == 0;
// 		offset += 8;
// 	}

// 	// the tail
// 	int remaining = batchSize - offset;
// 	if (remaining > 0) {
// 		byte data = byteStream.next();
// 		int value = (data & 0xff) >> (8 - remaining);
// 		unsetCount += (remaining - Integer.bitCount(value));
// 		switch (remaining) {
// 			case 7:
// 				vector[offset++] = (value & 64) == 0;
// 				// fall through
// 			case 6:
// 				vector[offset++] = (value & 32) == 0;
// 				// fall through
// 			case 5:
// 				vector[offset++] = (value & 16) == 0;
// 				// fall through
// 			case 4:
// 				vector[offset++] = (value & 8) == 0;
// 				// fall through
// 			case 3:
// 				vector[offset++] = (value & 4) == 0;
// 				// fall through
// 			case 2:
// 				vector[offset++] = (value & 2) == 0;
// 				// fall through
// 			case 1:
// 				vector[offset++] = (value & 1) == 0;
// 		}
// 		this.data = (byte) (data << remaining);
// 		bitsInData = 8 - remaining;
// 	}

// 	return unsetCount;
// }

func (bi *BooleanInputStream) GetUnsetBits(batchSize int32, vector []bool) int32 {
	unsetCount := util.INT32_ZERO
	offset := util.INT32_ZERO

	// handle the head
	count := maths.MinInt32(batchSize, bi.bitsInData)
	if count != 0 {
		value := uint8(bi.data&0xFF) >> (8 - count)
		unsetCount += (count - bitCount(value))
		switch count {
		case 7:
			vector[offset] = (value & 64) == 0
			offset++
			// fall through
		case 6:
			vector[offset] = (value & 32) == 0
			offset++
			// fall through
		case 5:
			vector[offset] = (value & 16) == 0
			offset++
			// fall through
		case 4:
			vector[offset] = (value & 8) == 0
			offset++
			// fall through
		case 3:
			vector[offset] = (value & 4) == 0
			offset++
			// fall through
		case 2:
			vector[offset] = (value & 2) == 0
			offset++
			// fall through
		case 1:
			vector[offset] = (value & 1) == 0
			offset++
		}
		bi.data <<= count
		bi.bitsInData -= count

		if count == batchSize {
			return unsetCount
		}
	}

	// the middle part
	for ; offset < batchSize-7; offset += 8 {
		value := bi.byteStream.Next()
		unsetCount += (8 - bitCount(value&0xFF))
		vector[offset+0] = (value & 128) == 0
		vector[offset+1] = (value & 64) == 0
		vector[offset+2] = (value & 32) == 0
		vector[offset+3] = (value & 16) == 0
		vector[offset+4] = (value & 8) == 0
		vector[offset+5] = (value & 4) == 0
		vector[offset+6] = (value & 2) == 0
		vector[offset+7] = (value & 1) == 0

	}

	// the tail
	remaining := batchSize - offset
	if remaining > 0 {
		data := bi.byteStream.Next()
		value := (data & 0xff) >> (8 - remaining)
		unsetCount += (remaining - bitCount(value))
		switch remaining {
		case 7:
			vector[offset] = (value & 64) == 0
			offset++
			// fall through
		case 6:
			vector[offset] = (value & 32) == 0
			offset++
			// fall through
		case 5:
			vector[offset] = (value & 16) == 0
			offset++
			// fall through
		case 4:
			vector[offset] = (value & 8) == 0
			offset++
			// fall through
		case 3:
			vector[offset] = (value & 4) == 0
			offset++
			// fall through
		case 2:
			vector[offset] = (value & 2) == 0
			offset++
			// fall through
		case 1:
			vector[offset] = (value & 1) == 0
			offset++
		}
		bi.data = byte(data << remaining)
		bi.bitsInData = 8 - remaining
	}

	return unsetCount
}

// private static int bitCount(byte data)
// {
// 	return Integer.bitCount(data & 0xFF);
// }

func bitCount(data byte) int32 {
	i := int32(data & 0xFF)
	i = i - int32((uint32(i)>>1)&0x55555555)
	i = (i & 0x33333333) + int32((uint32(i)>>2)&0x33333333)
	i = (i + int32(uint32(i)>>4)) & 0x0f0f0f0f
	i = i + int32(uint32(i)>>8)
	i = i + int32(uint32(i)>>16)
	return i & 0x3f
}
