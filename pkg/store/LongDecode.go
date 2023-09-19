package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/mothio"
)

type FixedBitSizes_V1 int8

// enum FixedBitSizes_V1
//
//	{
//		ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
//		THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
//		TWENTY, TWENTY_ONE, TWENTY_TWO, TWENTY_THREE, TWENTY_FOUR, TWENTY_SIX,
//		TWENTY_EIGHT, THIRTY, THIRTY_TWO, FORTY, FORTY_EIGHT, FIFTY_SIX, SIXTY_FOUR
//	}
const (
	ONE_V1 FixedBitSizes_V1 = iota
	TWO_V1
	THREE_V1
	FOUR_V1
	FIVE_V1
	SIX_V1
	SEVEN_V1
	EIGHT_V1
	NINE_V1
	TEN_V1
	ELEVEN_V1
	TWELVE_V1

	THIRTEEN_V1
	FOURTEEN_V1
	FIFTEEN_V1
	SIXTEEN_V1
	SEVENTEEN_V1
	EIGHTEEN_V1
	NINETEEN_V1

	TWENTY_V1
	TWENTY_ONE_V1
	TWENTY_TWO_V1
	TWENTY_THREE_V1
	TWENTY_FOUR_V1
	TWENTY_SIX_V1

	TWENTY_EIGHT_V1
	THIRTY_V1
	THIRTY_TWO_V1
	FORTY_V1

	FORTY_EIGHT_V1
	FIFTY_SIX_V1
	SIXTY_FOUR_V1
)

/**
* Decodes the ordinal fixed bit value to actual fixed bit width value.
 */
func DecodeBitWidth(n FixedBitSizes_V1) int32 {
	if n >= ONE_V1 && n <= TWENTY_FOUR_V1 {
		return int32(n) + 1
	} else if n == TWENTY_SIX_V1 {
		return 26
	} else if n == TWENTY_EIGHT_V1 {
		return 28
	} else if n == THIRTY_V1 {
		return 30
	} else if n == THIRTY_TWO_V1 {
		return 32
	} else if n == FORTY_V1 {
		return 40
	} else if n == FORTY_EIGHT_V1 {
		return 48
	} else if n == FIFTY_SIX_V1 {
		return 56
	} else {
		return 64
	}
}

func ReadSignedVInt(inputStream *MothInputStream) int64 {
	result := ReadUnsignedVInt(inputStream)
	return ZigzagDecode(result)
}

func ReadUnsignedVInt(inputStream *MothInputStream) int64 {
	var result int64 = 0
	var offset int64 = 0
	b, err := inputStream.ReadBS()

	if err != nil {
		panic("EOF while reading unsigned vint")
	}
	for (b & 0b1000_0000) != 0 {
		result |= int64(b&0b0111_1111) << offset
		offset += 7
		b, err := inputStream.ReadBS()
		if err != nil {
			panic(fmt.Sprintf("EOF while reading unsigned vint %d", b))
		}
	}
	return result
}

func ReadVInt(signed bool, inputStream *MothInputStream) int64 {
	if signed {
		return ReadSignedVInt(inputStream)
	} else {
		return ReadUnsignedVInt(inputStream)
	}
}

func ZigzagDecode(value int64) int64 {
	return int64(uint64(value)>>1) ^ -(value & 1)
}

func WriteVLong(buffer mothio.DataOutput, value int64, signed bool) {
	if signed {
		value = ZigzagEncode(value)
	}
	WriteVLongUnsigned(buffer, value)
}

func WriteVLongUnsigned(output mothio.DataOutput, value int64) {
	for {
		// if there are less than 7 bits left, we are done
		if (value & 0b111_1111) == 0 {
			output.WriteByte(byte(value))
			return
		} else {
			output.WriteByte((byte)(0x80 | (value & 0x7f)))
			value >>= uint64(7)
		}
	}
}

func ZigzagEncode(value int64) int64 {
	return (value << 1) ^ (value >> 63)
}

/**
* Gets the closest supported fixed bit width for the specified bit width.
 */
func GetClosestFixedBits(width int32) int32 {
	if width == 0 {
		return 1
	}

	if width >= 1 && width <= 24 {
		return width
	} else if width > 24 && width <= 26 {
		return 26
	} else if width > 26 && width <= 28 {
		return 28
	} else if width > 28 && width <= 30 {
		return 30
	} else if width > 30 && width <= 32 {
		return 32
	} else if width > 32 && width <= 40 {
		return 40
	} else if width > 40 && width <= 48 {
		return 48
	} else if width > 48 && width <= 56 {
		return 56
	} else {
		return 64
	}
}
