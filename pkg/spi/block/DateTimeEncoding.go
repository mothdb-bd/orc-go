package block

import (
	"fmt"
)

var (
	TIME_ZONE_MASK int32 = 0xFFF
	MILLIS_SHIFT   int32 = 12
)

// private static long pack(long millisUtc, short timeZoneKey)
// {
// 	if (millisUtc << MILLIS_SHIFT >> MILLIS_SHIFT != millisUtc) {
// 		throw new IllegalArgumentException("Millis overflow: " + millisUtc);
// 	}

// 	return (millisUtc << MILLIS_SHIFT) | (timeZoneKey & TIME_ZONE_MASK);
// }

func pack(millisUtc int64, timeZoneKey int16) int64 {
	if millisUtc<<MILLIS_SHIFT>>MILLIS_SHIFT != millisUtc {
		panic(fmt.Sprintf("Millis overflow: %d", millisUtc))
	}

	return (millisUtc << int64(MILLIS_SHIFT)) | int64((int32(timeZoneKey) & TIME_ZONE_MASK))
}

// public static long packDateTimeWithZone(long millisUtc, int offsetMinutes)
// {
// 	return packDateTimeWithZone(millisUtc, getTimeZoneKeyForOffset(offsetMinutes));
// }
func PackDateTimeWithZone2(millisUtc int64, offsetMinutes int32) int64 {
	return PackDateTimeWithZone3(millisUtc, GetTimeZoneKeyForOffset(int64(offsetMinutes)))
}

// public static long packDateTimeWithZone(long millisUtc, TimeZoneKey timeZoneKey)
// {
// 	requireNonNull(timeZoneKey, "timeZoneKey is null");
// 	return pack(millisUtc, timeZoneKey.getKey());
// }
func PackDateTimeWithZone3(millisUtc int64, timeZoneKey *TimeZoneKey) int64 {
	return pack(millisUtc, timeZoneKey.GetKey())
}

// public static long packDateTimeWithZone(long millisUtc, short timeZoneKey)
// {
// 	return pack(millisUtc, timeZoneKey);
// }
func PackDateTimeWithZone4(millisUtc int64, timeZoneKey int16) int64 {
	return pack(millisUtc, timeZoneKey)
}

// public static long unpackMillisUtc(long dateTimeWithTimeZone)
// {
// 	return dateTimeWithTimeZone >> MILLIS_SHIFT;
// }
func UnpackMillisUtc(dateTimeWithTimeZone int64) int64 {
	return dateTimeWithTimeZone >> MILLIS_SHIFT
}

// public static TimeZoneKey unpackZoneKey(long dateTimeWithTimeZone)
// {
// 	return getTimeZoneKey((short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
// }
func UnpackZoneKey(dateTimeWithTimeZone int64) *TimeZoneKey {
	return GetTimeZoneKey(int16(dateTimeWithTimeZone & int64(TIME_ZONE_MASK)))
}

// public static long updateMillisUtc(long newMillsUtc, long dateTimeWithTimeZone)
// {
// 	return pack(newMillsUtc, (short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
// }
func UpdateMillisUtc(newMillsUtc int64, dateTimeWithTimeZone int64) int64 {
	return pack(newMillsUtc, int16(dateTimeWithTimeZone&int64(TIME_ZONE_MASK)))
}

// public static long packTimeWithTimeZone(long nanos, int offsetMinutes)
// {
// 	// offset is encoded as a 2s complement 11-bit number
// 	return (nanos << 11) | (offsetMinutes & 0b111_1111_1111);
// }
func PackTimeWithTimeZone(nanos int64, offsetMinutes int32) int64 {
	return (nanos << 11) | (int64(offsetMinutes) & 0b111_1111_1111)
}

// public static long unpackTimeNanos(long packedTimeWithTimeZone)
// {
// 	return packedTimeWithTimeZone >>> 11;
// }
func UnpackTimeNanos(packedTimeWithTimeZone int64) int64 {
	return int64(uint64(packedTimeWithTimeZone) >> 11)
}

// public static int unpackOffsetMinutes(long packedTimeWithTimeZone)
// {
// 	int unpacked = (int) (packedTimeWithTimeZone & 0b11_1111_1111);
// 	if ((packedTimeWithTimeZone & 0b100_0000_0000) != 0) {
// 		// extend sign up to int
// 		unpacked |= 0b1111_1111_1111_1111_1111_1100_0000_0000;
// 	}
// 	return unpacked;
// }

func UnpackOffsetMinutes(packedTimeWithTimeZone int64) int32 {
	unpacked := packedTimeWithTimeZone & 0b11_1111_1111
	if (packedTimeWithTimeZone & 0b100_0000_0000) != 0 {
		unpacked |= 0b1111_1111_1111_1111_1111_1100_0000_0000
	}
	return int32(unpacked)
}
