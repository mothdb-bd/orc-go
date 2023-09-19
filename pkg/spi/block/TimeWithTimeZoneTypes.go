package block

import "github.com/mothdb-bd/orc-go/pkg/maths"

/**
 * Normalize to offset +00:00. The calculation is done modulo 24h
 */
func normalizePicos(picos int64, offsetMinutes int32) int64 {
	return maths.FloorMod(picos-int64(offsetMinutes)*TTS_PICOSECONDS_PER_MINUTE, TTS_PICOSECONDS_PER_DAY)
}

func normalizeNanos(nanos int64, offsetMinutes int32) int64 {
	return maths.FloorMod(nanos-int64(offsetMinutes)*TTS_NANOSECONDS_PER_MINUTE, TTS_NANOSECONDS_PER_DAY)
}

func normalizePackedTime(packedTime int64) int64 {
	return normalizeNanos(UnpackTimeNanos(packedTime), UnpackOffsetMinutes(packedTime))
}

func normalize(time *LongTimeWithTimeZone) int64 {
	return maths.FloorMod(time.GetPicoseconds()-int64(time.GetOffsetMinutes())*TTS_PICOSECONDS_PER_MINUTE, TTS_PICOSECONDS_PER_DAY)
}
