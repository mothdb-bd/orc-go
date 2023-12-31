package block

var (
	POWERS_OF_TEN []int64 = []int64{
		1,
		10,
		100,
		1000,
		10_000,
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
		1_000_000_000,
		10_000_000_000,
		100_000_000_000,
		1000_000_000_000}

	TTS_MILLISECONDS_PER_SECOND      int32 = 1_000
	TTS_MILLISECONDS_PER_MINUTE      int32 = TTS_MILLISECONDS_PER_SECOND * 60
	TTS_MILLISECONDS_PER_HOUR        int32 = TTS_MILLISECONDS_PER_MINUTE * 60
	TTS_MILLISECONDS_PER_DAY         int32 = TTS_MILLISECONDS_PER_HOUR * 24
	TTS_MICROSECONDS_PER_MILLISECOND int32 = 1_000
	TTS_MICROSECONDS_PER_SECOND      int32 = 1_000_000
	TTS_MICROSECONDS_PER_DAY         int64 = 24 * 60 * 60 * 1_000_000
	TTS_NANOSECONDS_PER_MICROSECOND  int32 = 1_000
	TTS_NANOSECONDS_PER_MILLISECOND  int32 = 1_000_000
	TTS_NANOSECONDS_PER_SECOND       int64 = 1_000_000_000
	TTS_NANOSECONDS_PER_MINUTE       int64 = TTS_NANOSECONDS_PER_SECOND * 60
	TTS_NANOSECONDS_PER_DAY          int64 = TTS_NANOSECONDS_PER_MINUTE * 60 * 24
	TTS_PICOSECONDS_PER_NANOSECOND   int32 = 1_000
	TTS_PICOSECONDS_PER_MICROSECOND  int32 = 1_000_000
	TTS_PICOSECONDS_PER_MILLISECOND  int32 = 1_000_000_000
	TTS_PICOSECONDS_PER_SECOND       int64 = 1_000_000_000_000
	TTS_PICOSECONDS_PER_MINUTE       int64 = TTS_PICOSECONDS_PER_SECOND * 60
	TTS_PICOSECONDS_PER_HOUR         int64 = TTS_PICOSECONDS_PER_MINUTE * 60
	TTS_PICOSECONDS_PER_DAY          int64 = TTS_PICOSECONDS_PER_HOUR * 24
	TTS_SECONDS_PER_MINUTE           int64 = 60
	TTS_MINUTES_PER_HOUR             int64 = 60
	TTS_SECONDS_PER_DAY              int64 = TTS_SECONDS_PER_MINUTE * TTS_MINUTES_PER_HOUR * 24
)

func RoundDiv(value int64, factor int64) int64 {
	if factor <= 0 {
		panic("Factor must be > 0")
	}

	if factor == 1 {
		return value
	}

	if value >= 0 {
		return (value + (factor / 2)) / factor
	}

	return (value + 1 - (factor / 2)) / factor
}
