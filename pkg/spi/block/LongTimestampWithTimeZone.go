package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type LongTimestampWithTimeZone struct {
	epochMillis  int64
	picosOfMilli int32
	timeZoneKey  int16
}

// var LongTimestampWithTimeZoneINSTANCE_SIZE int32 = ClassLayout.parseClass(LongTimestampWithTimeZone.class).instanceSize()
var LTWT_INSTANCE_SIZE int32 = util.SizeOf(&LongTimestampWithTimeZone{})

func FromEpochSecondsAndFraction(epochSecond int64, fractionInPicos int64, timeZoneKey *TimeZoneKey) *LongTimestampWithTimeZone {
	return FromEpochMillisAndFraction2(epochSecond*1_000+fractionInPicos/1_000_000_000, int32(fractionInPicos%1_000_000_000), timeZoneKey)
}

func FromEpochMillisAndFraction2(epochMillis int64, picosOfMilli int32, timeZoneKey *TimeZoneKey) *LongTimestampWithTimeZone {
	return NewLongTimestampWithTimeZone(epochMillis, picosOfMilli, timeZoneKey.GetKey())
}

func FromEpochMillisAndFraction(epochMillis int64, picosOfMilli int32, timeZoneKey int16) *LongTimestampWithTimeZone {
	return NewLongTimestampWithTimeZone(epochMillis, picosOfMilli, timeZoneKey)
}
func NewLongTimestampWithTimeZone(epochMillis int64, picosOfMilli int32, timeZoneKey int16) *LongTimestampWithTimeZone {
	le := new(LongTimestampWithTimeZone)
	if picosOfMilli < 0 {
		panic("picosOfMilli must be >= 0")
	}
	if picosOfMilli >= TTS_PICOSECONDS_PER_MILLISECOND {
		panic(fmt.Sprintf("picosOfMilli must be < %d", TTS_PICOSECONDS_PER_MILLISECOND))
	}
	le.epochMillis = epochMillis
	le.picosOfMilli = picosOfMilli
	le.timeZoneKey = timeZoneKey
	return le
}

func (le *LongTimestampWithTimeZone) GetEpochMillis() int64 {
	return le.epochMillis
}

func (le *LongTimestampWithTimeZone) GetPicosOfMilli() int32 {
	return le.picosOfMilli
}

func (le *LongTimestampWithTimeZone) GetTimeZoneKey() int16 {
	return le.timeZoneKey
}

// @Override
func (le *LongTimestampWithTimeZone) CompareTo(other *LongTimestampWithTimeZone) int {

	value := maths.Compare(le.epochMillis, other.epochMillis)
	if value != 0 {
		return value
	}
	return maths.CompareInt32(le.picosOfMilli, other.picosOfMilli)
}
