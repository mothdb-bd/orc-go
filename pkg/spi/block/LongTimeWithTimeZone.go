package block

import "github.com/mothdb-bd/orc-go/pkg/maths"

type LongTimeWithTimeZone struct {
	picoseconds   int64
	offsetMinutes int32
}

func NewLongTimeWithTimeZone(picoseconds int64, offsetMinutes int32) *LongTimeWithTimeZone {
	le := new(LongTimeWithTimeZone)
	le.picoseconds = picoseconds
	le.offsetMinutes = offsetMinutes
	return le
}

func (le *LongTimeWithTimeZone) GetPicoseconds() int64 {
	return le.picoseconds
}

func (le *LongTimeWithTimeZone) GetOffsetMinutes() int32 {
	return le.offsetMinutes
}

// @Override
func (le *LongTimeWithTimeZone) CompareTo(other *LongTimeWithTimeZone) int {
	return maths.Compare(normalize(le), normalize(other))
}
