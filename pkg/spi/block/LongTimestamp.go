package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LT_INSTANCE_SIZE            int32 = util.SizeOf(&LongTimestamp{})
	PICOSECONDS_PER_MICROSECOND int32 = 1_000_000
)

type LongTimestamp struct {
	epochMicros  int64
	picosOfMicro int32
}

func NewLongTimestamp(epochMicros int64, picosOfMicro int32) *LongTimestamp {
	lp := new(LongTimestamp)
	if picosOfMicro < 0 {
		panic(fmt.Sprintf("picosOfMicro must be >= 0: %d", picosOfMicro))
	}
	if picosOfMicro >= PICOSECONDS_PER_MICROSECOND {
		panic(fmt.Sprintf("picosOfMicro must be < 1_000_000: %d", picosOfMicro))
	}
	lp.epochMicros = epochMicros
	lp.picosOfMicro = picosOfMicro
	return lp
}

func (lp *LongTimestamp) GetEpochMicros() int64 {
	return lp.epochMicros
}

func (lp *LongTimestamp) GetPicosOfMicro() int32 {
	return lp.picosOfMicro
}

// @Override
func (lp *LongTimestamp) CompareTo(other *LongTimestamp) int {
	value := maths.Compare(lp.epochMicros, other.epochMicros)
	if value != 0 {
		return value
	}
	return maths.CompareInt32(lp.picosOfMicro, other.picosOfMicro)
}
