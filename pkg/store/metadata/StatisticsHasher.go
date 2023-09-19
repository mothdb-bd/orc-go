package metadata

import (
	"encoding/binary"
	"hash"
	"math"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
	"github.com/shopspring/decimal"
)

const (
	TRUE  = 0x01
	FALSE = 0x00
)

type StatisticsHasher struct {
	hasher hash.Hash
}

func NewStatisticsHasher(hasher hash.Hash) *StatisticsHasher {
	sh := new(StatisticsHasher)
	sh.hasher = hasher
	return sh
}

func makeByte(size int32) []byte {
	return make([]byte, size)
}

func PutBool(val bool, b []byte, i int) {
	if val {
		b[i] = TRUE
	} else {
		b[i] = FALSE
	}
}

func (sr *StatisticsHasher) PutInt(value int32) *StatisticsHasher {
	b := makeByte(util.INT32_BYTES)
	binary.LittleEndian.PutUint32(b, uint32(value))
	sr.hasher.Write(b)
	return sr
}

func (sr *StatisticsHasher) PutOptionalInt(present bool, value int32) *StatisticsHasher {
	b := makeByte(util.INT32_BYTES + 1)
	PutBool(present, b, 0)
	if present {
		binary.LittleEndian.PutUint32(b[1:], uint32(value))
	} else {
		binary.LittleEndian.PutUint32(b[1:], 0)
	}
	sr.hasher.Write(b)
	return sr
}

func (sr *StatisticsHasher) PutLong(value int64) *StatisticsHasher {
	b := makeByte(util.INT64_BYTES)
	binary.LittleEndian.PutUint64(b, uint64(value))
	sr.hasher.Write(b)
	return sr
}

func (sr *StatisticsHasher) PutOptionalLong(present bool, value int64) *StatisticsHasher {
	b := makeByte(util.INT64_BYTES + 1)
	PutBool(present, b, 0)
	if present {
		binary.LittleEndian.PutUint64(b[1:], uint64(value))
	} else {
		binary.LittleEndian.PutUint64(b[1:], 0)
	}
	sr.hasher.Write(b)
	return sr
}

func (sr *StatisticsHasher) PutOptionalDouble(present bool, value float64) *StatisticsHasher {
	b := makeByte(util.INT64_BYTES)
	binary.LittleEndian.PutUint64(b, math.Float64bits(value))
	sr.hasher.Write(b)
	return sr
}

func (sr *StatisticsHasher) PutOptionalHashable(value Hashable) *StatisticsHasher {
	if value != nil {
		sr.hasher.Write([]byte{TRUE})
	} else {
		sr.hasher.Write([]byte{FALSE})
	}
	value.AddHash(sr)
	return sr
}

func (sr *StatisticsHasher) PutOptionalSlice(value *slice.Slice) *StatisticsHasher {
	if value != nil {
		sr.hasher.Write([]byte{TRUE})
		sr.hasher.Write(value.AvailableBytes())
	} else {
		sr.hasher.Write([]byte{FALSE})
	}
	return sr
}

func (sr *StatisticsHasher) PutOptionalBigDecimal(value *decimal.Decimal) *StatisticsHasher {

	if value != nil {
		sr.hasher.Write([]byte{TRUE})

		b := makeByte(util.INT32_BYTES)
		binary.LittleEndian.PutUint32(b, uint32(value.Exponent()))
		sr.hasher.Write(b)
		// hasher.putBytes(value.unscaledValue().toByteArray())
		sr.hasher.Write(value.BigInt().Bytes())
	} else {
		sr.hasher.Write([]byte{FALSE})
	}
	return sr
}

func (sr *StatisticsHasher) Hash() int64 {
	b := sr.hasher.Sum(util.EMPTY)
	return padToLong(b)
}

func (sr *StatisticsHasher) PutLongs(array []int64) {

	b := makeByte(util.INT32_BYTES)
	binary.LittleEndian.PutUint32(b, uint32(len(array)))
	sr.hasher.Write(b)

	for _, entry := range array {
		b := makeByte(util.INT64_BYTES)
		binary.LittleEndian.PutUint64(b, uint64(entry))
		sr.hasher.Write(b)
	}
}

type Hashable interface {
	AddHash(hasher *StatisticsHasher)
}

func padToLong(bytes []byte) int64 {
	retVal := int64(bytes[0] & 0xFF)
	for i := util.INT64_ZERO; i < maths.Min(util.LensInt64(bytes), 8); i++ {
		retVal |= int64(bytes[i]&0xFF) << (i * 8)
	}
	return retVal
}
