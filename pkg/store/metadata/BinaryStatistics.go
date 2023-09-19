package metadata

import "github.com/mothdb-bd/orc-go/pkg/util"

var (
	BINARY_VALUE_BYTES_OVERHEAD int64 = util.BYTE_BYTES + util.INT32_BYTES
	BINARY_STAT_INSTANCE_SIZE   int32 = util.SizeOf(&BinaryStatistics{})
)

type BinaryStatistics struct {
	//继承
	Hashable

	sum int64
}

func NewBinaryStatistics(sum int64) *BinaryStatistics {
	bs := new(BinaryStatistics)
	bs.sum = sum
	return bs
}

func (bs *BinaryStatistics) GetSum() int64 {
	return bs.sum
}

func (bs *BinaryStatistics) GetSumPtr() *int64 {
	return &bs.sum
}

func (bs *BinaryStatistics) GetRetainedSizeInBytes() int64 {
	return int64(BINARY_STAT_INSTANCE_SIZE)
}

// @Override
func (bs *BinaryStatistics) ToString() string {
	return util.NewSB().AddInt64("sum", bs.sum).String()
}

// @Override
func (bs *BinaryStatistics) AddHash(hasher *StatisticsHasher) {
	hasher.PutLong(bs.sum)
}
