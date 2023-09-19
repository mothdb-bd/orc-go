package metadata

import "github.com/mothdb-bd/orc-go/pkg/slice"

type NoOpBloomFilterBuilder struct {
	// 继承
	BloomFilterBuilder
}

func NewNoOpBloomFilterBuilder() BloomFilterBuilder {
	return new(NoOpBloomFilterBuilder)
}

// @Override
func (nr *NoOpBloomFilterBuilder) AddString(val *slice.Slice) BloomFilterBuilder {
	return nr
}

// @Override
func (nr *NoOpBloomFilterBuilder) AddLong(val int64) BloomFilterBuilder {
	return nr
}

// @Override
func (nr *NoOpBloomFilterBuilder) AddDouble(val float64) BloomFilterBuilder {
	return nr
}

// @Override
func (nr *NoOpBloomFilterBuilder) AddFloat(val float32) BloomFilterBuilder {
	return nr
}

// @Override
func (nr *NoOpBloomFilterBuilder) BuildBloomFilter() *BloomFilter {
	return nil
}
