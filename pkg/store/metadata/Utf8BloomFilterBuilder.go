package metadata

import "github.com/mothdb-bd/orc-go/pkg/slice"

type Utf8BloomFilterBuilder struct {
	//继承
	BloomFilterBuilder

	bloomFilter *BloomFilter
}

func NewUtf8BloomFilterBuilder(expectedSize int32, fpp float64) *Utf8BloomFilterBuilder {
	ur := new(Utf8BloomFilterBuilder)
	ur.bloomFilter = NewBloomFilter(int64(expectedSize), fpp)
	return ur
}

// @Override
func (ur *Utf8BloomFilterBuilder) AddString(val *slice.Slice) BloomFilterBuilder {
	ur.bloomFilter.Add2(val)
	return ur
}

// @Override
func (ur *Utf8BloomFilterBuilder) AddLong(val int64) BloomFilterBuilder {
	ur.bloomFilter.AddLong(val)
	return ur
}

// @Override
func (ur *Utf8BloomFilterBuilder) AddDouble(val float64) BloomFilterBuilder {
	ur.bloomFilter.AddDouble(val)
	return ur
}

// @Override
func (ur *Utf8BloomFilterBuilder) AddFloat(val float32) BloomFilterBuilder {
	ur.bloomFilter.AddFloat(val)
	return ur
}

// @Override
func (ur *Utf8BloomFilterBuilder) BuildBloomFilter() *BloomFilter {
	return ur.bloomFilter
}
