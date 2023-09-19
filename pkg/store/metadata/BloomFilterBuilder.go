package metadata

import "github.com/mothdb-bd/orc-go/pkg/slice"

type BloomFilterBuilder interface {
	AddString(val *slice.Slice) BloomFilterBuilder

	AddLong(val int64) BloomFilterBuilder

	AddDouble(val float64) BloomFilterBuilder

	AddFloat(val float32) BloomFilterBuilder

	BuildBloomFilter() *BloomFilter
}
