package store

import (
	"sort"

	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

func MergeAdjacentDiskRanges(diskRanges *util.ArrayList[*DiskRange], maxMergeDistance util.DataSize, maxReadSize util.DataSize) *util.ArrayList[*DiskRange] {
	ranges := util.NewCmpList[*DiskRange](NewDiskRangeCmp())
	ranges.AddAll(diskRanges)
	sort.Sort(ranges)

	maxReadSizeBytes := maxReadSize.Bytes()
	maxMergeDistanceBytes := maxMergeDistance.Bytes()
	result := util.NewArrayList[*DiskRange]()
	last := ranges.Get(0)
	for i := 1; i < ranges.Size(); i++ {
		current := ranges.Get(i)
		merged := last.Span(current)
		if uint64(merged.GetLength()) <= maxReadSizeBytes && last.GetEnd()+int64(maxMergeDistanceBytes) >= current.GetOffset() {
			last = merged
		} else {
			result.Add(last)
			last = current
		}
	}
	result.Add(last)
	return result
}

func GetDiskRangeSlice(diskRange *DiskRange, buffers map[*DiskRange]*slice.Slice) *slice.Slice {
	for k, v := range buffers {
		// bufferRange := bufferEntry.getKey()
		// buffer := bufferEntry.getValue()
		if k.Contains(diskRange) {
			offset := util.Int32Exact(diskRange.GetOffset() - k.GetOffset())
			s, _ := v.MakeSlice(int(offset), int(diskRange.GetLength()))
			return s
		}
	}
	panic("No matching buffer for disk range")
}
