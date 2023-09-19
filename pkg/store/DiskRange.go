package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DiskRange struct {
	offset int64
	length int32
}

func NewDiskRange(offset int64, length int32) *DiskRange {
	de := new(DiskRange)
	de.offset = offset
	de.length = length
	return de
}

func (de *DiskRange) GetOffset() int64 {
	return de.offset
}

func (de *DiskRange) GetLength() int32 {
	return de.length
}

func (de *DiskRange) GetEnd() int64 {
	return de.offset + int64(de.length)
}

func (de *DiskRange) Contains(diskRange *DiskRange) bool {
	return de.offset <= diskRange.GetOffset() && diskRange.GetEnd() <= de.GetEnd()
}

func (de *DiskRange) Span(otherDiskRange *DiskRange) *DiskRange {
	start := maths.Min(de.offset, otherDiskRange.GetOffset())
	end := maths.Max(de.GetEnd(), otherDiskRange.GetEnd())
	l, _ := util.ToInt32Exact(end - start)
	return NewDiskRange(start, l)
}

// @Override
func (de *DiskRange) Equals(obj *DiskRange) bool {
	return de == obj
}

// @Override
func (de *DiskRange) String() string {
	return util.NewSB().AddInt64("offset", de.offset).AddInt32("length", de.length).String()
}

// disk range 比较类
type DiskRangeCmp struct {
	// 继承
	util.Compare[*DiskRange]
}

func NewDiskRangeCmp() *DiskRangeCmp {
	return new(DiskRangeCmp)
}

func (r *DiskRangeCmp) Cmp(i, j *DiskRange) int {
	return int(i.GetOffset() - j.GetOffset())
}
