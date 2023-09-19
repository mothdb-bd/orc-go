package store

import "github.com/mothdb-bd/orc-go/pkg/util"

var RowGroup_CMP *rowGroupCmp = newRowGroupCmp()

type RowGroup struct {
	groupId            int32
	rowOffset          int64
	rowCount           int64
	minAverageRowBytes int64
	streamSources      *InputStreamSources
}

func NewRowGroup(groupId int32, rowOffset int64, rowCount int64, minAverageRowBytes int64, streamSources *InputStreamSources) *RowGroup {
	rp := new(RowGroup)
	rp.groupId = groupId
	rp.rowOffset = rowOffset
	rp.rowCount = rowCount
	rp.minAverageRowBytes = minAverageRowBytes
	rp.streamSources = streamSources
	return rp
}

func (rp *RowGroup) GetGroupId() int32 {
	return rp.groupId
}

func (rp *RowGroup) GetRowOffset() int64 {
	return rp.rowOffset
}

func (rp *RowGroup) GetRowCount() int64 {
	return rp.rowCount
}

func (rp *RowGroup) GetMinAverageRowBytes() int64 {
	return rp.minAverageRowBytes
}

func (rp *RowGroup) GetStreamSources() *InputStreamSources {
	return rp.streamSources
}

func (rp *RowGroup) Cmp(o *RowGroup) int {
	return int(rp.rowOffset - o.rowOffset)
}

type rowGroupCmp struct {
	util.Compare[*RowGroup]
}

func newRowGroupCmp() *rowGroupCmp {
	return new(rowGroupCmp)
}

func (c *rowGroupCmp) Cmp(i, j *RowGroup) int {
	return i.Cmp(j)
}
