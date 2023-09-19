package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var ROOT_COLUMN MothColumnId = NewMothColumnId(0)

type MothColumnId uint32

func NewMothColumnId(id uint32) MothColumnId {

	return MothColumnId(id)
}

func (md MothColumnId) GetId() uint32 {
	return uint32(md)
}

func (md MothColumnId) GetIdPtr() *uint32 {
	i := uint32(md)
	return &i
}

// @Override
func (md MothColumnId) Equals(o MothColumnId) bool {
	return md == o
}

// @Override
func (md MothColumnId) CompareTo(o MothColumnId) uint32 {
	return uint32(md - o)
}

// @Override
func (md MothColumnId) String() string {
	return util.NewSB().AppendString("id:").AppendUInt32(uint32(md)).String()
}
