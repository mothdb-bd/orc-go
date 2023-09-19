package common

type MothDataSourceId struct {
	id string
}

func NewMothDataSourceId(id string) *MothDataSourceId {
	md := new(MothDataSourceId)
	md.id = id
	return md
}

//@Override
func (md *MothDataSourceId) Equals(o *MothDataSourceId) bool {
	return md == o
}

//@Override
func (md *MothDataSourceId) String() string {
	return md.id
}
