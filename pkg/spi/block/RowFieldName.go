package block

type RowFieldName struct {
	name string
}

func NewRowFieldName(name string) *RowFieldName {
	re := new(RowFieldName)
	re.name = name
	return re
}

func (re *RowFieldName) GetName() string {
	return re.name
}
