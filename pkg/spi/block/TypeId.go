package block

type TypeId struct {
	id string
}

func NewTypeId(id string) *TypeId {
	td := new(TypeId)
	td.id = id
	return td
}

//@JsonCreator
func OfTypeId(id string) *TypeId {
	return NewTypeId(id)
}

//@JsonValue
func (td *TypeId) GetId() string {
	return td.id
}

//@Override
func (td *TypeId) ToString() string {
	return "type:[" + td.GetId() + "]"
}
