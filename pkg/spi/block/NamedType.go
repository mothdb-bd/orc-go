package block

import "github.com/mothdb-bd/orc-go/pkg/optional"

type NamedType struct {
	// private final Optional<RowFieldName> name;
	name *optional.Optional[*RowFieldName]
	kind Type
}

func NewNamedType(name *optional.Optional[*RowFieldName], kind Type) *NamedType {
	ne := new(NamedType)
	ne.name = name
	ne.kind = kind
	return ne
}

func (ne *NamedType) GetName() *optional.Optional[*RowFieldName] {
	return ne.name
}

func (ne *NamedType) GetType() Type {
	return ne.kind
}
