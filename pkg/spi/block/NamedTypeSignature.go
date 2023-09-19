package block

import "github.com/mothdb-bd/orc-go/pkg/optional"

type NamedTypeSignature struct {
	fieldName     *optional.Optional[*RowFieldName]
	typeSignature *TypeSignature
}

func NewNamedTypeSignature(fieldName *optional.Optional[*RowFieldName], typeSignature *TypeSignature) *NamedTypeSignature {
	ne := new(NamedTypeSignature)
	ne.fieldName = fieldName
	ne.typeSignature = typeSignature
	return ne
}

func (ne *NamedTypeSignature) GetFieldName() *optional.Optional[*RowFieldName] {
	return ne.fieldName
}

func (ne *NamedTypeSignature) GetTypeSignature() *TypeSignature {
	return ne.typeSignature
}

func (ne *NamedTypeSignature) GetName() *optional.Optional[string] {
	fn := ne.GetFieldName()
	if fn.IsPresent() {
		return optional.Of(fn.Get().GetName())
	} else {
		return optional.Empty[string]()
	}
}
