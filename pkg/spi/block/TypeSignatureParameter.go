package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/optional"
)

type TypeSignatureParameter struct {
	kind  ParameterKind
	value basic.Object
}

func TSP_TypeParameter(typeSignature *TypeSignature) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_TYPE, typeSignature)
}

func NumericParameter(longLiteral int64) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_LONG, longLiteral)
}

func NamedTypeParameter(namedTypeSignature *NamedTypeSignature) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_NAMED_TYPE, namedTypeSignature)
}

func NamedField(name string, ts *TypeSignature) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_NAMED_TYPE, NewNamedTypeSignature(optional.Of(NewRowFieldName(name)), ts))
}

func AnonymousField(ts *TypeSignature) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_NAMED_TYPE, NewNamedTypeSignature(optional.Empty[*RowFieldName](), ts))
}

func TypeVariable(variable string) *TypeSignatureParameter {
	return NewTypeSignatureParameter(PK_VARIABLE, variable)
}
func NewTypeSignatureParameter(kind ParameterKind, value basic.Object) *TypeSignatureParameter {
	tr := new(TypeSignatureParameter)
	tr.kind = kind
	tr.value = value
	return tr
}

func (tr *TypeSignatureParameter) GetKind() ParameterKind {
	return tr.kind
}

func (tr *TypeSignatureParameter) IsTypeSignature() bool {
	return tr.kind == PK_TYPE
}

func (tr *TypeSignatureParameter) IsLongLiteral() bool {
	return tr.kind == PK_LONG
}

func (tr *TypeSignatureParameter) IsNamedTypeSignature() bool {
	return tr.kind == PK_NAMED_TYPE
}

func (tr *TypeSignatureParameter) IsVariable() bool {
	return tr.kind == PK_VARIABLE
}

func (tr *TypeSignatureParameter) GetTypeSignature() *TypeSignature {
	if tr.kind != PK_TYPE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_TYPE))
	}
	return tr.value.(*TypeSignature)
}

func (tr *TypeSignatureParameter) GetLongLiteral() int64 {
	if tr.kind != PK_LONG {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_LONG))
	}
	return tr.value.(int64)
}

func (tr *TypeSignatureParameter) GetNamedTypeSignature() *NamedTypeSignature {
	if tr.kind != PK_NAMED_TYPE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_NAMED_TYPE))
	}
	return tr.value.(*NamedTypeSignature)
}

func (tr *TypeSignatureParameter) GetVariable() string {
	if tr.kind != PK_VARIABLE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_VARIABLE))
	}
	return tr.value.(string)
}

func (tr *TypeSignatureParameter) GetTypeSignatureOrNamedTypeSignature() *optional.Optional[*TypeSignature] {
	switch tr.kind {
	case PK_TYPE:
		return optional.Of(tr.GetTypeSignature())
	case PK_NAMED_TYPE:
		return optional.Of(tr.GetNamedTypeSignature().GetTypeSignature())
	default:
		return optional.Empty[*TypeSignature]()
	}
}

func (tr *TypeSignatureParameter) IsCalculated() bool {
	switch tr.kind {
	case PK_TYPE:
		return tr.GetTypeSignature().IsCalculated()
	case PK_NAMED_TYPE:
		return tr.GetNamedTypeSignature().GetTypeSignature().IsCalculated()
	case PK_LONG:
		return false
	case PK_VARIABLE:
		return true
	}
	panic(fmt.Sprintf("Unexpected parameter kind: %d", tr.kind))
}
