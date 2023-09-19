package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
)

type TypeParameter struct {
	kind  ParameterKind
	value basic.Object
}

func NewTypeParameter(kind ParameterKind, value basic.Object) *TypeParameter {
	tr := new(TypeParameter)
	tr.kind = kind
	tr.value = value
	return tr
}

func OfType(ts Type) *TypeParameter {
	return NewTypeParameter(PK_TYPE, ts)
}

func OfLong(longLiteral int64) *TypeParameter {
	return NewTypeParameter(PK_LONG, longLiteral)
}

func OfNameType(namedType *NamedType) *TypeParameter {
	return NewTypeParameter(PK_NAMED_TYPE, namedType)
}

func OfVariable(variable string) *TypeParameter {
	return NewTypeParameter(PK_VARIABLE, variable)
}

func Of5(parameter *TypeSignatureParameter, typeManager TypeManager) *TypeParameter {
	switch parameter.GetKind() {
	case PK_TYPE:
		{
			kind := typeManager.GetType(parameter.GetTypeSignature())
			return OfType(kind)
		}
	case PK_LONG:
		return OfLong(parameter.GetLongLiteral())
	case PK_NAMED_TYPE:
		{
			kind := typeManager.GetType(parameter.GetNamedTypeSignature().GetTypeSignature())
			return OfNameType(NewNamedType(parameter.GetNamedTypeSignature().GetFieldName(), kind))
		}
	case PK_VARIABLE:
		return OfVariable(parameter.GetVariable())
	}
	panic(fmt.Sprintf("Unsupported parameter [%d]", parameter))
}

func (tr *TypeParameter) GetKind() ParameterKind {
	return tr.kind
}

func (tr *TypeParameter) IsLongLiteral() bool {
	return tr.kind == PK_LONG
}

func (tr *TypeParameter) GetType() Type {
	if tr.kind != PK_TYPE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_TYPE))
	}
	return tr.value.(Type)
}

func (tr *TypeParameter) GetLongLiteral() int64 {
	if tr.kind != PK_LONG {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_LONG))
	}
	return tr.value.(int64)
}

func (tr *TypeParameter) GetNamedType() *NamedType {
	if tr.kind != PK_NAMED_TYPE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_NAMED_TYPE))
	}
	return tr.value.(*NamedType)
}

func (tr *TypeParameter) GetVariable() string {
	if tr.kind != PK_VARIABLE {
		panic(fmt.Sprintf("ParameterKind is [%d] but expected [%d]", tr.kind, PK_VARIABLE))
	}
	return tr.value.(string)
}
