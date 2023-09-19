package block

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	IMESTAMP_WITH_TIME_ZONE    string = "timestamp with time zone"
	IMESTAMP_WITHOUT_TIME_ZONE string = "timestamp without time zone"
)

type TypeSignature struct {
	base       string
	parameters *util.ArrayList[*TypeSignatureParameter]
	calculated bool
	hashCode   int32
}

func NewTypeSignature(base string, parameters ...*TypeSignatureParameter) *TypeSignature {
	return NewTypeSignature2(base, util.NewArrayList(parameters...))
}
func NewTypeSignature2(base string, parameters *util.ArrayList[*TypeSignatureParameter]) *TypeSignature {
	te := new(TypeSignature)
	te.base = base
	ts_checkArgument(parameters != nil, "parameters is null")
	te.parameters = parameters

	calculated := false
	for i := 0; i < parameters.Size(); i++ {
		parm := parameters.Get(i)
		if parm.IsCalculated() {
			calculated = true
			break
		}
	}
	te.calculated = calculated
	return te
}

func (te *TypeSignature) GetBase() string {
	return te.base
}

// public List<TypeSignatureParameter> getParameters()
func (te *TypeSignature) GetParameters() *util.ArrayList[*TypeSignatureParameter] {
	return te.parameters
}

func (te *TypeSignature) GetTypeParametersAsTypeSignatures() *util.ArrayList[*TypeSignature] {
	result := util.NewArrayList[*TypeSignature]()
	for i := 0; i < te.parameters.Size(); i++ {
		parm := te.parameters.Get(i)
		if parm.GetKind() != PK_TYPE {
			panic(fmt.Sprintf("Expected all parameters to be TypeSignatures but [%d] was found", parm))
		}
		result.Add(parm.GetTypeSignature())
	}
	return result
}

func (te *TypeSignature) IsCalculated() bool {
	return te.calculated
}

func (te *TypeSignature) ToString() string {
	return te.base
}

func ts_checkArgument(argument bool, format string, args ...*basic.Object) {
	if !argument {
		panic(fmt.Sprintf(format, args))
	}
}

func Sig_ArrayType(elementType *TypeSignature) *TypeSignature {
	return NewTypeSignature(ST_ARRAY, TSP_TypeParameter(elementType))
}

func Sig_ArrayType2(elementType *TypeSignatureParameter) *TypeSignature {
	return NewTypeSignature(ST_ARRAY, elementType)
}

func MapTypeValue(keyType *TypeSignature, valueType *TypeSignature) *TypeSignature {
	return NewTypeSignature(ST_MAP, TSP_TypeParameter(keyType), TSP_TypeParameter(valueType))
}

func ParametricType(name string, parameters ...*TypeSignature) *TypeSignature {

	l := util.NewArrayList[*TypeSignatureParameter]()
	for _, p := range parameters {
		l.Add(TSP_TypeParameter(p))
	}
	// return NewTypeSignature(name, Arrays.stream(parameters).map(TypeSignatureParameter.typeParameter).collect(Lists.toUnmodifiableList()))
	return NewTypeSignature2(name, l)
}

func FunctionType(first *TypeSignature, rest ...*TypeSignature) *TypeSignature {
	parameters := util.NewArrayList[*TypeSignatureParameter]()
	parameters.Add(TSP_TypeParameter(first))

	for _, p := range rest {
		parameters.Add(TSP_TypeParameter(p))
	}

	// Arrays.stream(rest).map(TypeSignatureParameter.typeParameter).forEach(parameters.add)
	return NewTypeSignature2("function", parameters)
}

func F_RowType(fields ...*TypeSignatureParameter) *TypeSignature {
	return F_RowType2(util.NewArrayList(fields...))
}

func F_RowType2(fields *util.ArrayList[*TypeSignatureParameter]) *TypeSignature {

	return NewTypeSignature2(ST_ROW, fields)
}
