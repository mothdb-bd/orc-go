package block

// ParameterKind
type ParameterKind int8

// ParameterKind
const (
	PK_TYPE ParameterKind = iota
	PK_NAMED_TYPE
	PK_LONG
	PK_VARIABLE
)
