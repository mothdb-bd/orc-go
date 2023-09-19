package metadata

import (
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type ColumnEncodingKind int8

const (
	DIRECT ColumnEncodingKind = iota
	DICTIONARY
	DIRECT_V2
	DICTIONARY_V2
)

type ColumnEncoding struct {
	columnEncodingKind ColumnEncodingKind
	dictionarySize     uint32
}

func NewColumnEncoding(columnEncodingKind ColumnEncodingKind, dictionarySize uint32) *ColumnEncoding {
	cg := new(ColumnEncoding)
	cg.columnEncodingKind = columnEncodingKind
	cg.dictionarySize = dictionarySize
	return cg
}

func (cg *ColumnEncoding) GetColumnEncodingKind() ColumnEncodingKind {
	return cg.columnEncodingKind
}

func (cg *ColumnEncoding) GetDictionarySize() uint32 {
	return cg.dictionarySize
}

func (cg *ColumnEncoding) GetDictionarySizePtr() *uint32 {
	return &cg.dictionarySize
}

func (cg *ColumnEncoding) String() string {
	sb := util.NewSB()
	sb.AppendString("columnEncodingKind").AppendInt8(int8(cg.columnEncodingKind)).AppendString(",dictionarySize").AppendUInt32(cg.dictionarySize)
	return sb.String()
}
