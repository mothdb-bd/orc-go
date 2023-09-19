package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

type DataSupplier[T basic.Object] struct {
	data T
}

func (s *DataSupplier[T]) Get() T {
	return s.data
}

func NewDataSupplier[T basic.Object](data T) *DataSupplier[T] {
	return &DataSupplier[T]{data: data}
}

func CreateColumnWriter(columnId metadata.MothColumnId, mothTypes *metadata.ColumnMetadata[*metadata.MothType], kind block.Type, compression metadata.CompressionKind, bufferSize int32, stringStatisticsLimit util.DataSize, bloomFilterBuilder func() metadata.BloomFilterBuilder) ColumnWriter {
	mothType := mothTypes.Get(columnId)

	_, flag := kind.(*block.TimeType)
	if flag {
		var tmp metadata.LongValueStatisticsBuilder = metadata.NewIntegerStatisticsBuilder(bloomFilterBuilder())
		return NewTimeColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	}
	switch mothType.GetMothTypeKind() {
	case metadata.BOOLEAN:
		return NewBooleanColumnWriter(columnId, kind, compression, bufferSize)
	case metadata.FLOAT:
		tmp := metadata.NewDoubleStatisticsBuilder(bloomFilterBuilder())
		return NewFloatColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.DOUBLE:
		tmp := metadata.NewDoubleStatisticsBuilder(bloomFilterBuilder())
		return NewDoubleColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.BYTE:
		return NewByteColumnWriter(columnId, kind, compression, bufferSize)
	case metadata.DATE:
		var tmp metadata.LongValueStatisticsBuilder = metadata.NewDateStatisticsBuilder(bloomFilterBuilder())
		return NewLongColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.SHORT, metadata.INT, metadata.LONG:
		var tmp metadata.LongValueStatisticsBuilder = metadata.NewIntegerStatisticsBuilder(bloomFilterBuilder())
		return NewLongColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.DECIMAL:
		return NewDecimalColumnWriter(columnId, kind, compression, bufferSize)
	case metadata.TIMESTAMP, metadata.TIMESTAMP_INSTANT:
		tmp := metadata.NewTimestampStatisticsBuilder(bloomFilterBuilder())
		return NewTimestampColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.BINARY:
		var tmp metadata.SliceColumnStatisticsBuilder = metadata.NewBinaryStatisticsBuilder()
		return NewSliceDirectColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.CHAR, metadata.VARCHAR, metadata.STRING:
		var tmp metadata.SliceColumnStatisticsBuilder = metadata.NewStringStatisticsBuilder(util.Int32Exact(int64(stringStatisticsLimit.Bytes())), bloomFilterBuilder())
		return NewSliceDictionaryColumnWriter(columnId, kind, compression, bufferSize, NewDataSupplier(tmp))
	case metadata.LIST:
		{
			fieldColumnIndex := mothType.GetFieldTypeIndex(0)
			fieldType := kind.GetTypeParameters().Get(0)
			elementWriter := CreateColumnWriter(fieldColumnIndex, mothTypes, fieldType, compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder)
			return NewListColumnWriter(columnId, compression, bufferSize, elementWriter)
		}
	case metadata.MAP:
		{
			keyWriter := CreateColumnWriter(mothType.GetFieldTypeIndex(0), mothTypes, kind.GetTypeParameters().Get(0), compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder)
			valueWriter := CreateColumnWriter(mothType.GetFieldTypeIndex(1), mothTypes, kind.GetTypeParameters().Get(1), compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder)
			return NewMapColumnWriter(columnId, compression, bufferSize, keyWriter, valueWriter)
		}
	case metadata.STRUCT:
		{
			fieldWriters := util.NewArrayList[ColumnWriter]()
			for fieldId := util.INT32_ZERO; fieldId < mothType.GetFieldCount(); fieldId++ {
				fieldColumnIndex := mothType.GetFieldTypeIndex(fieldId)
				fieldType := kind.GetTypeParameters().GetByInt32(fieldId)
				fieldWriters.Add(CreateColumnWriter(fieldColumnIndex, mothTypes, fieldType, compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder))
			}
			return NewStructColumnWriter(columnId, compression, bufferSize, fieldWriters)
		}
	case metadata.UNION:
		// unsupported
	}
	panic(fmt.Sprintf("Unsupported kind: %s ,MothTypeKind %d", kind.GetDisplayName(), mothType.GetMothTypeKind()))
}
