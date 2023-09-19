package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/memory"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

func CreateColumnReader(kind block.Type, column *MothColumn, projectedLayout ProjectedLayout, memoryContext memory.AggregatedMemoryContext, blockFactory *MothBlockFactory, fieldMapperFactory FieldMapperFactory) ColumnReader {

	_, flag := kind.(*block.TimeType)
	if flag {
		if !kind.Equals(block.TIME_MICROS) || column.GetColumnType() != metadata.LONG || "TIME" != (column.GetAttributes()["iceberg.long-type"]) {
			panic(fmt.Sprintf("Cannot read SQL type '%s' from MOTH stream '%s' of type %d with attributes %s", kind, column.GetPath(), column.GetColumnType(), column.GetAttributes()))
		}
		return NewTimeColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	}
	switch column.GetColumnType() {
	case metadata.BOOLEAN:
		return NewBooleanColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.BYTE:
		return NewByteColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.SHORT, metadata.INT, metadata.LONG, metadata.DATE:
		return NewLongColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.FLOAT:
		return NewFloatColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.DOUBLE:
		return NewDoubleColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.BINARY, metadata.STRING, metadata.VARCHAR, metadata.CHAR:
		return NewSliceColumnReader(kind, column, memoryContext)
	case metadata.TIMESTAMP, metadata.TIMESTAMP_INSTANT:
		return NewTimestampColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.LIST:
		return NewListColumnReader(kind, column, memoryContext, blockFactory, fieldMapperFactory)
	case metadata.STRUCT:
		return NewStructColumnReader(kind, column, projectedLayout, memoryContext, blockFactory, fieldMapperFactory)
	case metadata.MAP:
		return NewMapColumnReader(kind, column, memoryContext, blockFactory, fieldMapperFactory)
	case metadata.DECIMAL:
		return NewDecimalColumnReader(kind, column, memoryContext.NewLocalMemoryContext("ColumnReaders"))
	case metadata.UNION:
		return NewUnionColumnReader(kind, column, memoryContext, blockFactory, fieldMapperFactory)
	}
	panic(fmt.Sprintf("Unsupported type: %d", column.GetColumnType()))
}
