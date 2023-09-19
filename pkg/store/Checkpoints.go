package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

func GetStreamCheckpoints(columns util.SetInterface[metadata.MothColumnId], columnTypes *metadata.ColumnMetadata[*metadata.MothType], compressed bool, rowGroupId int32, columnEncodings *metadata.ColumnMetadata[*metadata.ColumnEncoding], streams map[StreamId]*metadata.Stream, columnIndexes map[StreamId]*util.ArrayList[*metadata.RowGroupIndex]) map[StreamId]StreamCheckpoint {
	streamKinds := util.NewSetMap[metadata.MothColumnId, metadata.StreamKind]()
	for _, stream := range streams {
		streamKinds.Put(stream.GetColumnId(), stream.GetStreamKind())
	}
	checkpoints := make(map[StreamId]StreamCheckpoint)
	for key, value := range columnIndexes {
		columnId := key.GetColumnId()
		if !columns.Has(columnId) {
			continue
		}
		positionsList := value.GetByInt32(rowGroupId).GetPositions()
		columnEncoding := columnEncodings.Get(columnId).GetColumnEncodingKind()
		columnType := columnTypes.Get(columnId).GetMothTypeKind()
		availableStreams := streamKinds.Get(columnId)
		columnPositionsList := NewColumnPositionsList(columnId, columnType, positionsList)
		switch columnType {
		case metadata.BOOLEAN:
			util.PutAll(checkpoints, getBooleanColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList))
		case metadata.BYTE:
			util.PutAll(checkpoints, getByteColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList))
		case metadata.SHORT, metadata.INT, metadata.LONG, metadata.DATE:
			util.PutAll(checkpoints, getLongColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList))
		case metadata.FLOAT:
			util.PutAll(checkpoints, getFloatColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList))
		case metadata.DOUBLE:
			util.PutAll(checkpoints, getDoubleColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList))
		case metadata.TIMESTAMP, metadata.TIMESTAMP_INSTANT:
			util.PutAll(checkpoints, getTimestampColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList))
		case metadata.BINARY, metadata.STRING, metadata.VARCHAR, metadata.CHAR:
			util.PutAll(checkpoints, getSliceColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList))
		case metadata.LIST, metadata.MAP:
			util.PutAll(checkpoints, getListOrMapColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList))
		case metadata.STRUCT:
			util.PutAll(checkpoints, getStructColumnCheckpoints(columnId, compressed, availableStreams, columnPositionsList))
		case metadata.DECIMAL:
			util.PutAll(checkpoints, getDecimalColumnCheckpoints(columnId, columnEncoding, compressed, availableStreams, columnPositionsList))
		}
	}
	return checkpoints
}

func GetDictionaryStreamCheckpoint(streamId StreamId, columnType metadata.MothTypeKind, columnEncoding metadata.ColumnEncodingKind) StreamCheckpoint {
	if streamId.GetStreamKind() == metadata.DICTIONARY_DATA {
		switch columnType {
		case metadata.STRING, metadata.VARCHAR, metadata.CHAR, metadata.BINARY:
			return NewByteArrayStreamCheckpoint(CreateInputStreamCheckpoint2(0, 0))
		}
	}
	if streamId.GetStreamKind() == metadata.LENGTH || streamId.GetStreamKind() == metadata.DATA {
		if columnEncoding == metadata.DICTIONARY_V2 {
			return NewLongStreamV2Checkpoint(0, CreateInputStreamCheckpoint2(0, 0))
		} else if columnEncoding == metadata.DICTIONARY {
			return NewLongStreamV1Checkpoint(0, CreateInputStreamCheckpoint2(0, 0))
		}
	}
	panic(fmt.Sprintf("Unsupported column type %d for dictionary stream %s", columnType, streamId))
}

func getBooleanColumnCheckpoints(columnId metadata.MothColumnId, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	return checkpoints
}

func getByteColumnCheckpoints(columnId metadata.MothColumnId, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = NewByteStreamCheckpoint2(compressed, positionsList)
	}
	return checkpoints
}

func getLongColumnCheckpoints(columnId metadata.MothColumnId, encoding metadata.ColumnEncodingKind, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
	}
	return checkpoints
}

func getFloatColumnCheckpoints(columnId metadata.MothColumnId, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = NewFloatStreamCheckpoint2(compressed, positionsList)
	}
	return checkpoints
}

func getDoubleColumnCheckpoints(columnId metadata.MothColumnId, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = NewDoubleStreamCheckpoint2(compressed, positionsList)
	}
	return checkpoints
}

func getTimestampColumnCheckpoints(columnId metadata.MothColumnId, encoding metadata.ColumnEncodingKind, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
	}
	if availableStreams.Has(metadata.SECONDARY) {
		checkpoints[NewStreamId(columnId, metadata.SECONDARY)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
	}
	return checkpoints
}

func getSliceColumnCheckpoints(columnId metadata.MothColumnId, encoding metadata.ColumnEncodingKind, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if encoding == metadata.DIRECT || encoding == metadata.DIRECT_V2 {
		if availableStreams.Has(metadata.DATA) {
			checkpoints[NewStreamId(columnId, metadata.DATA)] = NewByteArrayStreamCheckpoint2(compressed, positionsList)
		}
		if availableStreams.Has(metadata.LENGTH) {
			checkpoints[NewStreamId(columnId, metadata.LENGTH)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
		}
	} else if encoding == metadata.DICTIONARY || encoding == metadata.DICTIONARY_V2 {
		if availableStreams.Has(metadata.DATA) {
			checkpoints[NewStreamId(columnId, metadata.DATA)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
		}
	} else {
		panic(fmt.Sprintf("Unsupported encoding for slice column: %d", encoding))
	}
	return checkpoints
}

func getListOrMapColumnCheckpoints(columnId metadata.MothColumnId, encoding metadata.ColumnEncodingKind, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.LENGTH) {
		checkpoints[NewStreamId(columnId, metadata.LENGTH)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
	}
	return checkpoints
}

func getStructColumnCheckpoints(columnId metadata.MothColumnId, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	return checkpoints
}

func getDecimalColumnCheckpoints(columnId metadata.MothColumnId, encoding metadata.ColumnEncodingKind, compressed bool, availableStreams util.SetInterface[metadata.StreamKind], positionsList *ColumnPositionsList) map[StreamId]StreamCheckpoint {
	checkpoints := make(map[StreamId]StreamCheckpoint)
	if availableStreams.Has(metadata.PRESENT) {
		checkpoints[NewStreamId(columnId, metadata.PRESENT)] = NewBooleanStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.DATA) {
		checkpoints[NewStreamId(columnId, metadata.DATA)] = NewDecimalStreamCheckpoint2(compressed, positionsList)
	}
	if availableStreams.Has(metadata.SECONDARY) {
		checkpoints[NewStreamId(columnId, metadata.SECONDARY)] = createLongStreamCheckpoint(encoding, compressed, positionsList)
	}
	return checkpoints
}

func createLongStreamCheckpoint(encoding metadata.ColumnEncodingKind, compressed bool, positionsList *ColumnPositionsList) StreamCheckpoint {
	if encoding == metadata.DIRECT_V2 || encoding == metadata.DICTIONARY_V2 {
		return NewLongStreamV2Checkpoint2(compressed, positionsList)
	}
	if encoding == metadata.DIRECT || encoding == metadata.DICTIONARY {
		return NewLongStreamV1Checkpoint2(compressed, positionsList)
	}
	panic(fmt.Sprintf("Unsupported encoding for long stream: %d", encoding))
}

type ColumnPositionsList struct {
	columnId      metadata.MothColumnId
	columnType    metadata.MothTypeKind
	positionsList *util.ArrayList[int32]
	index         int32
}

func NewColumnPositionsList(columnId metadata.MothColumnId, columnType metadata.MothTypeKind, positionsList *util.ArrayList[int32]) *ColumnPositionsList {
	ct := new(ColumnPositionsList)
	ct.columnId = columnId
	ct.columnType = columnType
	ct.positionsList = positionsList
	return ct
}

func (ct *ColumnPositionsList) GetIndex() int32 {
	return ct.index
}

func (ct *ColumnPositionsList) HasNextPosition() bool {
	return ct.index < int32(ct.positionsList.Size())
}

func (ct *ColumnPositionsList) NextPosition() int32 {
	if !ct.HasNextPosition() {
		panic(fmt.Sprintf("Not enough positions for column %s:%d checkpoints", ct.columnId.String(), ct.columnType))
	}
	position := ct.positionsList.Get(int(ct.index))
	ct.index++
	return position
}
