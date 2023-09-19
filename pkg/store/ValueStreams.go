package store

import (
	"fmt"

	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
)

func CreateValueStreams(streamId StreamId, chunkLoader MothChunkLoader, kind metadata.MothTypeKind, encoding metadata.ColumnEncodingKind) IValueInputStream {
	if streamId.GetStreamKind() == metadata.PRESENT {
		return NewBooleanInputStream(NewMothInputStream(chunkLoader))
	}
	if (encoding == metadata.DICTIONARY || encoding == metadata.DICTIONARY_V2) && (streamId.GetStreamKind() == metadata.LENGTH || streamId.GetStreamKind() == metadata.DATA) {
		return createLongStream(NewMothInputStream(chunkLoader), encoding, false)
	}
	if streamId.GetStreamKind() == metadata.DATA {
		switch kind {
		case metadata.BOOLEAN:
			return NewBooleanInputStream(NewMothInputStream(chunkLoader))
		case metadata.BYTE:
			return NewByteInputStream(NewMothInputStream(chunkLoader))
		case metadata.SHORT, metadata.INT, metadata.LONG, metadata.DATE:
			return createLongStream(NewMothInputStream(chunkLoader), encoding, true)
		case metadata.FLOAT:
			return NewFloatInputStream(NewMothInputStream(chunkLoader))
		case metadata.DOUBLE:
			return NewDoubleInputStream(NewMothInputStream(chunkLoader))
		case metadata.STRING, metadata.VARCHAR, metadata.CHAR, metadata.BINARY:
			return NewByteArrayInputStream(NewMothInputStream(chunkLoader))
		case metadata.TIMESTAMP, metadata.TIMESTAMP_INSTANT:
			return createLongStream(NewMothInputStream(chunkLoader), encoding, true)
		case metadata.DECIMAL:
			return NewDecimalInputStream(chunkLoader)
		case metadata.UNION:
			return NewByteInputStream(NewMothInputStream(chunkLoader))
		case metadata.LIST, metadata.MAP, metadata.STRUCT:
		}
	}
	if streamId.GetStreamKind() == metadata.LENGTH {
		switch kind {
		case metadata.STRING, metadata.VARCHAR, metadata.CHAR, metadata.BINARY, metadata.MAP, metadata.LIST:
			return createLongStream(NewMothInputStream(chunkLoader), encoding, false)
		default:
		}
	}
	if (kind == metadata.TIMESTAMP || kind == metadata.TIMESTAMP_INSTANT) && streamId.GetStreamKind() == metadata.SECONDARY {
		return createLongStream(NewMothInputStream(chunkLoader), encoding, false)
	}
	if kind == metadata.DECIMAL && streamId.GetStreamKind() == metadata.SECONDARY {
		return createLongStream(NewMothInputStream(chunkLoader), encoding, true)
	}
	if streamId.GetStreamKind() == metadata.DICTIONARY_DATA {
		switch kind {
		case metadata.STRING, metadata.VARCHAR, metadata.CHAR, metadata.BINARY:
			return NewByteArrayInputStream(NewMothInputStream(chunkLoader))
		default:
		}
	}
	panic(fmt.Sprintf("Unsupported column type %d for stream %s with encoding %d", kind, streamId, encoding))
}

func createLongStream(inputStream *MothInputStream, encoding metadata.ColumnEncodingKind, signed bool) IValueInputStream {
	if encoding == metadata.DIRECT_V2 || encoding == metadata.DICTIONARY_V2 {
		return NewLongInputStreamV2(inputStream, signed, false)
	} else if encoding == metadata.DIRECT || encoding == metadata.DICTIONARY {
		return NewLongInputStreamV1(inputStream, signed)
	} else {
		panic(fmt.Sprintf("Unsupported encoding for long stream: %d", encoding))
	}
}
