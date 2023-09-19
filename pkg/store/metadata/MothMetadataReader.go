package metadata

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/mothdb-bd/orc-go/pkg/mothio"
	"github.com/mothdb-bd/orc-go/pkg/optional"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/proto"
	"github.com/mothdb-bd/orc-go/pkg/util"

	protobuf "github.com/golang/protobuf/proto"
	"github.com/shopspring/decimal"
)

var (
	REPLACEMENT_CHARACTER_CODE_POINT int32 = 0xFFFD
	PROTOBUF_MESSAGE_MAX_LIMIT       int32 = util.Int32Exact(int64(util.GB.Bytes()))
)

type MothMetadataReader struct {
	MetadataReader
}

func NewMothMetadataReader() *MothMetadataReader {
	return new(MothMetadataReader)
}

// @Override
func (mr *MothMetadataReader) ReadPostScript(inputStream mothio.InputStream) *PostScript {
	postScript := &proto.PostScript{}
	readProtobufObject(inputStream, postScript)
	return NewPostScript(postScript.GetVersion(), int64(postScript.GetFooterLength()), int64(postScript.GetMetadataLength()), toCompression(postScript.GetCompression()), postScript.GetCompressionBlockSize(), toHiveWriterVersion(postScript.GetWriterVersion()))
}

func toHiveWriterVersion(writerVersion uint32) HiveWriterVersion {
	if writerVersion >= 1 {
		return MOTH_HIVE_8732
	}
	return ORIGINAL
}

// @Override
func (mr *MothMetadataReader) ReadMetadata(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Metadata {

	metadata := &proto.Metadata{}
	readProtobufObject(inputStream, metadata)
	return NewMetadata(reader_toStripeStatistics(hiveWriterVersion, metadata.GetStripeStats()))
}

func reader_toStripeStatistics(hiveWriterVersion HiveWriterVersion, types []*proto.StripeStatistics) *util.ArrayList[*optional.Optional[*StripeStatistics]] {

	list := util.NewArrayList[*optional.Optional[*StripeStatistics]]()
	for _, kind := range types {
		list.Add(toStripeStatistics2(hiveWriterVersion, kind))
	}
	return list
}

func toStripeStatistics2(hiveWriterVersion HiveWriterVersion, stripeStatistics *proto.StripeStatistics) *optional.Optional[*StripeStatistics] {
	return optional.Map(toColumnStatistics2(hiveWriterVersion, stripeStatistics.GetColStats(), false), NewStripeStatistics)
}

// @Override
func (mr *MothMetadataReader) ReadFooter(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *Footer {
	footer := &proto.Footer{}
	readProtobufObject(inputStream, footer)
	return NewFooter(footer.GetNumberOfRows(), util.Ternary(footer.GetRowIndexStride() == 0, optional.OptionalIntEmpty(), optional.OptionalIntof(int32(footer.GetRowIndexStride()))), r_toStripeInformation(footer.GetStripes()), r_toType2(footer.GetTypes()), toColumnStatistics2(hiveWriterVersion, footer.GetStatistics(), false), toUserMetadata(footer.GetMetadata()), optional.Of(footer.GetWriter()))
}

func r_toStripeInformation(types []*proto.StripeInformation) *util.ArrayList[*StripeInformation] {
	re := util.NewArrayList[*StripeInformation]()
	for i := 0; i < len(types); i++ {
		re.Add(r_toStripeInformation2(types[i]))
	}
	return re
}

func r_toStripeInformation2(stripeInformation *proto.StripeInformation) *StripeInformation {
	return NewStripeInformation(util.Int32Exact(int64(stripeInformation.GetNumberOfRows())), stripeInformation.GetOffset(), stripeInformation.GetIndexLength(), stripeInformation.GetDataLength(), stripeInformation.GetFooterLength())
}

// @Override
func (mr *MothMetadataReader) ReadStripeFooter(types *ColumnMetadata[*MothType], inputStream mothio.InputStream, legacyFileTimeZone *time.Location) *StripeFooter {
	stripeFooter := &proto.StripeFooter{}
	readProtobufObject(inputStream, stripeFooter)

	tzStr := stripeFooter.GetWriterTimezone()

	var tz *time.Location
	if tzStr == "" {
		tz, _ = time.LoadLocation(tzStr)
	} else {
		tz = legacyFileTimeZone
	}
	return NewStripeFooter(toStream2(stripeFooter.GetStreams()), toColumnEncoding2(stripeFooter.GetColumns()), tz)
}

func toStream(stream *proto.Stream) *Stream {
	return NewStream(NewMothColumnId(stream.GetColumn()), toStreamKind(stream.GetKind()), util.Int32Exact(int64(stream.GetLength())), true)
}

func toStream2(streams []*proto.Stream) *util.ArrayList[*Stream] {
	ss := util.NewArrayList[*Stream]()
	for _, stream := range streams {
		ss.Add(toStream(stream))
	}
	return ss
}

func toColumnEncoding(columnEncoding *proto.ColumnEncoding) *ColumnEncoding {
	return NewColumnEncoding(toColumnEncodingKind(columnEncoding.GetKind()), columnEncoding.GetDictionarySize())
}

func toColumnEncoding2(columnEncodings []*proto.ColumnEncoding) *ColumnMetadata[*ColumnEncoding] {
	ss := util.NewArrayList[*ColumnEncoding]()
	for _, columnEncoding := range columnEncodings {
		ss.Add(toColumnEncoding(columnEncoding))
	}
	return NewColumnMetadata(ss)
}

// @Override
func (mr *MothMetadataReader) ReadRowIndexes(hiveWriterVersion HiveWriterVersion, inputStream mothio.InputStream) *util.ArrayList[*RowGroupIndex] {
	rowIndex := &proto.RowIndex{}
	readProtobufObject(inputStream, rowIndex)

	list := util.NewArrayList[*RowGroupIndex]()
	for _, e := range rowIndex.GetEntry() {
		list.Add(toRowGroupIndex(hiveWriterVersion, e))
	}
	return list

}

// @Override
func (mr *MothMetadataReader) ReadBloomFilterIndexes(inputStream mothio.InputStream) *util.ArrayList[*BloomFilter] {
	bloomFilter := &proto.BloomFilterIndex{}
	readProtobufObject(inputStream, bloomFilter)
	bloomFilterList := bloomFilter.GetBloomFilter()

	builder := util.NewArrayList[*BloomFilter]()
	for _, mothBloomFilter := range bloomFilterList {
		if mothBloomFilter.GetUtf8Bitset() != nil {
			utf8Bitset := mothBloomFilter.GetUtf8Bitset()
			bits := make([]int64, util.Lens(utf8Bitset)/util.INT64_BYTES)
			j := 0
			for i := 0; i < len(utf8Bitset); i += util.INT64_BYTES {
				bits[j] = util.BytesToInt64(utf8Bitset, i)
				j++
			}
			// utf8Bitset.AsReadOnlySlicefer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(bits)
			builder.Add(NewBloomFilter2(bits, int32(mothBloomFilter.GetNumHashFunctions())))
		} else {
			longBits := make([]int64, len(mothBloomFilter.GetBitset()))
			for i, bt := range mothBloomFilter.GetBitset() {
				longBits[i] = int64(bt)
			}
			builder.Add(NewBloomFilter2(longBits, int32(mothBloomFilter.GetNumHashFunctions())))
		}
	}
	return builder
}

func toRowGroupIndex(hiveWriterVersion HiveWriterVersion, rowIndexEntry *proto.RowIndexEntry) *RowGroupIndex {
	positionsList := rowIndexEntry.GetPositions()
	positions := util.NewArrayList[int32]()
	for index := 0; index < len(positionsList); index++ {
		longPosition := positionsList[index]
		intPosition := int32(longPosition)
		util.CheckArgument2(uint64(intPosition) == longPosition, fmt.Sprintf("Expected checkpoint position [%d] value [%d] to be an integer", index, longPosition))
		positions.Add(intPosition)
	}
	return NewRowGroupIndex(positions, toColumnStatistics(hiveWriterVersion, rowIndexEntry.GetStatistics(), true))
}

func toColumnStatistics(hiveWriterVersion HiveWriterVersion, statistics *proto.ColumnStatistics, isRowGroup bool) *ColumnStatistics {
	var minAverageValueBytes int64
	if statistics.GetBucketStatistics() != nil {
		minAverageValueBytes = BOOLEAN_VALUE_BYTES
	} else if statistics.GetIntStatistics() != nil {
		minAverageValueBytes = INTEGER_VALUE_BYTES
	} else if statistics.GetDoubleStatistics() != nil {
		minAverageValueBytes = DOUBLE_VALUE_BYTES
	} else if statistics.GetStringStatistics() != nil {
		minAverageValueBytes = STRING_VALUE_BYTES_OVERHEAD
		if statistics.NumberOfValues != nil && statistics.GetNumberOfValues() > 0 {
			minAverageValueBytes += statistics.GetStringStatistics().GetSum() / int64(statistics.GetNumberOfValues())
		}
	} else if statistics.GetDateStatistics() != nil {
		minAverageValueBytes = DATE_VALUE_BYTES
	} else if statistics.GetTimestampStatistics() != nil {
		minAverageValueBytes = TIMESTAMP_VALUE_BYTES
	} else if statistics.GetDecimalStatistics() != nil {
		minAverageValueBytes = DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES
	} else if statistics.GetBinaryStatistics() != nil {
		minAverageValueBytes = BINARY_VALUE_BYTES_OVERHEAD
		if statistics.NumberOfValues != nil && statistics.GetNumberOfValues() > 0 {
			minAverageValueBytes += statistics.GetBinaryStatistics().GetSum() / int64(statistics.GetNumberOfValues())
		}
	} else {
		minAverageValueBytes = 0
	}
	if statistics.HasNull != nil && statistics.GetNumberOfValues() == 0 && !statistics.GetHasNull() {
		return NewColumnStatistics(0, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	}
	var bs *BooleanStatistics = nil
	if statistics.GetBucketStatistics() != nil {
		bs = toBooleanStatistics(statistics.GetBucketStatistics())
	}

	var is *IntegerStatistics = nil
	if statistics.GetIntStatistics() != nil {
		is = toIntegerStatistics(statistics.GetIntStatistics())
	}

	var ds *DoubleStatistics = nil
	if statistics.GetDoubleStatistics() != nil {
		ds = toDoubleStatistics(statistics.GetDoubleStatistics())
	}

	var ss *StringStatistics = nil
	if statistics.GetStringStatistics() != nil {
		ss = toStringStatistics(hiveWriterVersion, statistics.GetStringStatistics(), isRowGroup)
	}

	var dates *DateStatistics = nil
	if statistics.GetDateStatistics() != nil {
		dates = toDateStatistics(hiveWriterVersion, statistics.GetDateStatistics(), isRowGroup)
	}

	var ts *TimestampStatistics = nil
	if statistics.GetTimestampStatistics() != nil {
		ts = toTimestampStatistics(hiveWriterVersion, statistics.GetTimestampStatistics(), isRowGroup)
	}

	var decimals *DecimalStatistics = nil
	if statistics.GetDecimalStatistics() != nil {
		decimals = toDecimalStatistics(statistics.GetDecimalStatistics())
	}

	var bins *BinaryStatistics = nil
	if statistics.GetBinaryStatistics() != nil {
		bins = toBinaryStatistics(statistics.GetBinaryStatistics())
	}

	return NewColumnStatistics(int64(statistics.GetNumberOfValues()), minAverageValueBytes, bs, is, ds, ss, dates, ts, decimals, bins, nil)
}

func toColumnStatistics2(hiveWriterVersion HiveWriterVersion, columnStatistics []*proto.ColumnStatistics, isRowGroup bool) *optional.Optional[*ColumnMetadata[*ColumnStatistics]] {
	if columnStatistics == nil || len(columnStatistics) != 0 {
		return optional.Empty[*ColumnMetadata[*ColumnStatistics]]()
	}

	cs := util.NewArrayList[*ColumnStatistics]()
	for _, column := range columnStatistics {
		cs.Add(toColumnStatistics(hiveWriterVersion, column, isRowGroup))
	}
	return optional.Of(NewColumnMetadata(cs))
}

func toUserMetadata(metadataList []*proto.UserMetadataItem) map[string]*slice.Slice {
	mapBuilder := util.EmptyMap[string, *slice.Slice]()
	for _, item := range metadataList {
		s := slice.NewWithBuf(item.GetValue())
		mapBuilder[item.GetName()] = s
	}
	return mapBuilder
}

func toBooleanStatistics(bucketStatistics *proto.BucketStatistics) *BooleanStatistics {
	if len(bucketStatistics.GetCount()) == 0 {
		return nil
	}
	return NewBooleanStatistics(int64(bucketStatistics.GetCount()[0]))
}

func toIntegerStatistics(integerStatistics *proto.IntegerStatistics) *IntegerStatistics {
	return NewIntegerStatistics(integerStatistics.GetMinimum(), integerStatistics.GetMaximum(), integerStatistics.GetSum())
}

func toDoubleStatistics(doubleStatistics *proto.DoubleStatistics) *DoubleStatistics {
	if (doubleStatistics.Minimum != nil && math.IsNaN(doubleStatistics.GetMinimum())) || (doubleStatistics.Minimum != nil && math.IsNaN(doubleStatistics.GetMaximum())) || (doubleStatistics.Sum != nil && math.IsNaN(doubleStatistics.GetSum())) {
		return nil
	}
	return NewDoubleStatistics(doubleStatistics.GetMinimum(), doubleStatistics.GetMaximum())
}

func toStringStatistics(hiveWriterVersion HiveWriterVersion, stringStatistics *proto.StringStatistics, isRowGroup bool) *StringStatistics {
	if hiveWriterVersion == ORIGINAL && !isRowGroup {
		return nil
	}
	// maximum := util.Ternary(stringStatistics.Maximum != nil, MaxStringTruncateToValidRange(byteStringToSlice(stringStatistics.GetMaximum()), hiveWriterVersion), nil)
	// minimum := util.Ternary(stringStatistics.Minimum != nil, MinStringTruncateToValidRange(byteStringToSlice(stringStatistics.GetMinimum()), hiveWriterVersion), nil)
	maximum := byteStringToSlice(stringStatistics.GetMaximum())
	minimum := byteStringToSlice(stringStatistics.GetMinimum())
	sum := util.Ternary(stringStatistics.Sum != nil, stringStatistics.GetSum(), 0)
	return NewStringStatistics(minimum, maximum, sum)
}

func toDecimalStatistics(decimalStatistics *proto.DecimalStatistics) *DecimalStatistics {

	var minimum decimal.Decimal
	var maximum decimal.Decimal
	if decimalStatistics.Minimum != nil {
		minimum, _ = decimal.NewFromString(decimalStatistics.GetMinimum())
	}
	if decimalStatistics.Maximum != nil {
		maximum, _ = decimal.NewFromString(decimalStatistics.GetMaximum())
	}

	return NewDecimalStatistics(&minimum, &maximum, SHORT_DECIMAL_VALUE_BYTES)
}

func toBinaryStatistics(binaryStatistics *proto.BinaryStatistics) *BinaryStatistics {
	if binaryStatistics.Sum == nil {
		return nil
	}
	return NewBinaryStatistics(binaryStatistics.GetSum())
}

func byteStringToSlice(value string) *slice.Slice {
	s, _ := slice.NewByString(value)
	return s
}

// //@VisibleForTesting
// func MaxStringTruncateToValidRange(value *slice.Slice, version HiveWriterVersion) *slice.Slice {
// 	if value == nil {
// 		return nil
// 	}
// 	if version != ORIGINAL {
// 		return value
// 	}
// 	index := findStringStatisticTruncationPositionForOriginalMothWriter(value)
// 	if index == int32(value.Size()) {
// 		return value
// 	}
// 	newValue, _ := value.MakeSlice(0, int(index+1)) //Slices.copyOf(value, 0, index+1)
// 	// newValue.setByte(index, 0xFF)
// 	newValue.PutByte(int(index), 0xFF)
// 	return newValue
// }

// //@VisibleForTesting
// func MinStringTruncateToValidRange(value *slice.Slice, version HiveWriterVersion) *slice.Slice {
// 	if value == nil {
// 		return nil
// 	}
// 	if version != ORIGINAL {
// 		return value
// 	}
// 	index := findStringStatisticTruncationPositionForOriginalMothWriter(value)
// 	if index == int32(value.Size()) {
// 		return value
// 	}
// 	newValue, _ := value.MakeSlice(0, int(index))
// 	// Slices.copyOf(value, 0, index)
// 	return newValue
// }

// var MIN_SUPPLEMENTARY_CODE_POINT int32 = 0x010000

// //@VisibleForTesting
// func findStringStatisticTruncationPositionForOriginalMothWriter(utf8 *slice.Slice) int32 {
// 	length := utf8.Size()
// 	position := 0
// 	for position < length {
// 		codePoint := tryGetCodePointAt(utf8, position)
// 		if codePoint < 0 {
// 			break
// 		}
// 		if codePoint == REPLACEMENT_CHARACTER_CODE_POINT {
// 			break
// 		}
// 		if codePoint >= MIN_SUPPLEMENTARY_CODE_POINT {
// 			break
// 		}
// 		position += lengthOfCodePoint(codePoint)
// 	}
// 	return position
// }

func toDateStatistics(hiveWriterVersion HiveWriterVersion, dateStatistics *proto.DateStatistics, isRowGroup bool) *DateStatistics {
	if hiveWriterVersion == ORIGINAL && !isRowGroup {
		return nil
	}
	return NewDateStatistics(dateStatistics.GetMinimum(), dateStatistics.GetMaximum())
}

func toTimestampStatistics(hiveWriterVersion HiveWriterVersion, timestampStatistics *proto.TimestampStatistics, isRowGroup bool) *TimestampStatistics {
	if hiveWriterVersion == ORIGINAL && !isRowGroup {
		return nil
	}
	return NewTimestampStatistics(timestampStatistics.GetMinimumUtc(), timestampStatistics.GetMaximumUtc())
}

func r_toType(kind *proto.Type) *MothType {
	length := optional.Empty[int32]()
	if kind.GetKind() == proto.Type_VARCHAR || kind.GetKind() == proto.Type_CHAR {
		length = optional.Of(int32(kind.GetMaximumLength()))
	}
	precision := optional.Empty[int32]()
	scale := optional.Empty[int32]()
	if kind.GetKind() == proto.Type_DECIMAL {
		precision = optional.Of(int32(kind.GetPrecision()))
		scale = optional.Of(int32(kind.GetScale()))
	}
	return NewMothType5(toTypeKind(kind.GetKind()), toMothColumnId(kind.GetSubtypes()), util.NewArrayList(kind.GetFieldNames()...), length, precision, scale, toMap(kind.GetAttributes()))
}

func toMothColumnId(columnIds []uint32) *util.ArrayList[MothColumnId] {

	re := util.NewArrayList[MothColumnId]()
	for _, columnid := range columnIds {
		re.Add(NewMothColumnId(columnid))
	}
	return re
}

func r_toType2(types []*proto.Type) *ColumnMetadata[*MothType] {

	kinds := util.NewArrayList[*MothType]()
	for _, t := range types {
		kinds.Add(r_toType(t))
	}
	return NewColumnMetadata(kinds)
}

func toTypeKind(typeKind proto.Type_Kind) MothTypeKind {
	switch typeKind {
	case proto.Type_BOOLEAN:
		return BOOLEAN
	case proto.Type_BYTE:
		return BYTE
	case proto.Type_SHORT:
		return SHORT
	case proto.Type_INT:
		return INT
	case proto.Type_LONG:
		return LONG
	case proto.Type_FLOAT:
		return FLOAT
	case proto.Type_DOUBLE:
		return DOUBLE
	case proto.Type_STRING:
		return STRING
	case proto.Type_BINARY:
		return BINARY
	case proto.Type_TIMESTAMP:
		return TIMESTAMP
	case proto.Type_TIMESTAMP_INSTANT:
		return TIMESTAMP_INSTANT
	case proto.Type_LIST:
		return LIST
	case proto.Type_MAP:
		return MAP
	case proto.Type_STRUCT:
		return STRUCT
	case proto.Type_UNION:
		return UNION
	case proto.Type_DECIMAL:
		return DECIMAL
	case proto.Type_DATE:
		return DATE
	case proto.Type_VARCHAR:
		return VARCHAR
	case proto.Type_CHAR:
		return CHAR
	}
	panic(" stream type not implemented yet")
}

func toMap(attributes []*proto.StringPair) map[string]string {
	results := util.EmptyMap[string, string]()
	if attributes != nil {
		for _, attribute := range attributes {
			if attribute.Key != nil && attribute.Value != nil {
				results[attribute.GetKey()] = attribute.GetValue()
			}
		}
	}
	return results
}

func toStreamKind(streamKind proto.Stream_Kind) StreamKind {
	switch streamKind {
	case proto.Stream_PRESENT:
		return PRESENT
	case proto.Stream_DATA:
		return DATA
	case proto.Stream_LENGTH:
		return LENGTH
	case proto.Stream_DICTIONARY_DATA:
		return DICTIONARY_DATA
	case proto.Stream_DICTIONARY_COUNT:
		return DICTIONARY_COUNT
	case proto.Stream_SECONDARY:
		return SECONDARY
	case proto.Stream_ROW_INDEX:
		return ROW_INDEX
	case proto.Stream_BLOOM_FILTER:
		return BLOOM_FILTER
	case proto.Stream_BLOOM_FILTER_UTF8:
		return BLOOM_FILTER_UTF8
	case proto.Stream_ENCRYPTED_INDEX:
	case proto.Stream_ENCRYPTED_DATA:
	case proto.Stream_STRIPE_STATISTICS:
	case proto.Stream_FILE_STATISTICS:
		// unsupported
	}
	panic(" stream type not implemented yet")
}

func toColumnEncodingKind(columnEncodingKind proto.ColumnEncoding_Kind) ColumnEncodingKind {
	switch columnEncodingKind {
	case proto.ColumnEncoding_DIRECT:
		return DIRECT
	case proto.ColumnEncoding_DIRECT_V2:
		return DIRECT_V2
	case proto.ColumnEncoding_DICTIONARY:
		return DICTIONARY
	case proto.ColumnEncoding_DICTIONARY_V2:
		return DICTIONARY_V2
	}
	panic(" stream encoding not implemented yet")
}

func toCompression(compression proto.CompressionKind) CompressionKind {
	switch compression {
	case proto.CompressionKind_NONE:
		return NONE
	case proto.CompressionKind_ZLIB:
		return ZLIB
	case proto.CompressionKind_SNAPPY:
		return SNAPPY
	case proto.CompressionKind_LZO:
		// TODO unsupported
		break
	case proto.CompressionKind_LZ4:
		return LZ4
	case proto.CompressionKind_ZSTD:
		return ZSTD
	}
	panic(" compression not implemented yet")
}

func readProtobufObject(input mothio.InputStream, object protobuf.Message) {

	buf := make([]byte, util.INT32_BYTES)
	_, err := input.ReadBS2(buf)
	if err == nil {
		size := int32(binary.LittleEndian.Uint32(buf))
		b := make([]byte, size)
		input.ReadBS2(b)
		err := protobuf.Unmarshal(b, object)
		if err != nil {
			panic(err)
		}
	}
}
