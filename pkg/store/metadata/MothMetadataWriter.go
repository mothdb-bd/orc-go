package metadata

import (
	"fmt"

	protobuf "github.com/golang/protobuf/proto"
	"github.com/mothdb-bd/orc-go/pkg/slice"
	"github.com/mothdb-bd/orc-go/pkg/store/proto"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	MOTH_WRITER_ID             uint32   = 4
	MOTH_WRITER_VERSION        int32    = 6
	PRESTO_WRITER_ID           int32    = 2
	HIVE_LEGACY_WRITER_VERSION int32    = 4
	MOTH_METADATA_VERSION      []uint32 = []uint32{0, 12}
)

type WriterIdentification int8

const (
	/**
	 * Write MOTH files with a writer identification and version number that is readable by Hive 2.0.0 to 2.2.0
	 */
	LEGACY_HIVE_COMPATIBLE WriterIdentification = iota

	/**
	 * Write MOTH files with Moth writer identification.
	 */
	MOTH
)

type MothMetadataWriter struct {
	// 继承
	MetadataWriter

	writerIdentification WriterIdentification
}

func NewMothMetadataWriter(writerIdentification WriterIdentification) *MothMetadataWriter {
	mr := new(MothMetadataWriter)
	mr.writerIdentification = writerIdentification
	return mr
}

// @Override
func (mr *MothMetadataWriter) GetMothMetadataVersion() []uint32 {
	return MOTH_METADATA_VERSION
}

// @Override
func (mr *MothMetadataWriter) WritePostscript(output slice.SliceOutput, footerLength uint64, metadataLength uint64, compression CompressionKind, compressionBlockSize uint64) int32 {
	postScriptProtobuf := &proto.PostScript{}
	postScriptProtobuf.Version = MOTH_METADATA_VERSION  //addAllVersion(MOTH_METADATA_VERSION)
	postScriptProtobuf.FooterLength = &footerLength     //setFooterLength(footerLength)
	postScriptProtobuf.MetadataLength = &metadataLength // setMetadataLength()

	com := w_toCompression(compression)
	postScriptProtobuf.Compression = &com
	postScriptProtobuf.CompressionBlockSize = &compressionBlockSize

	tmp := mr.getMothWriterVersion()
	postScriptProtobuf.WriterVersion = &tmp
	postScriptProtobuf.Magic = &MAGIC //.setMagic(MAGIC.toStringUtf8())

	return writeProtobufObject(output, postScriptProtobuf)
}

func (mr *MothMetadataWriter) getMothWriterVersion() uint32 {
	switch mr.writerIdentification {
	case LEGACY_HIVE_COMPATIBLE:
		return uint32(HIVE_LEGACY_WRITER_VERSION)
	case MOTH:
		return uint32(MOTH_WRITER_VERSION)
	}
	panic(fmt.Sprintf("Unexpected value: %d", mr.writerIdentification))
}

// @Override
func (mr *MothMetadataWriter) WriteMetadata(output slice.SliceOutput, metadata *Metadata) int32 {
	metadataProtobuf := &proto.Metadata{}
	ssList := metadata.GetStripeStatsList()
	ssArray := make([]*proto.StripeStatistics, ssList.Size())

	for i, ss := range ssList.ToArray() {
		ssArray[i] = toStripeStatistics(ss.Get())
	}
	metadataProtobuf.StripeStats = ssArray
	return writeProtobufObject(output, metadataProtobuf)
}

func toStripeStatistics(stripeStatistics *StripeStatistics) *proto.StripeStatistics {
	stripeStatisticsProto := &proto.StripeStatistics{}

	stream := stripeStatistics.GetColumnStatistics().List()
	array := make([]*proto.ColumnStatistics, stream.Size())
	for i, ss := range stream.ToArray() {
		array[i] = w_toColumnStatistics(ss)
	}
	stripeStatisticsProto.ColStats = array
	return stripeStatisticsProto
}

// @Override
func (mr *MothMetadataWriter) WriteFooter(output slice.SliceOutput, footer *Footer) int32 {
	fotterProto := &proto.Footer{}
	ns := footer.GetNumberOfRows()
	fotterProto.NumberOfRows = &ns
	re := uint32(footer.GetRowsInRowGroup().OrElse(0))
	fotterProto.RowIndexStride = &re

	sn := make([]*proto.StripeInformation, footer.GetStripes().Size())
	for i, ss := range footer.GetStripes().ToArray() {
		sn[i] = toStripeInformation(ss)
	}
	fotterProto.Stripes = sn

	te := make([]*proto.Type, footer.GetTypes().Size())
	for i, kind := range footer.GetTypes().List().ToArray() {
		te[i] = toType(kind)
	}
	fotterProto.Types = te

	if footer.GetFileStats().IsPresent() {
		cs := make([]*proto.ColumnStatistics, footer.GetFileStats().Get().Size())
		for i, fs := range footer.GetFileStats().Get().List().ToArray() {
			cs[i] = w_toColumnStatistics(fs)
		}
		fotterProto.Statistics = cs
	} else {
		fotterProto.Statistics = make([]*proto.ColumnStatistics, 0)
	}

	ud := make([]*proto.UserMetadataItem, len(footer.GetUserMetadata()))
	i := 0
	for k, v := range footer.GetUserMetadata() {
		ud[i] = w_toUserMetadata(k, v)
		i++
	}
	fotterProto.Metadata = ud

	// builder := MothProto.Footer.newBuilder()
	// .setNumberOfRows(footer.getNumberOfRows())
	// .setRowIndexStride(footer.getRowsInRowGroup().orElse(0))
	// .addAllStripes(footer.getStripes().stream().Map(MothMetadataWriter.toStripeInformation).collect(toList()))
	// .addAllTypes(footer.getTypes().stream().Map(MothMetadataWriter.toType).collect(toList()))
	// .addAllStatistics(footer.getFileStats().Map(ColumnMetadata.stream).orElseGet(java.util.stream.Stream.empty).Map(MothMetadataWriter.w_toColumnStatistics).collect(toList()))
	// .addAllMetadata(footer.getUserMetadata().entrySet().stream().Map(MothMetadataWriter.w_toUserMetadata).collect(toList()))
	mr.setWriter(fotterProto)
	return writeProtobufObject(output, fotterProto)
}

func (mr *MothMetadataWriter) setWriter(builder *proto.Footer) {
	switch mr.writerIdentification {
	case LEGACY_HIVE_COMPATIBLE:
		return
	case MOTH:
		builder.Writer = &MOTH_WRITER_ID
		return
	}
	panic(fmt.Sprintf("Unexpected value: %d", mr.writerIdentification))
}

func toStripeInformation(stripe *StripeInformation) *proto.StripeInformation {
	sn := &proto.StripeInformation{}
	ns := uint64(stripe.GetNumberOfRows())
	sn.NumberOfRows = &ns
	ot := stripe.GetOffset()
	sn.Offset = &ot
	ih := stripe.GetIndexLength()
	sn.IndexLength = &ih
	dh := stripe.GetDataLength()
	sn.DataLength = &dh
	fh := stripe.GetFooterLength()
	sn.FooterLength = &fh
	return sn
}

func toType(kind *MothType) *proto.Type {
	te := &proto.Type{}
	td := w_toTypeKind(kind.GetMothTypeKind())
	te.Kind = &td

	ss := make([]uint32, kind.GetFieldTypeIndexes().Size())
	for i, fs := range kind.GetFieldTypeIndexes().ToArray() {
		ss[i] = uint32(fs.GetId())
	}
	te.Subtypes = ss

	te.FieldNames = kind.GetFieldNames().ToArray()

	te.Attributes = toStringPairList(kind.GetAttributes())
	if kind.GetLength().IsPresent() {
		mh := uint32(kind.GetLength().Get())
		te.MaximumLength = &mh
	}

	if kind.GetPrecision().IsPresent() {
		pn := uint32(kind.GetPrecision().Get())
		te.Precision = &pn
	}

	if kind.GetScale().IsPresent() {
		se := uint32(kind.GetScale().Get())
		te.Scale = &se
	}
	return te

}

func w_toTypeKind(mothTypeKind MothTypeKind) proto.Type_Kind {
	switch mothTypeKind {
	case BOOLEAN:
		return proto.Type_BOOLEAN
	case BYTE:
		return proto.Type_BYTE
	case SHORT:
		return proto.Type_SHORT
	case INT:
		return proto.Type_INT
	case LONG:
		return proto.Type_LONG
	case DECIMAL:
		return proto.Type_DECIMAL
	case FLOAT:
		return proto.Type_FLOAT
	case DOUBLE:
		return proto.Type_DOUBLE
	case STRING:
		return proto.Type_STRING
	case VARCHAR:
		return proto.Type_VARCHAR
	case CHAR:
		return proto.Type_CHAR
	case BINARY:
		return proto.Type_BINARY
	case DATE:
		return proto.Type_DATE
	case TIMESTAMP:
		return proto.Type_TIMESTAMP
	case TIMESTAMP_INSTANT:
		return proto.Type_TIMESTAMP_INSTANT
	case LIST:
		return proto.Type_LIST
	case MAP:
		return proto.Type_MAP
	case STRUCT:
		return proto.Type_STRUCT
	case UNION:
		return proto.Type_UNION
	}
	panic(fmt.Sprintf("Unsupported kind: %d", mothTypeKind))
}

func toStringPairList(attributes map[string]string) []*proto.StringPair {
	sr := make([]*proto.StringPair, len(attributes))
	i := 0
	for k, v := range attributes {
		s := &proto.StringPair{}
		s.Key = &k
		s.Value = &v
		sr[i] = s
	}
	return sr
	// return attributes.entrySet().stream().Map(func(entry interface{}) {
	// 	MothProto.StringPair.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build()
	// }).collect(toImmutableList())
}

func w_toColumnStatistics(columnStatistics *ColumnStatistics) *proto.ColumnStatistics {

	builder := &proto.ColumnStatistics{}

	if columnStatistics.HasNumberOfValues() {
		builder.NumberOfValues = columnStatistics.GetNumberOfValuesPtr()
	}
	if columnStatistics.GetBooleanStatistics() != nil {
		bs := &proto.BucketStatistics{}
		bs.Count = []uint64{uint64(columnStatistics.GetBooleanStatistics().GetTrueValueCount())}
		builder.BucketStatistics = bs
	}
	if columnStatistics.GetIntegerStatistics() != nil {

		integerStatistics := &proto.IntegerStatistics{}
		integerStatistics.Minimum = columnStatistics.GetIntegerStatistics().GetMinPtr()
		integerStatistics.Maximum = columnStatistics.GetIntegerStatistics().GetMaxPtr()

		// integerStatistics := MothProto.IntegerStatistics.newBuilder().setMinimum(columnStatistics.getIntegerStatistics().getMin()).setMaximum(columnStatistics.getIntegerStatistics().getMax())

		if columnStatistics.GetIntegerStatistics().hasSum {
			integerStatistics.Sum = columnStatistics.GetIntegerStatistics().GetSumPtr()
		}
		builder.IntStatistics = integerStatistics
	}
	if columnStatistics.GetDoubleStatistics() != nil {
		dobuleS := &proto.DoubleStatistics{}
		dobuleS.Minimum = columnStatistics.GetDoubleStatistics().GetMinPtr()
		dobuleS.Maximum = columnStatistics.GetDoubleStatistics().GetMaxPtr()
		builder.DoubleStatistics = dobuleS
	}
	if columnStatistics.GetStringStatistics() != nil {
		statisticsBuilder := &proto.StringStatistics{}

		if columnStatistics.GetStringStatistics().GetMin() != nil {
			s := columnStatistics.GetStringStatistics().GetMin().String()
			statisticsBuilder.Minimum = &s
		}
		if columnStatistics.GetStringStatistics().GetMax() != nil {
			s := columnStatistics.GetStringStatistics().GetMax().String()
			statisticsBuilder.Maximum = &s
		}
		statisticsBuilder.Sum = columnStatistics.GetStringStatistics().GetSumPtr()
		builder.StringStatistics = statisticsBuilder
	}
	if columnStatistics.GetDateStatistics() != nil {

		ds := &proto.DateStatistics{}
		ds.Maximum = columnStatistics.GetDateStatistics().GetMaxPtr()
		ds.Minimum = columnStatistics.GetDateStatistics().GetMinPtr()

		builder.DateStatistics = ds
	}
	if columnStatistics.GetTimestampStatistics() != nil {
		ds := &proto.TimestampStatistics{}
		ds.Maximum = columnStatistics.GetTimestampStatistics().GetMaxPtr()
		ds.Minimum = columnStatistics.GetTimestampStatistics().GetMinPtr()
		builder.TimestampStatistics = ds
	}
	if columnStatistics.GetDecimalStatistics() != nil {
		ds := &proto.DecimalStatistics{}
		ds.Maximum = columnStatistics.GetDecimalStatistics().GetMaxPtr()
		ds.Minimum = columnStatistics.GetDecimalStatistics().GetMinPtr()
		builder.DecimalStatistics = ds
	}
	if columnStatistics.GetBinaryStatistics() != nil {
		ds := &proto.BinaryStatistics{}
		ds.Sum = columnStatistics.GetBinaryStatistics().GetSumPtr()
		builder.BinaryStatistics = ds
	}
	return builder
}

func w_toUserMetadata(key string, value *slice.Slice) *proto.UserMetadataItem {
	um := &proto.UserMetadataItem{}
	um.Name = &key
	um.Value = value.AvailableBytes()
	return um
}

// @Override
func (mr *MothMetadataWriter) WriteStripeFooter(output slice.SliceOutput, footer *StripeFooter) int32 {
	footerProtobuf := &proto.StripeFooter{}

	streams := make([]*proto.Stream, footer.GetStreams().Size())
	for i, ss := range footer.GetStreams().ToArray() {
		streams[i] = w_toStream(ss)
	}
	footerProtobuf.Streams = streams

	columns := make([]*proto.ColumnEncoding, footer.GetColumnEncodings().Size())
	for i, ss := range footer.GetColumnEncodings().List().ToArray() {
		columns[i] = w_toColumnEncoding(ss)
	}
	footerProtobuf.Columns = columns
	timeZone := footer.GetTimeZone().String()
	footerProtobuf.WriterTimezone = &timeZone
	// footerProtobuf := MothProto.StripeFooter.newBuilder()
	// .addAllStreams(footer.getStreams().stream().Map(MothMetadataWriter.w_toStream).collect(toList()))
	// .addAllColumns(footer.getColumnEncodings().stream().Map(MothMetadataWriter.w_toColumnEncoding).collect(toList()))
	// .setWriterTimezone(footer.getTimeZone().getId()).build()
	return writeProtobufObject(output, footerProtobuf)
}

func w_toStream(stream *Stream) *proto.Stream {
	sm := &proto.Stream{}
	sm.Column = stream.GetColumnId().GetIdPtr()

	newKind := w_toStreamKind(stream.GetStreamKind())
	sm.Kind = &newKind

	sm.Length = stream.GetLengthPtr()
	return sm
}

func w_toStreamKind(streamKind StreamKind) proto.Stream_Kind {
	switch streamKind {
	case PRESENT:
		return proto.Stream_PRESENT
	case DATA:
		return proto.Stream_DATA
	case LENGTH:
		return proto.Stream_LENGTH
	case DICTIONARY_DATA:
		return proto.Stream_DICTIONARY_DATA
	case DICTIONARY_COUNT:
		return proto.Stream_DICTIONARY_COUNT
	case SECONDARY:
		return proto.Stream_SECONDARY
	case ROW_INDEX:
		return proto.Stream_ROW_INDEX
	case BLOOM_FILTER:
		// unsupported
	case BLOOM_FILTER_UTF8:
		return proto.Stream_BLOOM_FILTER_UTF8
	}
	panic(fmt.Sprintf("Unsupported stream kind: %d", streamKind))
}

func w_toColumnEncoding(columnEncodings *ColumnEncoding) *proto.ColumnEncoding {
	cg := &proto.ColumnEncoding{}

	newKind := w_toColumnEncoding2(columnEncodings.GetColumnEncodingKind())
	cg.Kind = &newKind

	cg.DictionarySize = columnEncodings.GetDictionarySizePtr()

	return cg
}

func w_toColumnEncoding2(columnEncodingKind ColumnEncodingKind) proto.ColumnEncoding_Kind {
	switch columnEncodingKind {
	case DIRECT:
		return proto.ColumnEncoding_DIRECT
	case DICTIONARY:
		return proto.ColumnEncoding_DICTIONARY
	case DIRECT_V2:
		return proto.ColumnEncoding_DIRECT_V2
	case DICTIONARY_V2:
		return proto.ColumnEncoding_DICTIONARY_V2
	}
	panic(fmt.Sprintf("Unsupported column encoding kind: %d", columnEncodingKind))
}

// @Override
func (mr *MothMetadataWriter) WriteRowIndexes(output slice.SliceOutput, rowGroupIndexes *util.ArrayList[*RowGroupIndex]) int32 {
	rowIndexProtobuf := &proto.RowIndex{}

	entrys := make([]*proto.RowIndexEntry, rowGroupIndexes.Size())

	for i, rx := range rowGroupIndexes.ToArray() {
		entrys[i] = w_toRowGroupIndex(rx)
	}

	rowIndexProtobuf.Entry = entrys
	return writeProtobufObject(output, rowIndexProtobuf)
}

func w_toRowGroupIndex(rowGroupIndex *RowGroupIndex) *proto.RowIndexEntry {
	rowIndexEntry := &proto.RowIndexEntry{}
	ps := make([]uint64, rowGroupIndex.GetPositions().Size())
	for i, p := range rowGroupIndex.GetPositions().ToArray() {
		ps[i] = uint64(p)
	}
	rowIndexEntry.Positions = ps

	rowIndexEntry.Statistics = w_toColumnStatistics(rowGroupIndex.GetColumnStatistics())
	return rowIndexEntry
}

// @Override
func (mr *MothMetadataWriter) WriteBloomFilters(output slice.SliceOutput, bloomFilters *util.ArrayList[*BloomFilter]) int32 {

	bloomFilterIndex := &proto.BloomFilterIndex{}

	bs := make([]*proto.BloomFilter, bloomFilters.Size())
	for i, b := range bloomFilters.ToArray() {
		bs[i] = toBloomFilter(b)
	}
	bloomFilterIndex.BloomFilter = bs

	return writeProtobufObject(output, bloomFilterIndex)
}

func toBloomFilter(bloomFilter *BloomFilter) *proto.BloomFilter {

	bf := &proto.BloomFilter{}

	bts := make([]uint64, len(bloomFilter.GetBitSet()))
	for i, b := range bloomFilter.GetBitSet() {
		bts[i] = uint64(b)
	}
	bf.Bitset = bts

	bf.NumHashFunctions = bloomFilter.GetNumHashFunctionsPtr()
	return bf
}

func w_toCompression(compressionKind CompressionKind) proto.CompressionKind {
	switch compressionKind {
	case NONE:
		return proto.CompressionKind_NONE
	case ZLIB:
		return proto.CompressionKind_ZLIB
	case SNAPPY:
		return proto.CompressionKind_SNAPPY
	case LZ4:
		return proto.CompressionKind_LZ4
	case ZSTD:
		return proto.CompressionKind_ZSTD
	}
	panic(fmt.Sprintf("Unsupported compression kind: %d", compressionKind))
}

func writeProtobufObject(output slice.SliceOutput, object protobuf.Message) int32 {

	b, err := protobuf.Marshal(object)
	if err == nil {
		size := util.Lens(b)
		output.WriteInt(size)
		output.WriteBS2(b, 0, size)
		return size
	} else {
		return 0
	}
}
