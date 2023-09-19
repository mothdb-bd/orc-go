package metadata

import "github.com/mothdb-bd/orc-go/pkg/slice"

var MAGIC string = "MOTH"
var MAGIC_SLICE = slice.NewWithString("MOTH")

type HiveWriterVersion int8

const (
	ORIGINAL HiveWriterVersion = iota
	MOTH_HIVE_8732
)

type PostScript struct {
	version              []uint32
	footerLength         int64
	metadataLength       int64
	compression          CompressionKind
	compressionBlockSize uint64
	hiveWriterVersion    HiveWriterVersion
}

func NewPostScript(version []uint32, footerLength int64, metadataLength int64, compression CompressionKind, compressionBlockSize uint64, hiveWriterVersion HiveWriterVersion) *PostScript {
	pt := new(PostScript)
	pt.version = version
	pt.footerLength = footerLength
	pt.metadataLength = metadataLength
	pt.compression = compression
	pt.compressionBlockSize = compressionBlockSize
	pt.hiveWriterVersion = hiveWriterVersion
	return pt
}

func (pt *PostScript) GetVersion() []uint32 {
	return pt.version
}

func (pt *PostScript) GetFooterLength() int64 {
	return pt.footerLength
}

func (pt *PostScript) GetMetadataLength() int64 {
	return pt.metadataLength
}

func (pt *PostScript) GetCompression() CompressionKind {
	return pt.compression
}

func (pt *PostScript) GetCompressionBlockSize() uint64 {
	return pt.compressionBlockSize
}

func (pt *PostScript) GetHiveWriterVersion() HiveWriterVersion {
	return pt.hiveWriterVersion
}
