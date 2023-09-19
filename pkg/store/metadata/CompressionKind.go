package metadata

type CompressionKind int8

const (
	NONE CompressionKind = iota
	ZLIB
	SNAPPY
	LZ4
	ZSTD
)
