package mothio

type DataInput interface {
	ReadFully(b []byte)
	ReadFully2(b []byte, off int, len int)
	SkipBytes(n int32) int32
	ReadBool() bool
	ReadByte() byte
	ReadUnsignedByte() uint8
	ReadShort() int16
	ReadUnsignedShort() uint16
	ReadChar() int8
	ReadInt() int32
	ReadLong() int64
	ReadFloat() float32
	ReadDouble() float64
	ReadLine() string
	ReadUTF() string
}
