package mothio

type DataOutput interface {
	Write(b byte)
	WriteBytes(b []byte)
	WriteBytes2(b []byte, off int32, len int32)
	WriteBoolean(v bool)
	WriteByte(v byte) error
	WriteShort(v int16)
	WriteChar(v byte)
	WriteInt(v int32)
	WriteLong(v int64)
	WriteFloat(v float32)
	WriteDouble(v float64)
	WriteString(s string)
	WriteChars(s string)
	WriteUTF(s string)
}
