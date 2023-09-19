package store

type ByteArrayInputStream struct {
	// 继承
	ValueInputStream[*ByteArrayStreamCheckpoint]

	inputStream *MothInputStream
}

func NewByteArrayInputStream(inputStream *MothInputStream) *ByteArrayInputStream {
	bm := new(ByteArrayInputStream)
	bm.inputStream = inputStream
	return bm
}

func (bm *ByteArrayInputStream) Next(length int) []byte {
	data := make([]byte, length)
	bm.Next2(data, 0, length)
	return data
}

func (bm *ByteArrayInputStream) Next2(data []byte, offset int, length int) {
	bm.inputStream.ReadFully(data, offset, length)
}

//@Override
func (bm *ByteArrayInputStream) SeekToCheckpoint(checkpoint StreamCheckpoint) {
	bt := checkpoint.(*ByteArrayStreamCheckpoint)
	bm.inputStream.SeekToCheckpoint(bt.GetInputStreamCheckpoint())
}

//@Override
func (bm *ByteArrayInputStream) Skip(skipSize int64) {
	bm.inputStream.SkipFully(skipSize)
}
