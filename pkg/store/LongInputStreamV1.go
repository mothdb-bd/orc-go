package store

import (
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	LONGV1_MIN_REPEAT_SIZE  int32 = 3
	LONGV1_MAX_LITERAL_SIZE int32 = 128
)

type LongInputStreamV1 struct {
	//继承
	LongInputStream

	input                   *MothInputStream
	signed                  bool
	literals                []int64
	numLiterals             int32
	delta                   int32
	used                    int32
	repeat                  bool
	lastReadInputCheckpoint int64
}

func NewLongInputStreamV1(input *MothInputStream, signed bool) *LongInputStreamV1 {
	l1 := new(LongInputStreamV1)

	l1.literals = make([]int64, LONGV1_MAX_LITERAL_SIZE)
	l1.input = input
	l1.signed = signed
	l1.lastReadInputCheckpoint = input.GetCheckpoint()
	return l1
}

func (l1 *LongInputStreamV1) readValues() {
	l1.lastReadInputCheckpoint = l1.input.GetCheckpoint()
	control, err := l1.input.ReadBS()

	if err != nil {
		panic("Read past end of RLE integer")
	}
	controlInt := int32(control)
	if control < 0x80 {
		l1.numLiterals = controlInt + LONGV1_MIN_REPEAT_SIZE
		l1.used = 0
		l1.repeat = true
		delta, err := l1.input.ReadBS()
		if err != nil {
			panic("End of stream in RLE Integer")
		}
		l1.delta = int32(byte(delta))
		l1.literals[0] = ReadVInt(l1.signed, l1.input)
	} else {
		l1.numLiterals = 0x100 - controlInt
		l1.used = 0
		l1.repeat = false
		for i := util.INT32_ZERO; i < l1.numLiterals; i++ {
			l1.literals[i] = ReadVInt(l1.signed, l1.input)
		}
	}
}

// @Override
func (l1 *LongInputStreamV1) Next() int64 {
	var result int64
	if l1.used == l1.numLiterals {
		l1.readValues()
	}
	if l1.repeat {
		result = l1.literals[0] + int64(l1.used*l1.delta)
		l1.used++
	} else {
		result = l1.literals[l1.used]
		l1.used++
	}
	return result
}

// @Override
func (l1 *LongInputStreamV1) Next2(values []int64, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l1.used == l1.numLiterals {
			l1.numLiterals = 0
			l1.used = 0
			l1.readValues()
		}
		chunkSize := maths.MinInt32(l1.numLiterals-l1.used, items)
		if l1.repeat {
			for i := util.INT32_ZERO; i < chunkSize; i++ {
				values[offset+i] = l1.literals[0] + int64((l1.used+i)*l1.delta)
			}
		} else {
			// System.arraycopy(literals, used, values, offset, chunkSize)
			util.CopyInt64s(l1.literals, l1.used, values, offset, chunkSize)
		}
		l1.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l1 *LongInputStreamV1) Next3(values []int32, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l1.used == l1.numLiterals {
			l1.numLiterals = 0
			l1.used = 0
			l1.readValues()
		}
		chunkSize := maths.MinInt32(l1.numLiterals-l1.used, items)
		if l1.repeat {
			for i := util.INT32_ZERO; i < chunkSize; i++ {
				literal := l1.literals[0] + int64((l1.used+i)*l1.delta)
				value := int32(literal)
				if int32(literal) != value {
					panic("Decoded value out of range for a 32bit number")
				}
				values[offset+i] = value
			}
		} else {
			for i := util.INT32_ZERO; i < chunkSize; i++ {
				literal := l1.literals[l1.used+i]
				value := int32(literal)
				if int32(literal) != value {
					panic("Decoded value out of range for a 32bit number")
				}
				values[offset+i] = value
			}
		}
		l1.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l1 *LongInputStreamV1) Next4(values []int16, items int32) {
	offset := util.INT32_ZERO
	for items > 0 {
		if l1.used == l1.numLiterals {
			l1.numLiterals = 0
			l1.used = 0
			l1.readValues()
		}
		chunkSize := maths.MinInt32(l1.numLiterals-l1.used, items)
		if l1.repeat {
			for i := util.INT32_ZERO; i < chunkSize; i++ {
				literal := l1.literals[0] + int64((l1.used+i)*l1.delta)
				value := int16(literal)
				if literal != int64(value) {
					panic("Decoded value out of range for a 16bit number")
				}
				values[offset+i] = value
			}
		} else {
			for i := util.INT32_ZERO; i < chunkSize; i++ {
				literal := l1.literals[l1.used+i]
				value := int16(literal)
				if literal != int64(value) {
					panic("Decoded value out of range for a 16bit number")
				}
				values[offset+i] = value
			}
		}
		l1.used += chunkSize
		offset += chunkSize
		items -= chunkSize
	}
}

// @Override
func (l1 *LongInputStreamV1) SeekToCheckpoint(cp StreamCheckpoint) {
	v1Checkpoint := cp.(*LongStreamV1Checkpoint)
	if l1.lastReadInputCheckpoint == v1Checkpoint.GetInputStreamCheckpoint() && v1Checkpoint.GetOffset() <= l1.numLiterals {
		l1.used = v1Checkpoint.GetOffset()
	} else {
		l1.input.SeekToCheckpoint(v1Checkpoint.GetInputStreamCheckpoint())
		l1.numLiterals = 0
		l1.used = 0
		l1.Skip(int64(v1Checkpoint.GetOffset()))
	}
}

// @Override
func (l1 *LongInputStreamV1) Skip(items int64) {
	for items > 0 {
		if l1.used == l1.numLiterals {
			l1.readValues()
		}
		consume := maths.Min(items, int64(l1.numLiterals-l1.used))
		l1.used += int32(consume)
		items -= int64(consume)
	}
}

// @Override
func (l1 *LongInputStreamV1) String() string {
	return "LongInputStreamV1"
}
