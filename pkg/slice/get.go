package slice

import (
	"encoding/binary"
	"math"
)

// GetBool get a bool at given position
func (b *Slice) GetBool(i int) (bool, error) {
	if i < 0 || i > b.capacity-1 {
		return false, ErrOutOfBound
	}
	return b.buf[i] != 0, nil
}

// GetByte get a byte at given position
func (b *Slice) GetByte(i int) (byte, error) {
	if i < 0 || i > b.capacity-1 {
		return 0, ErrOutOfBound
	}
	return b.buf[i], nil
}

// GetByte get a byte at given position
func (b *Slice) GetByteInt32(i int32) byte {
	if i < 0 || i > int32(b.capacity)-1 {
		panic(ErrOutOfBound.Error())
	}
	return b.buf[i]
}

// GetBytes get bytes at given position, GetBytes(p, start), GetBytes(p, start, end)
func (b *Slice) GetBytes(p []byte, start int, indexes ...int) (int, error) {

	end := b.capacity
	if len(indexes) >= 1 && indexes[0] < end {
		end = indexes[0]
	}

	if start > end {
		return 0, ErrOutOfBound
	}

	copy(p, b.buf[start:end])
	return end - start, nil
}

// GetUInt8 get an uint8 at given position
func (b *Slice) GetUInt8(i int) (uint8, error) {
	if i < 0 || i > b.capacity-1 {
		return 0, ErrOutOfBound
	}
	return uint8(b.buf[i]), nil
}

// GetInt8 get an int8 at given position
func (b *Slice) GetInt8(i int) (int8, error) {
	if i < 0 || i > b.capacity-1 {
		return 0, ErrOutOfBound
	}
	return int8(b.buf[i]), nil
}

// GetUInt16BE get an uint16 in big endian at given position
func (b *Slice) GetUInt16BE(i int) (uint16, error) {
	if i < 0 || i > b.capacity-2 {
		return 0, ErrOutOfBound
	}
	return binary.BigEndian.Uint16(b.buf[i : i+2]), nil
}

// GetUInt16LE get an uint16 in little endian at given position
func (b *Slice) GetUInt16LE(i int) (uint16, error) {
	if i < 0 || i > b.capacity-2 {
		return 0, ErrOutOfBound
	}
	return binary.LittleEndian.Uint16(b.buf[i : i+2]), nil
}

// GetInt16BE get an int16 in big endian at given position
func (b *Slice) GetInt16BE(i int) (int16, error) {
	if i < 0 || i > b.capacity-2 {
		return 0, ErrOutOfBound
	}
	return int16(binary.BigEndian.Uint16(b.buf[i : i+2])), nil
}

// GetInt16LE get an int16 in little endian at given position
func (b *Slice) GetInt16LE(i int) (int16, error) {
	if i < 0 || i > b.capacity-2 {
		return 0, ErrOutOfBound
	}
	return int16(binary.LittleEndian.Uint16(b.buf[i : i+2])), nil
}

// GetUInt32BE get an uint32 in big endian at given position
func (b *Slice) GetUInt32BE(i int) (uint32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0, ErrOutOfBound
	}
	return binary.BigEndian.Uint32(b.buf[i : i+4]), nil
}

// GetUInt32LE get an uint32 in little endian at given position
func (b *Slice) GetUInt32LE(i int) (uint32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0, ErrOutOfBound
	}
	return binary.LittleEndian.Uint32(b.buf[i : i+4]), nil
}

// GetInt32BE get an int32 in big endian at given position
func (b *Slice) GetInt32BE(i int) (int32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0, ErrOutOfBound
	}
	return int32(binary.BigEndian.Uint32(b.buf[i : i+4])), nil
}

// GetInt32LE get an int32 in little endian at given position
func (b *Slice) GetInt32LE(i int) (int32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0, ErrOutOfBound
	}
	return int32(binary.LittleEndian.Uint32(b.buf[i : i+4])), nil
}

// GetUInt64BE get an uint64 in big endian at given position
func (b *Slice) GetUInt64BE(i int) (uint64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0, ErrOutOfBound
	}
	return binary.BigEndian.Uint64(b.buf[i : i+8]), nil
}

// GetUInt64LE get an uint64 in little endian at given position
func (b *Slice) GetUInt64LE(i int) (uint64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0, ErrOutOfBound
	}
	return binary.LittleEndian.Uint64(b.buf[i : i+8]), nil
}

// GetInt64BE get a int64 in big endian at given position
func (b *Slice) GetInt64BE(i int) (int64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0, ErrOutOfBound
	}
	return int64(binary.BigEndian.Uint64(b.buf[i : i+8])), nil
}

// GetInt64LE get a int64 in little endian at given position
func (b *Slice) GetInt64LE(i int) (int64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0, ErrOutOfBound
	}
	return int64(binary.LittleEndian.Uint64(b.buf[i : i+8])), nil
}

// GetFloat32BE get a float32 in big endian at given position
func (b *Slice) GetFloat32BE(i int) (float32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0.0, ErrOutOfBound
	}
	return math.Float32frombits(binary.BigEndian.Uint32(b.buf[i : i+4])), nil
}

// GetFloat32LE get a float32 in little endian at given position
func (b *Slice) GetFloat32LE(i int) (float32, error) {
	if i < 0 || i > b.capacity-4 {
		return 0.0, ErrOutOfBound
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b.buf[i : i+4])), nil
}

// GetFloat64BE get a float64 in big endian at given position
func (b *Slice) GetFloat64BE(i int) (float64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0.0, ErrOutOfBound
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b.buf[i : i+8])), nil
}

// GetFloat64LE get a float64 in little endian at given position
func (b *Slice) GetFloat64LE(i int) (float64, error) {
	if i < 0 || i > b.capacity-8 {
		return 0.0, ErrOutOfBound
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b.buf[i : i+8])), nil
}
