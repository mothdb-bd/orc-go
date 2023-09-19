package slice

import (
	"encoding/binary"
	"math"
)

// PutBool put a bool at given position
func (b *Slice) PutBool(i int, val bool) error {
	if i+1 > b.capacity {
		return ErrOutOfBound
	}
	if val {
		b.buf[i] = 0x01
	} else {
		b.buf[i] = 0x00
	}
	return nil
}

// PutByte put a byte at given position
func (b *Slice) PutByte(i int, val byte) error {
	if i+1 > b.capacity {
		return ErrOutOfBound
	}
	b.buf[i] = val
	return nil
}

// PutBytes put a bool at given position
func (b *Slice) PutBytes(i int, p []byte) error {
	if len(p)+i > b.capacity {
		return ErrOutOfBound
	}
	copy(b.buf[i:], p)
	return nil
}

// PutUInt8 put an uint8 at given position
func (b *Slice) PutUInt8(i int, val uint8) error {
	if i+1 > b.capacity {
		return ErrOutOfBound
	}
	b.buf[i] = byte(val)
	return nil
}

// PutInt8 put an int8 at given position
func (b *Slice) PutInt8(i int, val int8) error {
	if i+1 > b.capacity {
		return ErrOutOfBound
	}
	b.buf[i] = byte(val)
	return nil
}

// PutUInt16BE put an uint16 at given position in big endian
func (b *Slice) PutUInt16BE(i int, val uint16) error {
	if i+2 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint16(b.buf[i:], val)
	return nil
}

// PutUInt16LE put an uint16 at given position in little endian
func (b *Slice) PutUInt16LE(i int, val uint16) error {
	if i+2 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint16(b.buf[i:], val)
	return nil
}

// PutInt16BE put an int16 at given position in big endian
func (b *Slice) PutInt16BE(i int, val int16) error {
	if i+2 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint16(b.buf[i:], uint16(val))
	return nil
}

// PutInt16LE put an int16 at given position in little endian
func (b *Slice) PutInt16LE(i int, val int16) error {
	if i+2 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint16(b.buf[i:], uint16(val))
	return nil
}

// PutUInt32BE put an uint32 at given position in big endian
func (b *Slice) PutUInt32BE(i int, val uint32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint32(b.buf[i:], val)
	return nil
}

// PutUInt32LE put an uint32 at given position in little endian
func (b *Slice) PutUInt32LE(i int, val uint32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint32(b.buf[i:], val)
	return nil
}

// PutInt32BE put an int32 at given position in big endian
func (b *Slice) PutInt32BE(i int, val int32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint32(b.buf[i:], uint32(val))
	return nil
}

// PutInt32LE put an int32 at given position in little endian
func (b *Slice) PutInt32LE(i int, val int32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint32(b.buf[i:], uint32(val))
	return nil
}

// PutUInt64BE put an uint64 at given position in big endian
func (b *Slice) PutUInt64BE(i int, val uint64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint64(b.buf[i:], val)
	return nil
}

// PutUInt64LE put an uint64 at given position in little endian
func (b *Slice) PutUInt64LE(i int, val uint64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint64(b.buf[i:], val)
	return nil
}

// PutInt64BE put an int64 at given position in big endian
func (b *Slice) PutInt64BE(i int, val int64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint64(b.buf[i:], uint64(val))
	return nil
}

// PutInt64LE put an int64 at given position in little endian
func (b *Slice) PutInt64LE(i int, val int64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint64(b.buf[i:], uint64(val))
	return nil
}

// PutFloat32BE put a float32 at given position in big endian
func (b *Slice) PutFloat32BE(i int, val float32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint32(b.buf[i:], math.Float32bits(val))
	return nil
}

// PutFloat32LE put a float32 at given position in little endian
func (b *Slice) PutFloat32LE(i int, val float32) error {
	if i+4 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint32(b.buf[i:], math.Float32bits(val))
	return nil
}

// PutFloat64BE put a float64 at given position in big endian
func (b *Slice) PutFloat64BE(i int, val float64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.BigEndian.PutUint64(b.buf[i:], math.Float64bits(val))
	return nil
}

// PutFloat64LE put a float64 at given position in little endian
func (b *Slice) PutFloat64LE(i int, val float64) error {
	if i+8 > b.capacity {
		return ErrOutOfBound
	}
	binary.LittleEndian.PutUint64(b.buf[i:], math.Float64bits(val))
	return nil
}
