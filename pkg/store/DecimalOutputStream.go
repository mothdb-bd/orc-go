package store

import (
	"math/big"

	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/spi/block"
	"github.com/mothdb-bd/orc-go/pkg/store/metadata"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var DECIMAL_OUTPUT_INSTANCE_SIZE int32 = util.SizeOf(&DecimalOutputStream{})

type DecimalOutputStream struct {
	//继承
	ValueOutputStream[*DecimalStreamCheckpoint]

	buffer      *MothOutputBuffer
	checkpoints *util.ArrayList[*DecimalStreamCheckpoint]
	closed      bool
}

func NewDecimalOutputStream(compression metadata.CompressionKind, bufferSize int32) *DecimalOutputStream {
	dm := new(DecimalOutputStream)
	dm.buffer = NewMothOutputBuffer(compression, bufferSize)
	dm.checkpoints = util.NewArrayList[*DecimalStreamCheckpoint]()
	return dm
}

func (dm *DecimalOutputStream) WriteUnscaledValue(decimal *block.Int128) {
	value := decimal.AsBigInt()
	value = value.Lsh(value, 1)
	sign := value.Sign()
	if sign < 0 {
		value = value.Neg(value)
		value = value.Sub(value, big.NewInt(1))
	}
	length := value.BitLen() // .bitLength()
	for true {
		lowBits := value.Int64() & 0x7fff_ffff_ffff_ffff
		length -= 63
		for i := util.INT32_ZERO; i < 9; i++ {
			if length <= 0 && (lowBits & ^0x7f) == 0 {
				dm.buffer.Write(byte(lowBits))
				return
			} else {
				dm.buffer.Write((0x80 | byte(lowBits&0x7f)))
				lowBits = maths.UnsignedRightShift(lowBits, 7)
			}
		}
		value = value.Rsh(value, 63) //.shiftRight(63)
	}
}

func (dm *DecimalOutputStream) WriteUnscaledValue2(value int64) {
	util.CheckState(!dm.closed)
	WriteVLong(dm.buffer, value, true)
}

// @Override
func (dm *DecimalOutputStream) RecordCheckpoint() {
	util.CheckState(!dm.closed)
	dm.checkpoints.Add(NewDecimalStreamCheckpoint(dm.buffer.GetCheckpoint()))
}

// @Override
func (dm *DecimalOutputStream) Close() {
	dm.closed = true
	dm.buffer.Close()
}

// @Override
func (dm *DecimalOutputStream) GetCheckpoints() *util.ArrayList[*DecimalStreamCheckpoint] {
	util.CheckState(dm.closed)
	return dm.checkpoints
}

// @Override
func (dm *DecimalOutputStream) GetStreamDataOutput(columnId metadata.MothColumnId) *StreamDataOutput {
	return NewStreamDataOutput2(dm.buffer.WriteDataTo, metadata.NewStream(columnId, metadata.DATA, util.Int32Exact(dm.buffer.GetOutputDataSize()), true))
}

// @Override
func (dm *DecimalOutputStream) GetBufferedBytes() int64 {
	return dm.buffer.EstimateOutputDataSize()
}

// @Override
func (dm *DecimalOutputStream) GetRetainedBytes() int64 {
	return int64(DECIMAL_OUTPUT_INSTANCE_SIZE) + dm.buffer.GetRetainedSize()
}

// @Override
func (dm *DecimalOutputStream) Reset() {
	dm.closed = false
	dm.buffer.Reset()
	dm.checkpoints.Clear()
}
