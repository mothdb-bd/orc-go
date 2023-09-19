package store

import (
	"fmt"
	"sync/atomic"

	"github.com/mothdb-bd/orc-go/pkg/util"
)

type MothWriterStats struct {
	allFlush            *MothWriterFlushStats
	maxRowsFlush        *MothWriterFlushStats
	maxBytesFlush       *MothWriterFlushStats
	dictionaryFullFlush *MothWriterFlushStats
	closedFlush         *MothWriterFlushStats

	// atomic
	writerSizeInBytes *int64
}

// public enum FlushReason
// {
// 	MAX_ROWS, MAX_BYTES, DICTIONARY_FULL, CLOSED
// }

type FlushReason int8

const (
	MAX_ROWS FlushReason = iota
	MAX_BYTES
	DICTIONARY_FULL
	CLOSED
)

func NewMothWriterStats() *MothWriterStats {
	ms := new(MothWriterStats)
	ms.allFlush = NewMothWriterFlushStats("ALL")
	ms.maxRowsFlush = NewMothWriterFlushStats("MAX_ROWS")
	ms.maxBytesFlush = NewMothWriterFlushStats("MAX_BYTES")
	ms.dictionaryFullFlush = NewMothWriterFlushStats("DICTIONARY_FULL")
	ms.closedFlush = NewMothWriterFlushStats("CLOSED")
	ms.writerSizeInBytes = new(int64)
	return ms
}

func (ms *MothWriterStats) RecordStripeWritten(flushReason FlushReason, stripeBytes int64, stripeRows int32, dictionaryBytes int32) {
	ms.getFlushStats(flushReason).RecordStripeWritten(stripeBytes, stripeRows, dictionaryBytes)
	ms.allFlush.RecordStripeWritten(stripeBytes, stripeRows, dictionaryBytes)
}

func (ms *MothWriterStats) UpdateSizeInBytes(deltaInBytes int64) {
	atomic.AddInt64(ms.writerSizeInBytes, deltaInBytes)
}

// @Managed
// @Nested
func (ms *MothWriterStats) GetAllFlush() *MothWriterFlushStats {
	return ms.allFlush
}

// @Managed
// @Nested
func (ms *MothWriterStats) GetMaxRowsFlush() *MothWriterFlushStats {
	return ms.maxRowsFlush
}

// @Managed
// @Nested
func (ms *MothWriterStats) GetMaxBytesFlush() *MothWriterFlushStats {
	return ms.maxBytesFlush
}

// @Managed
// @Nested
func (ms *MothWriterStats) GetDictionaryFullFlush() *MothWriterFlushStats {
	return ms.dictionaryFullFlush
}

// @Managed
// @Nested
func (ms *MothWriterStats) GetClosedFlush() *MothWriterFlushStats {
	return ms.closedFlush
}

// @Managed
func (ms *MothWriterStats) GetWriterSizeInBytes() int64 {
	return *ms.writerSizeInBytes
}

func (ms *MothWriterStats) getFlushStats(flushReason FlushReason) *MothWriterFlushStats {
	switch flushReason {
	case MAX_ROWS:
		return ms.maxRowsFlush
	case MAX_BYTES:
		return ms.maxBytesFlush
	case DICTIONARY_FULL:
		return ms.dictionaryFullFlush
	case CLOSED:
		return ms.closedFlush
	}
	panic(fmt.Sprintf("unknown flush reason %d", flushReason))
}

// @Override
func (ms *MothWriterStats) String() string {
	return util.NewSB().AddString("allFlush", ms.allFlush.String()).AddString("maxRowsFlush", ms.maxRowsFlush.String()).AddString("maxBytesFlush", ms.maxBytesFlush.String()).AddString("dictionaryFullFlush", ms.dictionaryFullFlush.String()).AddString("closedFlush", ms.closedFlush.String()).AddInt64("writerSizeInBytes", *ms.writerSizeInBytes).String()
}
