package store

import "time"

var SYSTEM_TICKER Ticker = new(SystemTicker)
var NO_TICKER Ticker = new(NoOpTicker)

type Ticker interface {
	Read() int64
}

func GetSystemTicker() Ticker {
	return SYSTEM_TICKER
}

func GetNoicker() Ticker {
	return NO_TICKER
}

type SystemTicker struct {
	// 继承
	Ticker
}

func (tr *SystemTicker) Read() int64 {
	return time.Now().UnixNano()
}

//
type NoOpTicker struct {
	// 继承
	Ticker
}

func (tr *NoOpTicker) Read() int64 {
	return 0
}
