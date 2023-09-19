package stats

import (
	"time"
)

func OneMinute() float64 {

	// alpha for a target weight of 1/E at 1 minute
	return 1.0 / time.Minute.Seconds()
}

func FiveMinutes() float64 {
	// alpha for a target weight of 1/E at 5 minutes
	return 1.0 / (time.Minute.Seconds() * 5)
}

func FifteenMinutes() float64 {
	// alpha for a target weight of 1/E at 15 minutes
	return 1.0 / (time.Minute.Seconds() * 15)
}

func Seconds(seconds float64) float64 {
	// alpha for a target weight of 1/E at the specified number of seconds
	return 1.0 / seconds
}
