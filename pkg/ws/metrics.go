package ws

import (
	"sync/atomic"
	"time"
)

var (
	totalMessages atomic.Uint64
	totalErrors   atomic.Uint64
	startedAt     = time.Now()
)

func RecordMessage() {
	totalMessages.Add(1)
}

func RecordError() {
	totalErrors.Add(1)
}

func MetricsSnapshot(activeConnections int) (messagesPerSecond float64, wsErrors uint64) {
	age := time.Since(startedAt).Seconds()
	if age <= 0 {
		age = 1
	}
	return float64(totalMessages.Load()) / age, totalErrors.Load()
}
