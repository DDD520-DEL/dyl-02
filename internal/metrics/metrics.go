package metrics

import (
	"sync"
	"sync/atomic"
)

// Metrics aggregates counters and gauges used by operations tooling.
type Metrics struct {
	Submitted     atomic.Int64
	Dispatched    atomic.Int64
	Completed     atomic.Int64
	Failed        atomic.Int64
	Cancelled     atomic.Int64
	Requeued      atomic.Int64
	LeaseExpired  atomic.Int64
	OpenSegments  atomic.Int64
	PurgedTasks   atomic.Int64
	Notifications atomic.Int64

	mu       sync.Mutex
	lastTick int64
}

// New creates an empty metrics registry.
func New() *Metrics {
	return &Metrics{}
}

// RecordTick stores the tick counter for observability.
func (m *Metrics) RecordTick(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTick = n
}

// Tick returns the last recorded tick counter.
func (m *Metrics) Tick() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTick
}
