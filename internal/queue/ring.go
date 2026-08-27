package queue

import (
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
)

// Ring is a fixed-size delay queue split into time buckets.
type Ring struct {
	buckets []bucket
	tick    time.Duration
	clock   clock.Clock
}

// NewRing creates a ring with the given bucket count and bucket width.
func NewRing(n int, tick time.Duration, clk clock.Clock) *Ring {
	return &Ring{
		buckets: make([]bucket, n),
		tick:    tick,
		clock:   clk,
	}
}

func (r *Ring) idx(ns int64) int {
	if r.tick <= 0 {
		return 0
	}
	n := int((ns / r.tick.Nanoseconds()) % int64(len(r.buckets)))
	if n < 0 {
		n += len(r.buckets)
	}
	return n
}

// Enqueue places an entry into the bucket that owns its due instant.
func (r *Ring) Enqueue(id string, seq int64, at time.Time) {
	ns := at.UnixNano()
	i := r.idx(ns)
	r.buckets[i].add(Entry{ID: id, Seq: seq, AtNS: ns})
}

// Drain returns all entries due at the current instant, ordered by sequence.
func (r *Ring) Drain(now time.Time) []Entry {
	i := r.idx(now.UnixNano())
	return r.buckets[i].due(now.UnixNano(), true)
}

// Pending reports how many entries are currently queued.
func (r *Ring) Pending() int {
	total := 0
	for i := range r.buckets {
		total += len(r.buckets[i].entries)
	}
	return total
}
