package queue

import (
	"sort"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
)

// Ring is a fixed-size delay queue split into time buckets.
type Ring struct {
	buckets []bucket
	tick    time.Duration
	clock   clock.Clock

	// lastDrainNS is the instant of the previous Drain, so the next Drain can
	// sweep every bucket the scan cursor passed over since then. Without it a
	// tick that lands more than one period late (time.Ticker drops lagged beats
	// under load) would leave already-due tasks stranded in the skipped
	// buckets until the ring wrapped a full BucketCount ticks back around.
	lastDrainNS int64
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
//
// It sweeps every bucket from just past the previous scan point up to and
// including the bucket that owns `now`. This is the key fix for the boundary
// defect the team was seeing: when a tick fires more than one period late
// (time.Ticker drops lagged beats under load, or the prior tick's work
// overran TickInterval), the intervening buckets are skipped. With a
// single-bucket read those already-due tasks would strand until the ring
// wrapped a full BucketCount ticks back to their index — late tasks firing
// seconds late. Sweeping the gap guarantees that anything whose AtNS is not
// after now is dispatched this pass, including tasks that landed exactly on
// the current tick boundary.
func (r *Ring) Drain(now time.Time) []Entry {
	nowNS := now.UnixNano()
	end := r.idx(nowNS)
	var out []Entry

	// First drain ever, or the clock ran backwards / stood still: read only
	// the bucket that owns `now` and do not treat anything as "skipped".
	if r.lastDrainNS <= 0 || nowNS <= r.lastDrainNS {
		out = append(out, r.buckets[end].due(nowNS, false)...)
		r.lastDrainNS = nowNS
		sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
		return out
	}

	n := len(r.buckets)
	start := r.idx(r.lastDrainNS)

	// sweep is the number of buckets to drain, starting just after the last
	// scan point. Compute it from elapsed wall time, not from the index delta,
	// so that more than a full ring period elapsed collapses into a whole-ring
	// sweep rather than silently skipping buckets.
	sweep := (nowNS - r.lastDrainNS) / r.tick.Nanoseconds()
	switch {
	case sweep >= int64(n):
		// At least one whole ring period passed: every bucket has been lapped,
		// so drain them all once.
		sweep = int64(n)
	case sweep < 1:
		sweep = 1
	}
	// Advance from start+1 so we never re-drain the bucket that was already
	// drained last time (unless the whole ring was lapped, in which case start
	// itself gets covered as part of the full sweep).
	for k := int64(1); k <= sweep; k++ {
		i := (start + int(k)) % n
		out = append(out, r.buckets[i].due(nowNS, false)...)
	}
	// Ensure the bucket owning `now` is always covered: when the elapsed time
	// rounded down to zero periods but `end` differs from the last sweep
	// position, drain it explicitly.
	if sweep < int64(n) {
		endCovered := (start + int(sweep)) % n
		if endCovered != end {
			out = append(out, r.buckets[end].due(nowNS, false)...)
		}
	}

	r.lastDrainNS = nowNS
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Pending reports how many entries are currently queued.
func (r *Ring) Pending() int {
	total := 0
	for i := range r.buckets {
		total += len(r.buckets[i].entries)
	}
	return total
}
