package queue

import (
	"testing"
	"time"
)

// TestBucket_dueInclusiveBoundary guards against the off-by-one in the due
// comparison: a task whose AtNS equals the scan instant must be dispatched
// in the same pass, not deferred to the next tick. Previously the strict `<`
// left boundary-aligned tasks stranded in the bucket for one extra tick,
// causing delayed tasks to fire late.
func TestBucket_dueInclusiveBoundary(t *testing.T) {
	const nowNS = int64(1_700_000_000_000_000_000) // a fixed instant
	base := time.Unix(0, nowNS)

	t.Run("equal instant is due", func(t *testing.T) {
		b := bucket{}
		b.add(Entry{ID: "boundary", Seq: 1, AtNS: nowNS})
		b.add(Entry{ID: "future", Seq: 2, AtNS: nowNS + int64(time.Millisecond)})

		out := b.due(nowNS, false)
		if len(out) != 1 || out[0].ID != "boundary" {
			t.Fatalf("expected only the boundary entry to be due, got %+v", out)
		}
		if len(b.entries) != 1 || b.entries[0].ID != "future" {
			t.Fatalf("future entry should remain queued, got %+v", b.entries)
		}
	})

	t.Run("ring drains boundary at its tick", func(t *testing.T) {
		r := NewRing(4, time.Second, nil)
		r.Enqueue("boundary", 1, base)             // AtNS == nowNS
		r.Enqueue("future", 2, base.Add(time.Second)) // AtNS > nowNS

		got := r.Drain(base)
		if len(got) != 1 || got[0].ID != "boundary" {
			t.Fatalf("Drain must include the boundary-aligned entry, got %+v", got)
		}
	})
}

// TestRing_skippedTickRecovers guards against the second half of the boundary
// defect: when a tick fires more than one period late (time.Ticker drops
// lagged beats under load, or the prior tick's work overran TickInterval),
// the intervening bucket is skipped. A single-bucket Drain would strand the
// already-due task there until the ring wrapped a full BucketCount ticks back
// to its index — the delayed task firing seconds late. The drain must sweep
// every bucket it passed since the last scan so the skipped task is recovered
// in the very pass it became due.
func TestRing_skippedTickRecovers(t *testing.T) {
	r := NewRing(4, time.Second, nil)
	// Anchor on a clean second boundary so the ring index is well defined.
	base := time.Unix(1_700_000_000, 0)

	// B is due one tick from now: it lands in the next bucket over.
	r.Enqueue("B", 2, base.Add(time.Second))

	// First drain at base drains nothing; B is not due yet, but it stamps
	// lastDrainNS so the next drain knows where the cursor left off.
	if got := r.Drain(base); len(got) != 0 {
		t.Fatalf("nothing should be due at base, got %+v", got)
	}

	// The next tick fires late, landing two ticks from base and skipping the
	// bucket that owns B. B must still be drained this pass, not deferred.
	got := r.Drain(base.Add(2 * time.Second))
	if len(got) != 1 || got[0].ID != "B" {
		t.Fatalf("skipped-tick drain must recover the already-due entry, got %+v", got)
	}
}
