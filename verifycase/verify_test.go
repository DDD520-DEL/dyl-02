package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/queue"
)

// TestDueExactlyAtBoundaryDispatched verifies a task whose due instant equals
// the current scan instant is dispatched in the same round.
func TestDueExactlyAtBoundaryDispatched(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(now)
	ring := queue.NewRing(4, time.Second, clk)
	ring.Enqueue("t1", 1, now)
	got := ring.Drain(now)
	if len(got) != 1 {
		t.Fatalf("task due exactly at the boundary must be dispatched, got %d entries", len(got))
	}
}
