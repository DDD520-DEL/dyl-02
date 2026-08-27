package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/queue"
)

// TestDueTasksDispatchInSequenceOrder verifies due tasks are handed out in
// monotonic sequence order regardless of identifier ordering.
func TestDueTasksDispatchInSequenceOrder(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(now)
	ring := queue.NewRing(4, time.Second, clk)
	ring.Enqueue("task-b", 1, now)
	ring.Enqueue("task-a", 2, now)
	got := ring.Drain(clk.Now())
	if len(got) != 2 {
		t.Fatalf("expected 2 due entries, got %d", len(got))
	}
	if got[0].ID != "task-b" || got[1].ID != "task-a" {
		t.Fatalf("dispatch order not by sequence: %+v", got)
	}
}
