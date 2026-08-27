package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/lifecycle"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/store"
	"github.com/dyl-02/taskflow/internal/wal"
)

// TestRetentionKeepsActiveTasks verifies tasks still inside the retention
// window survive a purge.
func TestRetentionKeepsActiveTasks(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(now)
	m := metrics.New()
	wm, err := wal.NewManager(t.TempDir(), 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()
	st := store.New(wm, clk, m)

	recent := &model.Task{
		ID: "recent", Type: "echo", State: model.StateSuccess,
		CreatedAt: now.Add(-time.Hour), ScheduledAt: now.Add(-time.Hour),
		NextRunAt: now.Add(-10 * time.Minute), Sequence: 1,
	}
	old := &model.Task{
		ID: "old", Type: "echo", State: model.StateFailed,
		CreatedAt: now.Add(-3 * time.Hour), ScheduledAt: now.Add(-3 * time.Hour),
		NextRunAt: now.Add(-2 * time.Hour), Sequence: 2,
	}
	if err := st.Save(recent); err != nil {
		t.Fatal(err)
	}
	if err := st.Save(old); err != nil {
		t.Fatal(err)
	}
	p := lifecycle.New(st, clk, time.Hour, m)
	if err := p.Purge(); err != nil {
		t.Fatal(err)
	}
	if st.Get("recent") == nil {
		t.Fatal("task still inside retention window was purged")
	}
}
