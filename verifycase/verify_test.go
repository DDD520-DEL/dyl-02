package verifycase

import (
	"context"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/dispatch"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/retry"
	"github.com/dyl-02/taskflow/internal/scheduler"
	"github.com/dyl-02/taskflow/internal/store"
	"github.com/dyl-02/taskflow/internal/wal"
	"github.com/dyl-02/taskflow/internal/worker"
)

// TestLeaseExpiryReclaimsRunningTask verifies that a running task whose
// worker lease expired is returned to the pending queue.
func TestLeaseExpiryReclaimsRunningTask(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(now)
	m := metrics.New()
	wm, err := wal.NewManager(t.TempDir(), 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()
	st := store.New(wm, clk, m)
	ring := queue.NewRing(8, time.Second, clk)
	reg := registry.New()
	if err := reg.Register("echo", func(_ context.Context, payload []byte) ([]byte, error) {
		return payload, nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = audit.New(10, clk)
	lm := lease.New(10*time.Second, clk, m)
	nc := notify.New(retry.DefaultPolicy(), m)
	jobs := make(chan worker.Job, 4)
	pool := worker.NewPool(1, jobs, worker.Deps{
		Store: st, Registry: reg, Lease: lm, Policy: retry.DefaultPolicy(),
		Notify: nc, Audit: audit.New(10, clk), Clock: clk, Metrics: m,
	})
	disp := dispatch.New(st, lm, pool, reg, clk, m)
	sched := scheduler.New(st, ring, disp, lm, clk, m, time.Second)

	task := &model.Task{
		ID: "t1", Type: "echo", State: model.StateRunning,
		CreatedAt: now, ScheduledAt: now, NextRunAt: now, Sequence: 1,
	}
	if err := st.Save(task); err != nil {
		t.Fatal(err)
	}
	_ = lm.Grant("t1", "w0")
	clk.Advance(11 * time.Second)
	if err := sched.Reconcile(clk.Now()); err != nil {
		t.Fatal(err)
	}
	got := st.Get("t1")
	if got == nil || got.State != model.StatePending {
		t.Fatalf("expired running task was not reclaimed: %+v", got)
	}
	if ring.Pending() != 1 {
		t.Fatalf("expected one requeued entry, got %d", ring.Pending())
	}
}
