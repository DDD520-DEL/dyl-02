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

// TestReRegisterTypeDispatchesToNewHandler verifies a replaced handler is
// used for subsequent dispatches.
func TestReRegisterTypeDispatchesToNewHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	if err := reg.Register("greet", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte("v1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Replace("greet", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte("v2"), nil
	}); err != nil {
		t.Fatal(err)
	}
	lm := lease.New(10*time.Second, clk, m)
	nc := notify.New(retry.DefaultPolicy(), m)
	al := audit.New(10, clk)
	jobs := make(chan worker.Job, 4)
	pool := worker.NewPool(1, jobs, worker.Deps{
		Store: st, Registry: reg, Lease: lm, Policy: retry.DefaultPolicy(),
		Notify: nc, Audit: al, Clock: clk, Metrics: m,
	})
	go pool.Workers()[0].Run(ctx)
	disp := dispatch.New(st, lm, pool, reg, clk, m)
	sched := scheduler.New(st, ring, disp, lm, clk, m, time.Second)

	task := &model.Task{
		ID: "t1", Type: "greet", State: model.StatePending,
		CreatedAt: now, ScheduledAt: now, NextRunAt: now, Sequence: 1,
		Retry: &model.RetryConfig{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Minute, Multiplier: 1},
	}
	if err := st.Save(task); err != nil {
		t.Fatal(err)
	}
	ring.Enqueue(task.ID, task.Sequence, now)
	sched.Tick(now)

	deadline := time.Now().Add(3 * time.Second)
	for {
		got := st.Get("t1")
		if got != nil && got.State == model.StateSuccess {
			if string(got.Result) != "v2" {
				t.Fatalf("dispatched with stale handler, result=%q", got.Result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not complete, state=%+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
