package verifycase

import (
	"context"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/api"
	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/idgen"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/retry"
	"github.com/dyl-02/taskflow/internal/store"
	"github.com/dyl-02/taskflow/internal/wal"
)

// TestCancelWithoutPolicyNoPanic verifies cancelling a task that carries no
// retry schedule does not panic while delivering its completion webhook.
func TestCancelWithoutPolicyNoPanic(t *testing.T) {
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
	if err := api.RegisterBuiltins(reg); err != nil {
		t.Fatal(err)
	}
	nc := notify.New(retry.DefaultPolicy(), m)
	srv := api.New(api.Deps{
		Store: st, Ring: ring, Reg: reg, IDs: idgen.New(1), Notify: nc,
		Clock: clk, Metrics: m, Audit: audit.New(10, clk),
		Limits: store.BatchLimits{MaxTasks: 100, MaxPayload: 1 << 20,
			KnownTypes: map[string]bool{"echo": true, "noop": true, "fail-once": true}},
	})
	task, err := srv.Submit(context.Background(), api.TaskInput{
		Type:      "echo",
		Payload:   []byte("x"),
		DelayMS:   1000,
		NotifyURL: "http://127.0.0.1:1/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.Retry != nil {
		t.Fatal("expected task without retry schedule")
	}
	if err := srv.Cancel(context.Background(), task.ID); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	got := st.Get(task.ID)
	if got == nil || got.State != model.StateCancelled {
		t.Fatalf("task not cancelled: %+v", got)
	}
}
