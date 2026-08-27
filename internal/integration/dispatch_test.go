// Package integration exercises the dispatch->worker->lease pipeline
// concurrently to guard against data races and duplicate execution.
package integration

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
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

// TestSchedulerConcurrentLeaseNoDuplicateExecution runs the real single-threaded
// scheduler Tick with several concurrent workers, a lease that is shorter than
// the handler so the reconciler re-dispatches while the original is still
// running, and asserts that no two workers ever execute the same task at once.
// This reproduces the reported "dirty lease -> duplicate execution" failure.
//
// The wall clock is used (rather than the non-thread-safe Manual clock) so the
// only state under test is the lease/concurrency path itself.
func TestSchedulerConcurrentLeaseNoDuplicateExecution(t *testing.T) {
	dir := t.TempDir()
	wm, err := wal.NewManager(dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("wal: %v", err)
	}
	defer wm.Close()

	clk := clock.Wall()
	m := metrics.New()
	st := store.New(wm, clk, m)
	if err := st.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Lease shorter than the handler so the reconciler reclaims mid-flight.
	leaseDuration := 20 * time.Millisecond
	tickInterval := 5 * time.Millisecond
	lm := lease.New(leaseDuration, clk, m)

	reg := registry.New()
	// Track concurrent execution per task ID.
	var inflight sync.Map // taskID -> *int32
	var duplicate atomic.Int64
	var execs atomic.Int64
	if err := reg.Register("echo", func(_ context.Context, payload []byte) ([]byte, error) {
		id := string(payload)
		execs.Add(1)
		v, _ := inflight.LoadOrStore(id, new(int32))
		n := atomic.AddInt32(v.(*int32), 1)
		defer atomic.AddInt32(v.(*int32), -1)
		if n > 1 {
			duplicate.Add(1)
		}
		// Hold the handler longer than the lease so a reclaim lands mid-flight.
		time.Sleep(60 * time.Millisecond)
		return payload, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	policy := retry.DefaultPolicy()
	notifyClient := notify.New(policy, m)
	auditLog := audit.New(100, clk)
	jobs := make(chan worker.Job, 64)
	dep := worker.Deps{Store: st, Registry: reg, Lease: lm, Policy: policy, Notify: notifyClient, Audit: auditLog, Clock: clk, Metrics: m}
	pool := worker.NewPool(4, jobs, dep)
	for _, w := range pool.Workers() {
		go w.Run(ctx)
	}

	disp := dispatch.New(st, lm, pool, reg, clk, m)
	ring := queue.NewRing(64, tickInterval, clk)
	sched := scheduler.New(st, ring, disp, lm, clk, m, tickInterval)

	var requeued atomic.Int64
	beforeMetrics := m.Requeued.Load()

	// Submit a handful of tasks.
	const tasks = 8
	for i := 0; i < tasks; i++ {
		task := &model.Task{
			ID:        "task-" + strconv.Itoa(i),
			Type:      "echo",
			Payload:   []byte("task-" + strconv.Itoa(i)),
			State:     model.StatePending,
			CreatedAt: clk.Now(),
			NextRunAt: clk.Now(),
			Sequence:  st.NextSequence(),
		}
		if err := st.Save(task); err != nil {
			t.Fatalf("save: %v", err)
		}
		ring.Enqueue(task.ID, task.Sequence, task.NextRunAt)
	}

	go sched.Run(ctx)

	<-ctx.Done()
	close(jobs)

	requeued.Store(m.Requeued.Load() - beforeMetrics)
	t.Logf("total executions=%d duplicate=%d requeued=%d", execs.Load(), duplicate.Load(), requeued.Load())
	if got := duplicate.Load(); got > 0 {
		t.Fatalf("duplicate concurrent execution detected: %d (total executions=%d)", got, execs.Load())
	}
	if requeued.Load() == 0 {
		t.Fatalf("test did not exercise the reconcile/re-dispatch path: requeued=0")
	}
}
