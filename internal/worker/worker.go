package worker

import (
	"context"
	"fmt"

	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/retry"
	"github.com/dyl-02/taskflow/internal/store"
)

// Job couples a dispatched task with the lease that authorizes execution.
type Job struct {
	Task  *model.Task
	Lease model.Lease
}

// Worker executes dispatched tasks and drives their state transitions.
type Worker struct {
	id      string
	jobs    chan Job
	store   *store.Store
	reg     *registry.Registry
	lease   *lease.Manager
	fence   *lease.Fence
	policy  *retry.Policy
	notify  *notify.Client
	audit   *audit.Log
	clock   clock.Clock
	metrics *metrics.Metrics
}

// New creates a worker that consumes from the given channel.
func New(id string, jobs chan Job, st *store.Store, reg *registry.Registry,
	lm *lease.Manager, policy *retry.Policy, nc *notify.Client, al *audit.Log, clk clock.Clock, m *metrics.Metrics) *Worker {
	return &Worker{
		id:      id,
		jobs:    jobs,
		store:   st,
		reg:     reg,
		lease:   lm,
		fence:   &lease.Fence{Manager: lm},
		policy:  policy,
		notify:  nc,
		audit:   al,
		clock:   clk,
		metrics: m,
	}
}

// ID returns the worker identity.
func (w *Worker) ID() string { return w.id }

// Run processes jobs until the context is cancelled.
func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.jobs:
			w.execute(ctx, job)
		}
	}
}

func (w *Worker) execute(ctx context.Context, job Job) {
	task := job.Task
	if !w.fence.Valid(task.ID, job.Lease) {
		// The lease was reassigned; this execution is stale and must not
		// touch task state.
		return
	}
	task.State = model.StateRunning
	if err := w.store.Update(task); err != nil {
		return
	}
	if w.audit != nil {
		w.audit.Record(task.ID, model.StatePending, model.StateRunning, w.id, "dispatched")
	}
	if w.metrics != nil {
		w.metrics.Dispatched.Add(1)
	}
	handler, err := w.reg.Resolve(task.Type)
	if err != nil {
		w.finish(ctx, task, model.Result{TaskID: task.ID, Success: false, Message: err.Error(), Attempt: task.Attempts + 1, Finished: w.clock.Now()})
		return
	}
	out, execErr := handler(ctx, task.Payload)
	task.Attempts++
	task.LastAttemptAt = w.clock.Now()
	if execErr == nil {
		task.Result = out
		w.finish(ctx, task, model.Result{TaskID: task.ID, Success: true, Message: "ok", Attempt: task.Attempts, Finished: task.LastAttemptAt})
		return
	}
	task.Result = []byte(execErr.Error())
	w.finish(ctx, task, model.Result{TaskID: task.ID, Success: false, Message: execErr.Error(), Attempt: task.Attempts, Finished: task.LastAttemptAt})
}

func (w *Worker) finish(ctx context.Context, task *model.Task, result model.Result) {
	policy := retry.FromConfig(task.Retry)
	if policy == nil {
		policy = w.policy
	}
	if result.Success {
		task.State = model.StateSuccess
		_ = w.store.Update(task)
		if w.audit != nil {
			w.audit.Record(task.ID, model.StateRunning, model.StateSuccess, w.id, "completed")
		}
		if w.metrics != nil {
			w.metrics.Completed.Add(1)
		}
	} else if policy != nil && policy.CanRetry(task) {
		task.State = model.StateWaitingRetry
		task.NextRunAt = policy.NextRunAt(task)
		_ = w.store.Update(task)
		if w.audit != nil {
			w.audit.Record(task.ID, model.StateRunning, model.StateWaitingRetry, w.id, "scheduled retry")
		}
		if w.metrics != nil {
			w.metrics.Requeued.Add(1)
		}
	} else {
		task.State = model.StateFailed
		_ = w.store.Update(task)
		if w.audit != nil {
			w.audit.Record(task.ID, model.StateRunning, model.StateFailed, w.id, "failed")
		}
		if w.metrics != nil {
			w.metrics.Failed.Add(1)
		}
	}
	_ = w.notify.Notify(ctx, task, result)
	_ = w.lease.Expire(task.ID, w.id, task.Lease.Epoch)
}

// String renders the worker identity for logs.
func (w *Worker) String() string {
	return fmt.Sprintf("worker-%s", w.id)
}
