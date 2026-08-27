package dispatch

import (
	"fmt"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/store"
	"github.com/dyl-02/taskflow/internal/worker"
)

// Dispatcher grants leases and routes tasks to workers.
type Dispatcher struct {
	store   *store.Store
	lease   *lease.Manager
	pool    *worker.Pool
	reg     *registry.Registry
	clock   clock.Clock
	metrics *metrics.Metrics
}

// New creates a dispatcher.
func New(st *store.Store, lm *lease.Manager, pool *worker.Pool, reg *registry.Registry, clk clock.Clock, m *metrics.Metrics) *Dispatcher {
	return &Dispatcher{store: st, lease: lm, pool: pool, reg: reg, clock: clk, metrics: m}
}

// Dispatch grants a lease for the task and hands it to a worker.
func (d *Dispatcher) Dispatch(task *model.Task) error {
	if !task.State.Active() {
		return fmt.Errorf("task %s is not dispatchable in state %s", task.ID, task.State)
	}
	if _, err := d.reg.Resolve(task.Type); err != nil {
		return err
	}
	w := d.pool.Next()
	if w == nil {
		return fmt.Errorf("no worker available")
	}
	l := d.lease.Grant(task.ID, w.ID())
	task.Lease = l
	if err := d.store.Update(task); err != nil {
		return err
	}
	ch := w.Send()
	select {
	case ch <- worker.Job{Task: task, Lease: l}:
		return nil
	default:
		_ = d.lease.Expire(task.ID, w.ID(), l.Epoch)
		return fmt.Errorf("worker %s is busy", w.ID())
	}
}
