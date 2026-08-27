package scheduler

import (
	"context"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/dispatch"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/store"
)

// Scheduler drains due buckets and hands tasks to the dispatcher.
type Scheduler struct {
	store    *store.Store
	ring     *queue.Ring
	disp     *dispatch.Dispatcher
	lease    *lease.Manager
	clock    clock.Clock
	metrics  *metrics.Metrics
	tick     time.Duration
}

// New creates a scheduler.
func New(st *store.Store, ring *queue.Ring, d *dispatch.Dispatcher,
	lm *lease.Manager, clk clock.Clock, m *metrics.Metrics, tick time.Duration) *Scheduler {
	return &Scheduler{
		store:   st,
		ring:    ring,
		disp:    d,
		lease:   lm,
		clock:   clk,
		metrics: m,
		tick:    tick,
	}
}

// Run ticks on the configured interval until the context is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.Tick(now)
		}
	}
}

// Tick performs one scheduling pass at the given instant.
func (s *Scheduler) Tick(now time.Time) {
	for _, e := range s.ring.Drain(now) {
		t := s.store.Get(e.ID)
		if t == nil || !t.State.Active() {
			continue
		}
		_ = s.disp.Dispatch(t)
	}
	_ = s.Reconcile(now)
	if s.metrics != nil {
		s.metrics.RecordTick(now.UnixNano())
	}
}

// EnqueueDue schedules a task for the given instant.
func (s *Scheduler) EnqueueDue(t *model.Task, at time.Time) {
	s.ring.Enqueue(t.ID, t.Sequence, at)
}
