package lifecycle

import (
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/store"
)

// Purger removes terminal tasks that have fallen entirely out of the
// retention window.
type Purger struct {
	store  *store.Store
	clock  clock.Clock
	window time.Duration
	metrics *metrics.Metrics
}

// New creates a retention purger.
func New(st *store.Store, clk clock.Clock, window time.Duration, m *metrics.Metrics) *Purger {
	return &Purger{store: st, clock: clk, window: window, metrics: m}
}

// Expired reports whether a task's next run is completely outside the
// retention window.
func (p *Purger) Expired(t *model.Task, now time.Time) bool {
	if p.window <= 0 {
		return false
	}
	return !t.NextRunAt.Before(now.Add(-p.window))
}

// Purge deletes terminal tasks that no longer fall inside the window.
func (p *Purger) Purge() error {
	now := p.clock.Now()
	for _, st := range []model.State{model.StateSuccess, model.StateFailed, model.StateCancelled} {
		for _, t := range p.store.ListByState(st) {
			if !p.Expired(t, now) {
				continue
			}
			if err := p.store.Delete(t.ID); err != nil {
				return err
			}
			if p.metrics != nil {
				p.metrics.PurgedTasks.Add(1)
			}
		}
	}
	return nil
}
