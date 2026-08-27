package scheduler

import (
	"time"

	"github.com/dyl-02/taskflow/internal/model"
)

// Reconcile reclaims tasks whose worker lease has expired and returns them
// to the pending queue so they can be dispatched again.
//
// A task that crashed while running stays in StateRunning; it is never
// pending, so we must scan running tasks too, otherwise an expired lease on
// a running task is never observed and the task sticks in "running" forever.
func (s *Scheduler) Reconcile(now time.Time) error {
	for _, t := range s.store.ListByState(model.StateRunning) {
		l, ok := s.lease.Lookup(t.ID)
		if ok && !s.lease.IsExpired(l, now) {
			// Still owned by a live worker; leave it alone.
			continue
		}
		// Lease missing or expired: the worker is gone or stalled. Reset
		// the task to pending so it can be picked up again.
		t.State = model.StatePending
		t.NextRunAt = now
		t.Lease = model.Lease{}
		if err := s.store.Update(t); err != nil {
			return err
		}
		s.ring.Enqueue(t.ID, t.Sequence, now)
		if s.metrics != nil {
			s.metrics.Requeued.Add(1)
		}
	}
	return nil
}
