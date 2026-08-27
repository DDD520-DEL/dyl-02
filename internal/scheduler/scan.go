package scheduler

import (
	"time"

	"github.com/dyl-02/taskflow/internal/model"
)

// Reconcile reclaims tasks whose worker lease has expired and returns them
// to the pending queue so they can be dispatched again.
func (s *Scheduler) Reconcile(now time.Time) error {
	for _, t := range s.store.ListByState(model.StatePending) {
		l, ok := s.lease.Lookup(t.ID)
		if ok && !s.lease.IsExpired(l, now) {
			continue
		}
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
