package lease

import (
	"sync"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
)

// Manager tracks the lease of each running task.
type Manager struct {
	mu       sync.Mutex
	leases   map[string]model.Lease
	duration time.Duration
	clock    clock.Clock
	metrics  *metrics.Metrics
}

// New creates a lease manager.
func New(duration time.Duration, clk clock.Clock, m *metrics.Metrics) *Manager {
	return &Manager{
		leases:   make(map[string]model.Lease),
		duration: duration,
		clock:    clk,
		metrics:  m,
	}
}

// Grant assigns the task to an owner, always advancing the fencing epoch.
func (m *Manager) Grant(taskID, owner string) model.Lease {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.leases[taskID]
	next.Epoch++
	next.Owner = owner
	next.ExpiresAt = m.clock.Now().Add(m.duration)
	m.leases[taskID] = next
	return next
}

// Renew extends the lease but only when the caller still owns the current
// generation. A stale owner or epoch is rejected so a late renewal cannot
// clobber a newer assignment.
func (m *Manager) Renew(taskID, owner string, epoch int64) (model.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.leases[taskID]
	if !ok {
		return model.Lease{}, ErrNoLease
	}
	if cur.Owner != owner || cur.Epoch != epoch {
		return model.Lease{}, ErrStaleLease
	}
	cur.ExpiresAt = m.clock.Now().Add(m.duration)
	m.leases[taskID] = cur
	return cur, nil
}

// Expire removes the lease only if it belongs to the given generation.
func (m *Manager) Expire(taskID, owner string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.leases[taskID]
	if !ok {
		return ErrNoLease
	}
	if cur.Owner != owner || cur.Epoch != epoch {
		return ErrStaleLease
	}
	delete(m.leases, taskID)
	if m.metrics != nil {
		m.metrics.LeaseExpired.Add(1)
	}
	return nil
}

// Lookup returns the current lease of a task.
func (m *Manager) Lookup(taskID string) (model.Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[taskID]
	return l, ok
}

// IsExpired reports whether the task's lease has lapsed at the given instant.
func (m *Manager) IsExpired(l model.Lease, now time.Time) bool {
	return !l.ExpiresAt.After(now)
}

var (
	// ErrNoLease is returned when a task has no lease at all.
	ErrNoLease = errNoLease{}
	// ErrStaleLease is returned when the caller no longer owns the lease.
	ErrStaleLease = errStaleLease{}
)

type errNoLease struct{}

func (errNoLease) Error() string { return "no lease for task" }

type errStaleLease struct{}

func (errStaleLease) Error() string { return "lease belongs to a newer generation" }
