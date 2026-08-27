package store

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/wal"
)

// Store keeps the in-memory task view and mirrors every mutation to the WAL.
type Store struct {
	mu       sync.RWMutex
	tasks    map[string]*model.Task
	byState  map[model.State]map[string]struct{}
	sequence int64
	wal      *wal.Manager
	clock    clock.Clock
	metrics  *metrics.Metrics
}

// New creates an empty store backed by the given WAL.
func New(w *wal.Manager, clk clock.Clock, m *metrics.Metrics) *Store {
	return &Store{
		tasks:   make(map[string]*model.Task),
		byState: make(map[model.State]map[string]struct{}),
		wal:     w,
		clock:   clk,
		metrics: m,
	}
}

// Recover replays the WAL into memory.
//
// Any unrecognized or corrupt record aborts recovery: a partial/corrupt
// segment must surface a fatal error at startup instead of being silently
// skipped, otherwise tasks stored after the corrupt record would vanish.
func (s *Store) Recover() error {
	return s.wal.Replay(func(rec wal.Record) error {
		switch rec.Event {
		case wal.EventSubmit, wal.EventUpdate:
			var t model.Task
			if err := json.Unmarshal(rec.Payload, &t); err != nil {
				return fmt.Errorf("replay task %s: %w", rec.TaskID, err)
			}
			s.apply(&t)
		case wal.EventDelete:
			s.remove(rec.TaskID)
		default:
			// Defense in depth: Decode already rejects unknown events, but a
			// future caller path that bypasses validation must not silently
			// drop the record.
			return fmt.Errorf("replay record %s: unknown event %d", rec.TaskID, rec.Event)
		}
		return nil
	})
}

// NextSequence returns the next monotonic task sequence number.
func (s *Store) NextSequence() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	return s.sequence
}

// Save persists a new task and indexes it by state.
func (s *Store) Save(t *model.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := s.wal.Append(wal.Record{Event: wal.EventSubmit, TaskID: t.ID, Payload: payload}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply(t.Clone())
	return nil
}

// Update persists an in-place state transition.
func (s *Store) Update(t *model.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := s.wal.Append(wal.Record{Event: wal.EventUpdate, TaskID: t.ID, Payload: payload}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(t.ID)
	s.apply(t.Clone())
	return nil
}

// Delete removes a task entirely.
func (s *Store) Delete(id string) error {
	if err := s.wal.Append(wal.Record{Event: wal.EventDelete, TaskID: id}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(id)
	return nil
}

// Get returns a clone of the task, or nil when missing.
func (s *Store) Get(id string) *model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[id].Clone()
}

// ListByState returns clones of every task currently in the given state.
func (s *Store) ListByState(st model.State) []*model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byState[st]
	out := make([]*model.Task, 0, len(ids))
	for id := range ids {
		if t := s.tasks[id]; t != nil {
			out = append(out, t.Clone())
		}
	}
	return out
}

// CountByState returns the number of tasks in a state.
func (s *Store) CountByState(st model.State) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byState[st])
}

// All returns every task currently held, for snapshots and tests.
func (s *Store) All() []*model.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, t.Clone())
	}
	return out
}

func (s *Store) apply(t *model.Task) {
	s.tasks[t.ID] = t
	byState := s.byState[t.State]
	if byState == nil {
		byState = make(map[string]struct{})
		s.byState[t.State] = byState
	}
	byState[t.ID] = struct{}{}
	if t.Sequence > s.sequence {
		s.sequence = t.Sequence
	}
}

func (s *Store) remove(id string) {
	t := s.tasks[id]
	if t == nil {
		return
	}
	if byState := s.byState[t.State]; byState != nil {
		delete(byState, id)
		if len(byState) == 0 {
			delete(s.byState, t.State)
		}
	}
	delete(s.tasks, id)
}

// WAL exposes the underlying manager for maintenance operations.
func (s *Store) WAL() *wal.Manager {
	return s.wal
}

// Clock returns the store's time source.
func (s *Store) Clock() clock.Clock {
	return s.clock
}
