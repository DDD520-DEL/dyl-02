package audit

import (
	"sync"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/model"
)

// Entry is one recorded transition in a task's history.
type Entry struct {
	TaskID  string
	From    model.State
	To      model.State
	At      time.Time
	Owner   string
	Message string
}

// Log keeps a bounded per-task transition history.
type Log struct {
	mu      sync.Mutex
	entries map[string][]Entry
	limit   int
	clock   clock.Clock
}

// New creates a transition log with the given per-task capacity.
func New(limit int, clk clock.Clock) *Log {
	return &Log{
		entries: make(map[string][]Entry),
		limit:   limit,
		clock:   clk,
	}
}

// Record appends a transition for a task, dropping the oldest entry when the
// per-task limit is exceeded.
func (l *Log) Record(taskID string, from, to model.State, owner, message string) {
	if taskID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	hist := l.entries[taskID]
	hist = append(hist, Entry{
		TaskID:  taskID,
		From:    from,
		To:      to,
		At:      l.clock.Now(),
		Owner:   owner,
		Message: message,
	})
	if len(hist) > l.limit {
		hist = hist[len(hist)-l.limit:]
	}
	l.entries[taskID] = hist
}

// History returns a copy of the transitions recorded for a task.
func (l *Log) History(taskID string) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	hist := l.entries[taskID]
	out := make([]Entry, len(hist))
	copy(out, hist)
	return out
}

// TaskIDs returns every task that has at least one recorded transition.
func (l *Log) TaskIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]string, 0, len(l.entries))
	for id := range l.entries {
		ids = append(ids, id)
	}
	return ids
}
