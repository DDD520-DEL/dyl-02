package model

import "time"

// State describes the lifecycle of a scheduled task.
type State string

const (
	StatePending       State = "pending"
	StateRunning       State = "running"
	StateWaitingRetry  State = "waiting_retry"
	StateSuccess       State = "success"
	StateFailed        State = "failed"
	StateCancelled     State = "cancelled"
)

// Terminal reports whether the state is a terminal one that no longer
// participates in scheduling.
func (s State) Terminal() bool {
	return s == StateSuccess || s == StateFailed || s == StateCancelled
}

// Active reports whether the state can still be picked up by the scheduler.
func (s State) Active() bool {
	return s == StatePending || s == StateWaitingRetry
}

// Task is the unit of work managed by the scheduler.
type Task struct {
	ID            string
	Type          string
	Payload       []byte
	State         State
	CreatedAt     time.Time
	ScheduledAt   time.Time
	NextRunAt     time.Time
	Attempts      int
	MaxAttempts   int
	LastAttemptAt time.Time
	NotifyURL     string
	Retry         *RetryConfig
	Result        []byte
	Sequence      int64
	Lease         Lease
}

// RetryConfig is the serializable retry schedule attached to a task.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// Lease records the worker currently executing the task and its fencing epoch.
type Lease struct {
	Owner     string
	ExpiresAt time.Time
	Epoch     int64
}

// Clone returns a deep copy of the task so callers cannot mutate the stored
// instance through a returned reference.
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	c := *t
	c.Payload = append([]byte(nil), t.Payload...)
	c.Result = append([]byte(nil), t.Result...)
	return &c
}

// DueAt reports whether the task is ready to run at the given instant.
func (t *Task) DueAt(now time.Time) bool {
	return t.State.Active() && !t.NextRunAt.After(now)
}
