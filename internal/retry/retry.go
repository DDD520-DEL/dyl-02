package retry

import (
	"time"

	"github.com/dyl-02/taskflow/internal/model"
)

// Policy controls how failed attempts are rescheduled.
type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// FromConfig builds a policy from a task's serializable retry config.
func FromConfig(c *model.RetryConfig) *Policy {
	if c == nil {
		return nil
	}
	return &Policy{
		MaxAttempts: c.MaxAttempts,
		BaseDelay:   c.BaseDelay,
		MaxDelay:    c.MaxDelay,
		Multiplier:  c.Multiplier,
	}
}

// DefaultPolicy returns the built-in exponential backoff policy.
func DefaultPolicy() *Policy {
	return &Policy{
		MaxAttempts: 3,
		BaseDelay:   time.Second,
		MaxDelay:    5 * time.Minute,
		Multiplier:  2,
	}
}

// CanRetry reports whether another attempt is allowed.
func (p *Policy) CanRetry(task *model.Task) bool {
	if p == nil {
		return false
	}
	return task.Attempts < p.MaxAttempts
}

// NextRunAt computes the next execution instant based on the most recent
// attempt, so retries spread out from the last failure rather than from the
// original submission.
func (p *Policy) NextRunAt(task *model.Task) time.Time {
	base := task.LastAttemptAt
	if base.IsZero() {
		base = task.CreatedAt
	}
	delay := p.BaseDelay
	for i := 1; i < task.Attempts; i++ {
		delay = time.Duration(float64(delay) * p.Multiplier)
		if delay > p.MaxDelay {
			delay = p.MaxDelay
			break
		}
	}
	return base.Add(delay)
}
