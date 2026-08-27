package verifycase

import (
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/retry"
)

// TestRetryBackoffFromLastAttempt verifies retries are scheduled from the
// most recent attempt rather than the original submission.
func TestRetryBackoffFromLastAttempt(t *testing.T) {
	created := time.Unix(100, 0)
	last := time.Unix(200, 0)
	task := &model.Task{
		CreatedAt:     created,
		LastAttemptAt: last,
		Attempts:      1,
		Retry: &model.RetryConfig{
			MaxAttempts: 3,
			BaseDelay:   60 * time.Second,
			MaxDelay:    time.Hour,
			Multiplier:  2,
		},
	}
	p := retry.FromConfig(task.Retry)
	next := p.NextRunAt(task)
	want := last.Add(60 * time.Second)
	if !next.Equal(want) {
		t.Fatalf("next run %s should be based on last attempt %s, not creation %s", next, want, created)
	}
}
