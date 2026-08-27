package model

import (
	"fmt"
	"time"
)

// Schedule carries the user-facing timing options for a submitted task.
type Schedule struct {
	Delay       time.Duration
	RunAt       time.Time
	MaxAttempts int
}

// Normalize fills defaults and validates the schedule.
func (s *Schedule) Normalize(now time.Time) error {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 3
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > 100 {
		return fmt.Errorf("max_attempts must be between 1 and 100")
	}
	base := s.RunAt
	if base.IsZero() {
		base = now.Add(s.Delay)
	}
	if base.Before(now) {
		return fmt.Errorf("run time %s is in the past", base.Format(time.RFC3339))
	}
	s.RunAt = base
	return nil
}
