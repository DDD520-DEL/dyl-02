package clock

import "time"

// Clock abstracts time so scheduling logic can be tested deterministically.
type Clock interface {
	Now() time.Time
}

// Wall returns a clock backed by the real system time.
func Wall() Clock {
	return wallClock{}
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Manual is a controllable clock for tests and deterministic replay.
type Manual struct {
	now time.Time
}

// NewManual creates a manual clock starting at the given instant.
func NewManual(now time.Time) *Manual {
	return &Manual{now: now}
}

// Now returns the current fake instant.
func (m *Manual) Now() time.Time { return m.now }

// Advance moves the fake instant forward by d.
func (m *Manual) Advance(d time.Duration) {
	m.now = m.now.Add(d)
}

// Set replaces the fake instant.
func (m *Manual) Set(t time.Time) {
	m.now = t
}
