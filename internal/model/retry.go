package model

import "time"

// Normalize fills missing fields with defaults.
func (c *RetryConfig) Normalize() {
	if c == nil {
		return
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = time.Second
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 5 * time.Minute
	}
	if c.Multiplier <= 0 {
		c.Multiplier = 2
	}
}
