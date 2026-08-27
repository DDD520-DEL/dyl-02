package model

import "time"

// Result is the outcome recorded after a task execution attempt.
type Result struct {
	TaskID   string
	Success  bool
	Message  string
	Attempt  int
	Finished time.Time
}
