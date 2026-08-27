package store

import (
	"fmt"

	"github.com/dyl-02/taskflow/internal/model"
)

// BatchLimits are the constraints enforced before any task in a batch is saved.
type BatchLimits struct {
	MaxTasks    int
	MaxPayload  int
	KnownTypes  map[string]bool
}

// ValidateBatch checks every task without mutating the store.
func ValidateBatch(tasks []*model.Task, limits BatchLimits) error {
	if len(tasks) == 0 {
		return fmt.Errorf("batch must contain at least one task")
	}
	if len(tasks) > limits.MaxTasks {
		return fmt.Errorf("batch size %d exceeds limit %d", len(tasks), limits.MaxTasks)
	}
	seen := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		if t.ID == "" {
			return fmt.Errorf("task id must not be empty")
		}
		if _, dup := seen[t.ID]; dup {
			return fmt.Errorf("duplicate task id %s in batch", t.ID)
		}
		seen[t.ID] = struct{}{}
		if len(t.Payload) > limits.MaxPayload {
			return fmt.Errorf("task %s payload exceeds %d bytes", t.ID, limits.MaxPayload)
		}
		if limits.KnownTypes != nil && !limits.KnownTypes[t.Type] {
			return fmt.Errorf("task %s has unknown type %q", t.ID, t.Type)
		}
	}
	return nil
}
