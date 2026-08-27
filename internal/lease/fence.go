package lease

import "github.com/dyl-02/taskflow/internal/model"

// Fence validates that a worker still owns a task before it mutates state.
type Fence struct {
	Manager *Manager
}

// Valid returns true when the provided lease matches the stored one.
func (f *Fence) Valid(taskID string, l model.Lease) bool {
	cur, ok := f.Manager.Lookup(taskID)
	if !ok {
		return false
	}
	return cur.Owner == l.Owner && cur.Epoch == l.Epoch
}
