package store

import (
	"sort"

	"github.com/dyl-02/taskflow/internal/model"
)

// BySequence returns tasks sorted by their monotonic sequence number.
func BySequence(tasks []*model.Task) []*model.Task {
	out := append([]*model.Task(nil), tasks...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Sequence < out[j].Sequence
	})
	return out
}
