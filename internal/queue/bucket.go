package queue

import "sort"

// Entry is a queued reference to a scheduled task.
type Entry struct {
	ID   string
	Seq  int64
	AtNS int64
}

// bucket holds ordered entries for one slot of the ring.
type bucket struct {
	entries []Entry
}

func (b *bucket) add(e Entry) {
	b.entries = append(b.entries, e)
}

// due returns the entries due at nowNS and removes them from the bucket.
func (b *bucket) due(nowNS int64, bySequence bool) []Entry {
	kept := b.entries[:0]
	var out []Entry
	for _, e := range b.entries {
		if e.AtNS <= nowNS {
			out = append(out, e)
		} else {
			kept = append(kept, e)
		}
	}
	b.entries = kept
	if bySequence {
		// Order by the monotonic submit-time sequence so that tasks due at the
		// same instant are dispatched in the order they were submitted. Do NOT
		// sort by ID: the identifier is not guaranteed to be monotonic with
		// submission order (e.g. random/UUID schemes, multi-node node ids),
		// so ID ordering would break submit-order fairness.
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].Seq < out[j].Seq
		})
	}
	return out
}
