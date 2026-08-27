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
// An entry is due when its scheduled instant is not after now, i.e. AtNS <= nowNS,
// so tasks whose next_run lands exactly on the current tick boundary are
// dispatched this pass instead of being deferred to the next one.
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
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].Seq < out[j].Seq
		})
	}
	return out
}
