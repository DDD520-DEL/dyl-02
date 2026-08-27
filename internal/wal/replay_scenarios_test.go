package wal

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestReplayStructuralCorruptionMustAbort verifies that every form of
// structural corruption encountered mid-segment aborts replay with an error
// instead of silently skipping the record and the records that follow.
//
// These cases already errored before the fix; they are kept as a regression
// guard so that a future "tolerate tail corruption" change cannot reintroduce
// silent data loss.
func TestReplayStructuralCorruptionMustAbort(t *testing.T) {
	good1 := lenPrefixed(recBlob(t, Record{Event: EventSubmit, TaskID: "t1", Payload: []byte("{}")}))
	good2 := lenPrefixed(recBlob(t, Record{Event: EventSubmit, TaskID: "t2", Payload: []byte("{}")}))

	scenarios := []struct {
		name string
		seg  []byte
	}{
		{
			name: "truncated-length-prefix-at-tail",
			seg:  append(append([]byte{}, good1...), 0xAB, 0xCD), // partial 4-byte length, then EOF
		},
		{
			name: "truncated-body-at-tail",
			seg: func() []byte {
				// length prefix claims a large body, but file ends early
				var b bytes.Buffer
				b.Write(good1)
				var lp [4]byte
				binary.BigEndian.PutUint32(lp[:], 1000)
				b.Write(lp[:])
				b.Write([]byte("short"))
				return b.Bytes()
			}(),
		},
		{
			name: "zero-length-record-size",
			seg: func() []byte {
				var b bytes.Buffer
				b.Write(good1)
				var lp [4]byte // size == 0
				b.Write(lp[:])
				b.Write(good2)
				return b.Bytes()
			}(),
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSeg(t, dir, "000000.seg", sc.seg)
			mgr, err := NewManager(dir, 64<<20, nil)
			if err != nil {
				t.Fatalf("new manager: %v", err)
			}
			defer mgr.Close()
			var seen []string
			err = mgr.Replay(func(rec Record) error {
				seen = append(seen, rec.TaskID)
				return nil
			})
			if err == nil {
				t.Fatalf("replay returned nil for %s; expected a corruption error (seen=%v)", sc.name, seen)
			}
		})
	}
}
