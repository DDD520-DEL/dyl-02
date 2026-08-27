package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// writeSeg writes a raw byte stream (already in length-prefixed form) to a
// segment file named 000000.seg under dir.
func writeSeg(t *testing.T, dir, name string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}
}

func recBlob(t *testing.T, r Record) []byte {
	t.Helper()
	b, err := r.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func lenPrefixed(blob []byte) []byte {
	out := make([]byte, 4+len(blob))
	binary.BigEndian.PutUint32(out[:4], uint32(len(blob)))
	copy(out[4:], blob)
	return out
}

// TestReplayCorruptRecordMustAbort verifies that encountering a corrupt record
// during replay surfaces an error instead of silently skipping it (and the
// records that follow).
func TestReplayCorruptRecordMustAbort(t *testing.T) {
	dir := t.TempDir()

	good1 := lenPrefixed(recBlob(t, Record{Event: EventSubmit, TaskID: "t1", Payload: []byte("{}")}))
	good2 := lenPrefixed(recBlob(t, Record{Event: EventSubmit, TaskID: "t2", Payload: []byte("{}")}))
	good3 := lenPrefixed(recBlob(t, Record{Event: EventSubmit, TaskID: "t3", Payload: []byte("{}")}))

	// A corrupt record: valid 4-byte length prefix, but a body that is not a
	// valid snappy-compressed record.
	corrupt := lenPrefixed([]byte("this-is-not-a-valid-snappy-blob"))

	var seg bytes.Buffer
	seg.Write(good1)
	seg.Write(corrupt)
	seg.Write(good2)
	seg.Write(good3)
	writeSeg(t, dir, "000000.seg", seg.Bytes())

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

	// Expectation: replay MUST return an error and abort. It must not silently
	// skip the corrupt record and continue replaying good2/good3.
	if err == nil {
		t.Fatalf("replay returned nil error on a corrupt record; saw tasks %v (silent skip detected)", seen)
	}
	if len(seen) != 1 || seen[0] != "t1" {
		t.Fatalf("expected replay to abort after t1 (saw %v), got %v", []string{"t1"}, seen)
	}
}
