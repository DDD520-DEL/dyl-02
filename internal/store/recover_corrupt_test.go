package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/wal"
	"github.com/golang/snappy"
)

func writeSegRaw(t *testing.T, dir, name string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
		t.Fatalf("write seg: %v", err)
	}
}

func lenPrefixedRaw(blob []byte) []byte {
	out := make([]byte, 4+len(blob))
	binary.BigEndian.PutUint32(out[:4], uint32(len(blob)))
	copy(out[4:], blob)
	return out
}

// corruptEventBlob builds a record blob whose Event byte holds an unknown
// value (0xFE), simulating a single corrupted record byte. The surrounding
// length prefix and snappy framing are valid, so the record reaches the decode
// layer where the event must be rejected.
func corruptEventBlob(id string, event byte) []byte {
	header := make([]byte, wal.HeaderSize())
	binary.BigEndian.PutUint32(header[0:4], uint32(len(id)))
	header[4] = event
	binary.BigEndian.PutUint32(header[5:9], 0)
	raw := append(header, id...)
	return snappy.Encode(nil, raw)
}

// TestRecoverUnknownEventAbortsStartup verifies that a record whose event byte
// was corrupted to an unknown value aborts recovery with a fatal error instead
// of being silently dropped.
func TestRecoverUnknownEventAbortsStartup(t *testing.T) {
	dir := t.TempDir()

	corrupt := lenPrefixedRaw(corruptEventBlob("t1", 0xFE))
	writeSegRaw(t, dir, "000000.seg", corrupt)

	mgr, err := wal.NewManager(dir, 64<<20, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	st := New(mgr, clock.NewManual(time.Now()), nil)
	if err := st.Recover(); err == nil {
		t.Fatalf("Recover returned nil for an unknown-event record; corruption silently dropped")
	}
}

// TestRecoverCorruptRecordAbortsAndStopsReplay reproduces the reported failure
// mode: a corrupt record sits between two valid records. Before the fix, replay
// silently skipped the corrupt record and kept replaying the records after it
// as if nothing had happened, so the corrupt segment produced no error yet
// tasks were lost. After the fix, recovery must abort at the corrupt record.
func TestRecoverCorruptRecordAbortsAndStopsReplay(t *testing.T) {
	dir := t.TempDir()

	good1, err := (wal.Record{Event: wal.EventSubmit, TaskID: "t1", Payload: []byte(`{"id":"t1","state":"pending"}`)}).Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	good2, err := (wal.Record{Event: wal.EventSubmit, TaskID: "t2", Payload: []byte(`{"id":"t2","state":"pending"}`)}).Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	corrupt := corruptEventBlob("ghost", 0xFE)

	var seg []byte
	seg = append(seg, lenPrefixedRaw(good1)...)
	seg = append(seg, lenPrefixedRaw(corrupt)...)
	seg = append(seg, lenPrefixedRaw(good2)...)
	writeSegRaw(t, dir, "000000.seg", seg)

	mgr, err := wal.NewManager(dir, 64<<20, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	st := New(mgr, clock.NewManual(time.Now()), nil)
	err = st.Recover()
	if err == nil {
		t.Fatalf("Recover silently skipped the corrupt record and continued (saw %d tasks); expected fatal error", len(st.All()))
	}
	// t1 was applied before the corrupt record; t2 must NOT have been replayed
	// past the corruption point.
	ids := make(map[string]bool)
	for _, tk := range st.All() {
		ids[tk.ID] = true
	}
	if !ids["t1"] {
		t.Errorf("expected t1 to be replayed before the corrupt record; got %v", ids)
	}
	if ids["t2"] {
		t.Errorf("t2 was replayed past the corrupt record; recovery should have stopped at the corruption")
	}
}
