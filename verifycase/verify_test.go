package verifycase

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/wal"
)

// TestReplayRejectsCorruptRecord verifies WAL replay fails loudly instead of
// silently skipping a corrupted segment record.
func TestReplayRejectsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	m := metrics.New()
	wm, err := wal.NewManager(dir, 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := wm.Append(wal.Record{Event: wal.EventUpdate, TaskID: "x", Payload: []byte("ok")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wm.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a length prefix plus invalid compressed bytes to corrupt the tail.
	f, err := os.OpenFile(filepath.Join(dir, "000000.seg"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 4)
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	wm2, err := wal.NewManager(dir, 1<<20, m)
	if err != nil {
		t.Fatal(err)
	}
	defer wm2.Close()
	if err := wm2.Replay(func(wal.Record) error { return nil }); err == nil {
		t.Fatal("replay must reject a corrupted segment record")
	}
}
