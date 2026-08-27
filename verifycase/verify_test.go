package verifycase

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/wal"
)

// TestWalSegmentsReleasedAfterArchive verifies archived WAL segments are
// closed and dropped from the manager.
func TestWalSegmentsReleasedAfterArchive(t *testing.T) {
	dir := t.TempDir()
	m := metrics.New()
	wm, err := wal.NewManager(dir, 1024, m)
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()
	payload := bytes.Repeat([]byte("a"), 200)
	for i := 0; i < 50; i++ {
		if err := wm.Append(wal.Record{Event: wal.EventUpdate, TaskID: "x", Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wm.Archive(1); err != nil {
		t.Fatalf("archive failed: %v", err)
	}
	if n := wm.SegmentCount(); n != 1 {
		t.Fatalf("expected exactly 1 live segment after archive, got %d", n)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(files) != 1 {
		t.Fatalf("expected 1 segment file on disk, got %d", len(files))
	}
}
