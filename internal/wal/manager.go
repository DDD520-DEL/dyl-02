package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dyl-02/taskflow/internal/metrics"
)

// Manager owns the segment files and appends records to the active one.
type Manager struct {
	dir        string
	maxBytes   int64
	mu         sync.Mutex
	segments   []*segmentWriter
	active     *segmentWriter
	nextSeq    int
	metrics    *metrics.Metrics
	replayDone bool
}

// NewManager opens the WAL directory for append.
func NewManager(dir string, maxBytes int, m *metrics.Metrics) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	w := &Manager{
		dir:      dir,
		maxBytes: int64(maxBytes),
		metrics:  m,
	}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Manager) openActive() error {
	name := filepath.Join(w.dir, fmt.Sprintf("%06d.seg", w.nextSeq))
	seg, err := openSegment(name, w.maxBytes)
	if err != nil {
		return err
	}
	w.active = seg
	w.segments = append(w.segments, seg)
	if w.metrics != nil {
		w.metrics.OpenSegments.Add(1)
	}
	return nil
}

// Append writes a record to the active segment, rotating when full.
func (w *Manager) Append(rec Record) error {
	blob, err := rec.Encode()
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	written, err := w.active.append(blob)
	if err != nil {
		return err
	}
	if written >= w.maxBytes {
		if err := w.active.sync(); err != nil {
			return err
		}
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	return nil
}

// rotateLocked closes the current segment and opens the next one.
func (w *Manager) rotateLocked() error {
	w.nextSeq++
	if err := w.openActive(); err != nil {
		return err
	}
	return nil
}

// Close flushes and closes every segment.
func (w *Manager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var first error
	for _, s := range w.segments {
		if err := s.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Replay reads all segment files in order and applies each record once.
func (w *Manager) Replay(apply func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.replayDone {
		return nil
	}
	paths, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	sort.Slice(paths, func(i, j int) bool {
		return segmentOrder(paths[i]) < segmentOrder(paths[j])
	})
	for _, p := range paths {
		if err := readSegment(p, apply); err != nil {
			return err
		}
	}
	w.replayDone = true
	return nil
}

// Archive removes segments that are no longer the active tail.
func (w *Manager) Archive(keep int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keepFrom := len(w.segments) - keep
	if keepFrom <= 0 {
		return nil
	}
	for i := 0; i < keepFrom; i++ {
		seg := w.segments[i]
		if seg == w.active {
			break
		}
		_ = os.Remove(seg.path)
	}
	return nil
}

// SegmentCount returns the number of tracked open segments.
func (w *Manager) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segments)
}

func segmentOrder(path string) int {
	base := strings.TrimSuffix(filepath.Base(path), ".seg")
	n, _ := strconv.Atoi(base)
	return n
}
