package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// segmentWriter appends length-prefixed, compressed records to one segment file.
type segmentWriter struct {
	path     string
	file     *os.File
	buf      *bufio.Writer
	written  int64
	maxBytes int64
	closed   bool
}

// openSegment creates or reopens a segment for append.
func openSegment(path string, maxBytes int64) (*segmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &segmentWriter{
		path:     path,
		file:     f,
		buf:      bufio.NewWriter(f),
		written:  info.Size(),
		maxBytes: maxBytes,
	}, nil
}

// append writes one record and returns the number of raw bytes stored.
func (s *segmentWriter) append(blob []byte) (int64, error) {
	if s.closed {
		return 0, fmt.Errorf("segment %s already closed", s.path)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(blob)))
	if _, err := s.buf.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	n, err := s.buf.Write(blob)
	if err != nil {
		return 0, err
	}
	s.written += int64(len(lenBuf)) + int64(n)
	return s.written, nil
}

// sync flushes buffered bytes to the underlying file.
func (s *segmentWriter) sync() error {
	if s.closed {
		return nil
	}
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

// close releases the file handle. It is safe to call more than once.
func (s *segmentWriter) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.buf.Flush(); err != nil {
		_ = s.file.Close()
		return err
	}
	return s.file.Close()
}

// remove deletes the segment file after closing it.
func (s *segmentWriter) remove() error {
	if err := s.close(); err != nil {
		return err
	}
	return os.Remove(s.path)
}

// readSegment decodes every record stored in one segment file.
func readSegment(path string, apply func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment %s: %w", path, err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read segment %s length: %w", path, err)
		}
		size := int(binary.BigEndian.Uint32(lenBuf[:]))
		if size <= 0 || size > 64<<20 {
			return fmt.Errorf("segment %s contains invalid record size %d", path, size)
		}
		blob := make([]byte, size)
		if _, err := io.ReadFull(r, blob); err != nil {
			return fmt.Errorf("read segment %s body: %w", path, err)
		}
		rec, err := Decode(blob)
		if err != nil {
			return fmt.Errorf("decode segment %s record: %w", path, err)
		}
		if err := apply(rec); err != nil {
			return err
		}
	}
}

// listSegments returns the segment paths in lexical order.
func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	return paths, nil
}
