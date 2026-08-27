package idgen

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// Generator produces unique, sortable task identifiers.
type Generator struct {
	node   uint16
	seq    uint32
	lastMS int64
	mu     sync.Mutex
}

// New creates a generator for the given node id.
func New(node uint16) *Generator {
	return &Generator{node: node}
}

// Next returns the next unique identifier.
func (g *Generator) Next() (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := time.Now().UnixMilli()
	if ms == g.lastMS {
		g.seq++
	} else {
		g.lastMS = ms
		g.seq = 0
	}
	if g.seq > 0xFFFF {
		return "", fmt.Errorf("sequence exhausted within one millisecond")
	}
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(ms))
	binary.BigEndian.PutUint32(buf[8:12], uint32(g.node)<<16|g.seq)
	if _, err := rand.Read(buf[12:16]); err != nil {
		return "", fmt.Errorf("random suffix: %w", err)
	}
	return fmt.Sprintf("%x", buf[:]), nil
}
