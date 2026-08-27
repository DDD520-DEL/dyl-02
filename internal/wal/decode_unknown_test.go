package wal

import (
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
)

func snappyEncode(raw []byte) []byte {
	return snappy.Encode(nil, raw)
}

// TestDecodeRejectsUnknownEvent verifies that Decode rejects a record whose
// event byte holds an unknown value, so a corrupted record aborts replay
// instead of being silently dropped.
func TestDecodeRejectsUnknownEvent(t *testing.T) {
	id := "t1"
	payload := []byte("{}")
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(id)))
	header[4] = 0xFE // unknown event
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	raw := append(header, id...)
	raw = append(raw, payload...)

	blob := snappyEncode(raw)
	if _, err := Decode(blob); err == nil {
		t.Fatalf("Decode accepted unknown event 0xFE; expected a corruption error")
	}
}
