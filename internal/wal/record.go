package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// Event describes the mutation stored in a WAL record.
type Event uint8

const (
	EventSubmit Event = iota + 1
	EventUpdate
	EventDelete

	// eventMax is the highest valid Event value. It bounds the event byte so
	// that a corrupted or future/unknown event value is rejected during decode
	// instead of silently replayed as a no-op and dropped.
	eventMax Event = EventDelete
)

// valid reports whether e is a known event on the wire.
func (e Event) valid() bool { return e >= EventSubmit && e <= eventMax }

// Record is a single durable mutation.
type Record struct {
	Event   Event
	TaskID  string
	Payload []byte
}

const headerSize = 4 + 1 + 4

// HeaderSize is the byte length of the fixed record header (idLen + event +
// payloadLen). Exported so callers and tests can reason about the wire format.
func HeaderSize() int { return headerSize }

// Encode serializes the record into a snappy-compressed blob.
func (r Record) Encode() ([]byte, error) {
	raw := make([]byte, headerSize+len(r.TaskID)+len(r.Payload))
	binary.BigEndian.PutUint32(raw[0:4], uint32(len(r.TaskID)))
	raw[4] = byte(r.Event)
	binary.BigEndian.PutUint32(raw[5:9], uint32(len(r.Payload)))
	copy(raw[headerSize:], r.TaskID)
	copy(raw[headerSize+len(r.TaskID):], r.Payload)
	return snappy.Encode(nil, raw), nil
}

// Decode parses a record from a compressed blob.
func Decode(blob []byte) (Record, error) {
	raw, err := snappy.Decode(nil, blob)
	if err != nil {
		return Record{}, fmt.Errorf("decompress wal record: %w", err)
	}
	if len(raw) < headerSize {
		return Record{}, io.ErrUnexpectedEOF
	}
	idLen := int(binary.BigEndian.Uint32(raw[0:4]))
	payloadLen := int(binary.BigEndian.Uint32(raw[5:9]))
	if headerSize+idLen+payloadLen != len(raw) {
		return Record{}, fmt.Errorf("wal record length mismatch")
	}
	event := Event(raw[4])
	if !event.valid() {
		// A corrupted event byte produces an unknown mutation. Reject it here
		// so replay aborts rather than silently skipping the record (and the
		// records that follow), which would lose tasks on recovery.
		return Record{}, fmt.Errorf("wal record has unknown event %d", event)
	}
	return Record{
		Event:   event,
		TaskID:  string(raw[headerSize : headerSize+idLen]),
		Payload: bytes.Clone(raw[headerSize+idLen:]),
	}, nil
}
