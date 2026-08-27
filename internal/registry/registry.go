package registry

import (
	"context"
	"fmt"
	"sync"
)

// Handler executes a task payload and returns its result.
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

// Registry maps task types to their execution handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	snapshot map[string]Handler
	version  int64
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
		snapshot: make(map[string]Handler),
	}
}

// Register adds a new task type. Duplicate registration is an error.
func (r *Registry) Register(typ string, h Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[typ]; ok {
		return fmt.Errorf("task type %q already registered", typ)
	}
	r.handlers[typ] = h
	r.snapshot[typ] = h
	r.version++
	return nil
}

// Replace swaps the handler of an existing type and refreshes the dispatch
// snapshot so the next resolution sees the new implementation.
func (r *Registry) Replace(typ string, h Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[typ]; !ok {
		return fmt.Errorf("task type %q is not registered", typ)
	}
	r.handlers[typ] = h
	r.version++
	return nil
}

// Unregister removes a task type entirely.
func (r *Registry) Unregister(typ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[typ]; !ok {
		return fmt.Errorf("task type %q is not registered", typ)
	}
	delete(r.handlers, typ)
	delete(r.snapshot, typ)
	r.version++
	return nil
}

// Resolve returns the handler for a type using the dispatch snapshot.
func (r *Registry) Resolve(typ string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.snapshot[typ]
	if !ok {
		return nil, fmt.Errorf("unknown task type %q", typ)
	}
	return h, nil
}

// Known reports whether a type is registered.
func (r *Registry) Known(typ string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.handlers[typ]
	return ok
}

// Version returns the registry generation for observability.
func (r *Registry) Version() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}
