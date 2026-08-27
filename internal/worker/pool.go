package worker

import (
	"fmt"
	"sync"
)

// Pool owns the worker set and assigns jobs round-robin.
type Pool struct {
	mu      sync.Mutex
	workers []*Worker
	next    int
}

// NewPool creates a pool with n workers.
func NewPool(n int, jobs chan Job, deps Deps) *Pool {
	pool := &Pool{}
	for i := 0; i < n; i++ {
		w := New(fmt.Sprintf("w%d", i), jobs, deps.Store, deps.Registry, deps.Lease,
			deps.Policy, deps.Notify, deps.Audit, deps.Clock, deps.Metrics)
		pool.workers = append(pool.workers, w)
	}
	return pool
}

// Next returns the next worker in round-robin order.
func (p *Pool) Next() *Worker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.workers) == 0 {
		return nil
	}
	w := p.workers[p.next]
	p.next = (p.next + 1) % len(p.workers)
	return w
}

// Workers returns all workers in the pool.
func (p *Pool) Workers() []*Worker {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*Worker(nil), p.workers...)
}

// Send returns the worker's job channel for injection by the dispatcher.
func (w *Worker) Send() chan<- Job {
	return w.jobs
}
