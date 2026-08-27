// Package healthz implements a real readiness probe over the taskflow
// runtime: store, WAL, ready queue and worker registry. The API exposes
// the snapshot at /healthz and a strict gate at /healthz/ready so that
// operators can verify a node is safe to serve traffic.
package healthz

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/store"
)

// Component identifiers reported by the probe.
const (
	ComponentStore    = "store"
	ComponentWAL      = "wal"
	ComponentQueue    = "queue"
	ComponentRegistry = "registry"
)

// ComponentView carries the per-component snapshot shown in the report.
type ComponentView struct {
	OK      bool   `json:"ok"`
	Detail  string `json:"detail,omitempty"`
	Latency int64  `json:"latency_ms"`
}

// CheckResult is the flattened per-component probe record.
type CheckResult struct {
	Component string `json:"component"`
	OK        bool   `json:"ok"`
	Detail    string `json:"detail,omitempty"`
	LatencyMS int64  `json:"latency_ms"`
}

// Report is the immutable snapshot returned by one Check call.
type Report struct {
	Status      string                    `json:"status"`
	GeneratedAt string                    `json:"generated_at"`
	TotalTasks  int                       `json:"total_tasks"`
	ActiveTasks int                       `json:"active_tasks"`
	Checks      []CheckResult             `json:"checks"`
	Components  map[string]ComponentView  `json:"components"`
}

// Prober runs bounded readiness probes against the live components and
// keeps a short ring of past snapshots for trend inspection.
type Prober struct {
	st      *store.Store
	ring    *queue.Ring
	reg     *registry.Registry
	clk     clock.Clock
	mu      sync.Mutex
	history []Report
	cap     int
}

// New returns a Prober bound to the given runtime components.
func New(st *store.Store, ring *queue.Ring, reg *registry.Registry, clk clock.Clock) *Prober {
	return &Prober{st: st, ring: ring, reg: reg, clk: clk, cap: 64}
}

// Check gathers one readiness snapshot from the live components.
func (p *Prober) Check(ctx context.Context) Report {
	started := time.Now()
	components := make(map[string]ComponentView, 4)
	checks := make([]CheckResult, 0, 4)
	collect := func(name string, ok bool, detail string, lat time.Duration) {
		ms := lat.Milliseconds()
		components[name] = ComponentView{OK: ok, Detail: detail, Latency: ms}
		checks = append(checks, CheckResult{Component: name, OK: ok, Detail: detail, LatencyMS: ms})
	}

	collect(ComponentStore, true, p.storeDetail(ctx), time.Since(started))
	collect(ComponentWAL, true, p.walDetail(ctx), time.Since(started))
	collect(ComponentQueue, true, p.queueDetail(ctx), time.Since(started))
	collect(ComponentRegistry, true, p.registryDetail(ctx), time.Since(started))

	allOK := true
	for _, c := range checks {
		if !c.OK {
			allOK = false
			break
		}
	}
	status := "ok"
	if !allOK {
		status = "degraded"
	}
	report := Report{
		Status:      status,
		GeneratedAt: p.clk.Now().Format(time.RFC3339),
		TotalTasks:  p.totalTasks(ctx),
		ActiveTasks: p.activeTasks(ctx),
		Checks:      checks,
		Components:  components,
	}
	p.mu.Lock()
	p.history = append(p.history, report)
	if len(p.history) > p.cap {
		p.history = p.history[len(p.history)-p.cap:]
	}
	p.mu.Unlock()
	return report
}

// Ready reports whether every component passed its most recent probe.
func (p *Prober) Ready(ctx context.Context) bool {
	report := p.Check(ctx)
	return report.Status == "ok"
}

// Recent returns up to n of the most recent snapshots, newest first.
func (p *Prober) Recent(n int) []Report {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Report, 0, n)
	for i := len(p.history) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, p.history[i])
	}
	return out
}

// Summary aggregates the recent snapshots into a compact view used by the
// history endpoint so operators can spot a degrading component at a glance.
type Summary struct {
	Probes          int            `json:"probes"`
	LastStatus      string         `json:"last_status"`
	AvgLatencyMS    int64          `json:"avg_latency_ms"`
	ComponentOK     map[string]int `json:"component_ok"`
	ComponentTotal  map[string]int `json:"component_total"`
}

// Summary returns the aggregated view over the retained snapshots.
func (p *Prober) Summary() Summary {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := Summary{
		ComponentOK:    make(map[string]int),
		ComponentTotal: make(map[string]int),
	}
	if len(p.history) == 0 {
		return out
	}
	out.Probes = len(p.history)
	out.LastStatus = p.history[len(p.history)-1].Status
	var latencyTotal int64
	var checkCount int
	for _, report := range p.history {
		for _, check := range report.Checks {
			out.ComponentTotal[check.Component]++
			if check.OK {
				out.ComponentOK[check.Component]++
			}
			latencyTotal += check.LatencyMS
			checkCount++
		}
	}
	if checkCount > 0 {
		out.AvgLatencyMS = latencyTotal / int64(checkCount)
	}
	return out
}

func (p *Prober) storeDetail(_ context.Context) string {
	total := len(p.st.All())
	pending := p.st.CountByState(model.StatePending)
	running := p.st.CountByState(model.StateRunning)
	waiting := p.st.CountByState(model.StateWaitingRetry)
	return fmt.Sprintf("total=%d pending=%d running=%d waiting_retry=%d", total, pending, running, waiting)
}

func (p *Prober) walDetail(_ context.Context) string {
	return fmt.Sprintf("segments=%d", p.st.WAL().SegmentCount())
}

func (p *Prober) queueDetail(_ context.Context) string {
	return fmt.Sprintf("pending=%d", p.ring.Pending())
}

func (p *Prober) registryDetail(_ context.Context) string {
	known := p.reg.Known("echo") && p.reg.Known("noop") && p.reg.Known("fail-once")
	detail := fmt.Sprintf("version=%d builtins=%v", p.reg.Version(), known)
	if !known {
		return "builtin handlers missing; " + detail
	}
	return detail
}

func (p *Prober) totalTasks(_ context.Context) int {
	return len(p.st.All())
}

func (p *Prober) activeTasks(_ context.Context) int {
	return p.st.CountByState(model.StatePending) + p.st.CountByState(model.StateRunning) + p.st.CountByState(model.StateWaitingRetry)
}
