package api

import (
	"context"
	"fmt"
	"time"

	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/healthz"
	"github.com/dyl-02/taskflow/internal/idgen"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/store"
)

// Server exposes the task submission and management API.
type Server struct {
	store   *store.Store
	ring    *queue.Ring
	reg     *registry.Registry
	ids     *idgen.Generator
	notify  *notify.Client
	audit   *audit.Log
	clk     clock.Clock
	metrics *metrics.Metrics
	health  *healthz.Prober
	limits  store.BatchLimits
}

// Deps carries the shared dependencies for the API server.
type Deps struct {
	Store   *store.Store
	Ring    *queue.Ring
	Reg     *registry.Registry
	IDs     *idgen.Generator
	Notify  *notify.Client
	Audit   *audit.Log
	Clock   clock.Clock
	Metrics *metrics.Metrics
	Health  *healthz.Prober
	Limits  store.BatchLimits
}

// New creates the API server.
func New(d Deps) *Server {
	return &Server{
		store:   d.Store,
		ring:    d.Ring,
		reg:     d.Reg,
		ids:     d.IDs,
		notify:  d.Notify,
		audit:   d.Audit,
		clk:     d.Clock,
		metrics: d.Metrics,
		health:  d.Health,
		limits:  d.Limits,
	}
}

// TaskInput is the JSON body used to submit a task.
type TaskInput struct {
	Type        string             `json:"type"`
	Payload     []byte             `json:"payload"`
	RunAt       string             `json:"run_at"`
	DelayMS     int64              `json:"delay_ms"`
	MaxAttempts int                `json:"max_attempts"`
	NotifyURL   string             `json:"notify_url"`
	Retry       *model.RetryConfig `json:"retry"`
}

// buildTask converts the input into a persisted model without side effects.
func (s *Server) buildTask(in TaskInput) (*model.Task, error) {
	if in.Type == "" {
		return nil, fmt.Errorf("task type must not be empty")
	}
	if !s.reg.Known(in.Type) {
		return nil, fmt.Errorf("unknown task type %q", in.Type)
	}
	id, err := s.ids.Next()
	if err != nil {
		return nil, err
	}
	now := s.clk.Now()
	sched := model.Schedule{Delay: time.Duration(in.DelayMS) * time.Millisecond, MaxAttempts: in.MaxAttempts}
	if in.RunAt != "" {
		runAt, err := time.Parse(time.RFC3339, in.RunAt)
		if err != nil {
			return nil, fmt.Errorf("invalid run_at: %w", err)
		}
		sched.RunAt = runAt
	}
	if err := sched.Normalize(now); err != nil {
		return nil, err
	}
	retryCfg := in.Retry
	if retryCfg != nil {
		retryCfg.Normalize()
	}
	seq := s.store.NextSequence()
	return &model.Task{
		ID:          id,
		Type:        in.Type,
		Payload:     in.Payload,
		State:       model.StatePending,
		CreatedAt:   now,
		ScheduledAt: sched.RunAt,
		NextRunAt:   sched.RunAt,
		MaxAttempts: sched.MaxAttempts,
		NotifyURL:   in.NotifyURL,
		Retry:       retryCfg,
		Sequence:    seq,
	}, nil
}

// Submit handles a single task submission.
func (s *Server) Submit(ctx context.Context, in TaskInput) (*model.Task, error) {
	t, err := s.buildTask(in)
	if err != nil {
		return nil, err
	}
	if err := s.store.Save(t); err != nil {
		return nil, err
	}
	if s.audit != nil {
		s.audit.Record(t.ID, model.StatePending, model.StatePending, "api", "submitted")
	}
	s.ring.Enqueue(t.ID, t.Sequence, t.NextRunAt)
	if s.metrics != nil {
		s.metrics.Submitted.Add(1)
	}
	return t.Clone(), nil
}

// SubmitBatch handles an all-or-nothing batch submission.
func (s *Server) SubmitBatch(ctx context.Context, inputs []TaskInput) ([]*model.Task, error) {
	tasks := make([]*model.Task, 0, len(inputs))
	for _, in := range inputs {
		t, err := s.buildTask(in)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := store.ValidateBatch(tasks, s.limits); err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if err := s.store.Save(t); err != nil {
			return nil, err
		}
		if s.audit != nil {
			s.audit.Record(t.ID, model.StatePending, model.StatePending, "api", "submitted")
		}
		s.ring.Enqueue(t.ID, t.Sequence, t.NextRunAt)
	}
	if s.metrics != nil {
		s.metrics.Submitted.Add(int64(len(tasks)))
	}
	return tasks, nil
}

// Cancel transitions a pending task to cancelled and delivers its webhook.
func (s *Server) Cancel(ctx context.Context, id string) error {
	t := s.store.Get(id)
	if t == nil {
		return fmt.Errorf("task %s not found", id)
	}
	if t.State.Terminal() {
		return fmt.Errorf("task %s already finished", id)
	}
	t.State = model.StateCancelled
	if err := s.store.Update(t); err != nil {
		return err
	}
	if s.audit != nil {
		s.audit.Record(t.ID, model.StatePending, model.StateCancelled, "api", "cancelled")
	}
	if s.metrics != nil {
		s.metrics.Cancelled.Add(1)
	}
	return s.notify.Notify(ctx, t, model.Result{TaskID: t.ID, Success: false, Message: "cancelled", Finished: s.clk.Now()})
}

// Get returns the current task view.
func (s *Server) Get(id string) (*model.Task, error) {
	t := s.store.Get(id)
	if t == nil {
		return nil, fmt.Errorf("task %s not found", id)
	}
	return t, nil
}

// History returns the recorded transitions for a task.
func (s *Server) History(id string) ([]audit.Entry, error) {
	if s.store.Get(id) == nil {
		return nil, fmt.Errorf("task %s not found", id)
	}
	return s.audit.History(id), nil
}
