package worker

import (
	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/retry"
	"github.com/dyl-02/taskflow/internal/store"
)

// Deps bundles the shared dependencies required to construct workers.
type Deps struct {
	Store    *store.Store
	Registry *registry.Registry
	Lease    *lease.Manager
	Policy   *retry.Policy
	Notify   *notify.Client
	Audit    *audit.Log
	Clock    clock.Clock
	Metrics  *metrics.Metrics
}
