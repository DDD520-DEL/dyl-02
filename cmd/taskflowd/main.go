package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dyl-02/taskflow/internal/api"
	"github.com/dyl-02/taskflow/internal/audit"
	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/config"
	"github.com/dyl-02/taskflow/internal/dispatch"
	"github.com/dyl-02/taskflow/internal/healthz"
	"github.com/dyl-02/taskflow/internal/idgen"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/lifecycle"
	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/notify"
	"github.com/dyl-02/taskflow/internal/queue"
	"github.com/dyl-02/taskflow/internal/registry"
	"github.com/dyl-02/taskflow/internal/retry"
	"github.com/dyl-02/taskflow/internal/scheduler"
	"github.com/dyl-02/taskflow/internal/store"
	"github.com/dyl-02/taskflow/internal/wal"
	"github.com/dyl-02/taskflow/internal/worker"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clk := clock.Wall()
	metricsRegistry := metrics.New()
	ids := idgen.New(1)

	walMgr, err := wal.NewManager(cfg.WALDir, cfg.WALSegmentBytes, metricsRegistry)
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}
	defer walMgr.Close()

	st := store.New(walMgr, clk, metricsRegistry)
	if err := st.Recover(); err != nil {
		log.Fatalf("recover wal: %v", err)
	}

	ring := queue.NewRing(cfg.BucketCount, cfg.TickInterval, clk)
	leaseMgr := lease.New(cfg.LeaseDuration, clk, metricsRegistry)
	reg := registry.New()
	if err := api.RegisterBuiltins(reg); err != nil {
		log.Fatalf("register builtins: %v", err)
	}

	defaultPolicy := retry.DefaultPolicy()
	notifyClient := notify.New(defaultPolicy, metricsRegistry)
	auditLog := audit.New(100, clk)
	pool := worker.NewPool(cfg.WorkerCount, make(chan worker.Job, cfg.WorkerCount*4), worker.Deps{
		Store:    st,
		Registry: reg,
		Lease:    leaseMgr,
		Policy:   defaultPolicy,
		Notify:   notifyClient,
		Audit:    auditLog,
		Clock:    clk,
		Metrics:  metricsRegistry,
	})
	for _, w := range pool.Workers() {
		go w.Run(ctx)
	}

	disp := dispatch.New(st, leaseMgr, pool, reg, clk, metricsRegistry)
	sched := scheduler.New(st, ring, disp, leaseMgr, clk, metricsRegistry, cfg.TickInterval)
	go sched.Run(ctx)

	purger := lifecycle.New(st, clk, cfg.RetentionWindow, metricsRegistry)
	go runPurgeLoop(ctx, purger, cfg.TickInterval)

	apiServer := api.New(api.Deps{
		Store:   st,
		Ring:    ring,
		Reg:     reg,
		IDs:     ids,
		Notify:  notifyClient,
		Audit:   auditLog,
		Clock:   clk,
		Metrics: metricsRegistry,
		Health:  healthz.New(st, ring, reg, clk),
		Limits: store.BatchLimits{
			MaxTasks:   100,
			MaxPayload: 1 << 20,
			KnownTypes: map[string]bool{"echo": true, "noop": true, "fail-once": true},
		},
	})

	log.Printf("taskflow listening on %s", cfg.ListenAddr)
	if err := apiServer.Serve(ctx, cfg.ListenAddr); err != nil && ctx.Err() == nil {
		log.Fatalf("serve: %v", err)
	}
}

func runPurgeLoop(ctx context.Context, p *lifecycle.Purger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.Purge(); err != nil {
				log.Printf("purge: %v", err)
			}
		}
	}
}
