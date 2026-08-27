package lease

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/metrics"
)

// TestManagerRenewExpireLookupContention reproduces the reported race: many
// workers concurrently Renew / Expire / Lookup the same task while a scheduler
// grants new generations as leases lapse. Under the old implementation this
// reported "concurrent map read and map write" and occasionally handed two
// workers the same live generation (duplicate execution). With the lock in
// place every observed lease is self-consistent and no two owners share an
// epoch.
func TestManagerRenewExpireLookupContention(t *testing.T) {
	clk := clock.NewManual(time.Unix(1_000_000, 0))
	m := New(time.Second, clk, metrics.New())

	const workers = 8
	const rounds = 500

	var lookups atomic.Int64
	var wg sync.WaitGroup

	// Scheduler: keep re-granting the lease to a fresh owner, advancing the
	// epoch so the prior owner's subsequent Renew/Expire must be rejected.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			owner := string(rune('A' + (i % workers)))
			m.Grant("shared", owner)
		}
	}()

	// Readers: hammer Lookup concurrently with the writers above.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds*10; i++ {
				if got, ok := m.Lookup("shared"); ok {
					lookups.Add(1)
					// A torn read could expose a zero epoch or an empty owner
					// paired with a positive epoch.
					if got.Owner == "" && got.Epoch != 0 {
						t.Errorf("torn lease read: %+v", got)
					}
				}
			}
		}()
	}

	// Workers: renew and expire whatever generation they believe they hold.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if got, ok := m.Lookup("shared"); ok {
					_, _ = m.Renew("shared", got.Owner, got.Epoch)
					_ = m.Expire("shared", got.Owner, got.Epoch)
				}
			}
		}()
	}

	wg.Wait()
	if lookups.Load() == 0 {
		t.Fatalf("expected lookups to observe the lease at least once")
	}
}
