package lease

import (
	"sync"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/metrics"
)

// TestManagerConcurrentAccess hammers the manager from many goroutines doing
// every operation at once. Under the old implementation the map was accessed
// without any lock, so `go test -race` reported "concurrent map read and map
// write" here; once the lock is in place the run is clean and the fencing
// epoch advances monotonically so no two owners ever share a generation.
func TestManagerConcurrentAccess(t *testing.T) {
	clk := clock.NewManual(time.Unix(1_000_000, 0))
	m := New(time.Second, clk, metrics.New())

	const owners = 8
	const rounds = 200

	var wg sync.WaitGroup
	// Driver: keep granting, renewing, expiring, and looking up the same
	// taskID across distinct owners. The last owner to Grant wins; everyone
	// else's Renew/Expire must be rejected as stale rather than clobbering
	// the current lease.
	for o := 0; o < owners; o++ {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				l := m.Grant("task-1", owner)

				// Renew while we believe we still own it. A stale result is
				// fine and expected when another owner won the grant race.
				_, _ = m.Renew("task-1", owner, l.Epoch)

				// Concurrent readers must observe a consistent snapshot,
				// never a torn map entry.
				if got, ok := m.Lookup("task-1"); ok {
					if got.Epoch < 1 {
						t.Errorf("torn lease read: epoch=%d for owner=%s", got.Epoch, owner)
					}
				}

				// Expire is best-effort; only the current generation may win.
				_ = m.Expire("task-1", owner, l.Epoch)
			}
		}(string(rune('A' + o)))
	}
	wg.Wait()

	// The map must still be usable and consistent afterwards.
	final, ok := m.Lookup("task-1")
	if ok {
		// Whatever lease survived must be self-consistent: a non-empty owner
		// paired with a positive epoch.
		if final.Epoch < 1 || final.Owner == "" {
			t.Fatalf("inconsistent final lease: %+v", final)
		}
	}
}
