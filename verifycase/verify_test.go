package verifycase

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dyl-02/taskflow/internal/clock"
	"github.com/dyl-02/taskflow/internal/lease"
	"github.com/dyl-02/taskflow/internal/metrics"
)

// TestConcurrentLeaseOpsRaceFree verifies lease operations are safe under
// concurrent workers (must pass under -race).
func TestConcurrentLeaseOpsRaceFree(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(now)
	m := metrics.New()
	lm := lease.New(10*time.Second, clk, m)

	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			owner := fmt.Sprintf("w%d", n%4)
			_ = lm.Grant("t1", owner)
			_, _ = lm.Renew("t1", owner, 1)
			_, _ = lm.Lookup("t1")
			_ = lm.Expire("t1", owner, 1)
		}(i)
	}
	wg.Wait()
}
