package val_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Requests validate concurrently: compiling and caching a new pattern must be safe
// (run with -race).
func TestConstrainedIsSafeConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := &val.V{}
			pattern := fmt.Sprintf("^x{0,%d}$", i%8+1)
			if _, ok := v.Constrained(nil, "x", val.StrRules{Pattern: pattern}); !ok {
				t.Errorf("%s rejected x: %v", pattern, v.Errors)
			}
		}()
	}
	wg.Wait()
}
