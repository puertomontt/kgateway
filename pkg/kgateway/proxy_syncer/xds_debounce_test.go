package proxy_syncer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"istio.io/istio/pkg/util/sets"
)

// TestRunXDSPushDebounceCoalesces verifies that a burst of events for the same
// set of resources collapses into a small number of flushes carrying the
// deduplicated resource names, rather than one flush per event.
func TestRunXDSPushDebounceCoalesces(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	ch := make(chan string, 100)

	var mu sync.Mutex
	var flushes []sets.Set[string]
	flushed := make(chan struct{}, 100)

	go runXDSPushDebounce(stop, ch, 20*time.Millisecond, 200*time.Millisecond, func(names sets.Set[string]) {
		mu.Lock()
		flushes = append(flushes, names.Copy())
		mu.Unlock()
		flushed <- struct{}{}
	})

	// Emit a tight burst of events for two resources.
	for range 50 {
		ch <- "gw-a"
		ch <- "gw-b"
	}

	// Wait for at least one flush, then allow the quiet period to fully elapse.
	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first debounced flush")
	}
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// The 100 events must coalesce into far fewer flushes.
	assert.Less(t, len(flushes), 100, "expected events to be coalesced into fewer flushes")
	assert.NotEmpty(t, flushes, "expected at least one flush")

	// Every flushed resource name must be one we sent (deduplicated to gw-a/gw-b).
	all := sets.New[string]()
	for _, f := range flushes {
		all = all.Merge(f)
	}
	assert.True(t, all.Contains("gw-a"), "expected gw-a to be pushed")
	assert.True(t, all.Contains("gw-b"), "expected gw-b to be pushed")
	assert.Equal(t, 2, all.Len(), "expected only the two distinct resources to be pushed")
}
