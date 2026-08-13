package krtcollections

import (
	"os"
	"sync/atomic"
	"time"

	"istio.io/istio/pkg/kube/krt"
)

// PROTOTYPE - spike for measuring the effect of debouncing route changes on gateway
// retranslation. Config is via env only; a real change would plumb this through settings.

// seededSyncer reports synced only once the source has synced AND the mirror has been
// seeded from it, so dependents never observe an empty debounced collection.
type seededSyncer struct {
	src    krt.Syncer
	seeded *atomic.Bool
}

func (s seededSyncer) HasSynced() bool { return s.src.HasSynced() && s.seeded.Load() }

func (s seededSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	if !s.src.WaitUntilSynced(stop) {
		return false
	}
	for !s.seeded.Load() {
		select {
		case <-stop:
			return false
		case <-time.After(time.Millisecond):
		}
	}
	return true
}

// NewDebouncedCollection mirrors src, flushing accumulated changes as a single batched
// event set at most once per window, and at least once per maxDelay while changes keep
// arriving.
//
// krt dedupes changed input keys within one event set, but never across queued ones:
// processorListener buffers whole event sets and pops them one at a time. So a burst of
// N route changes produces N separate recomputations of every dependent gateway instead
// of one, which is what makes bulk convergence O(N^2). Collapsing the burst into a
// single event set restores the dedupe.
//
// A window of 0 disables debouncing and returns src unchanged.
func NewDebouncedCollection[T any](
	src krt.Collection[T],
	window, maxDelay time.Duration,
	stop <-chan struct{},
	opts ...krt.CollectionOption,
) krt.Collection[T] {
	if window <= 0 {
		return src
	}
	seeded := &atomic.Bool{}
	out := krt.NewStaticCollection[T](seededSyncer{src: src, seeded: seeded}, nil, opts...)

	dirty := make(chan struct{}, 1)
	src.RegisterBatch(func([]krt.Event[T]) {
		select {
		case dirty <- struct{}{}:
		default: // a flush is already pending and will pick this up
		}
	}, false)

	go func() {
		// Seed before serving, so dependents waiting on HasSynced never see a partial view.
		if !src.WaitUntilSynced(stop) {
			return
		}
		out.Reset(src.List())
		seeded.Store(true)

		var settle, ceiling <-chan time.Time
		flush := func() {
			out.Reset(src.List()) // one batched event set
			settle, ceiling = nil, nil
		}
		for {
			select {
			case <-stop:
				return
			case <-dirty:
				settle = time.After(window)
				if ceiling == nil {
					ceiling = time.After(maxDelay) // no starvation under continuous churn
				}
			case <-settle:
				flush()
			case <-ceiling:
				flush()
			}
		}
	}()
	return out
}

func routeDebounceWindow() time.Duration { return envDur("KGW_ROUTE_DEBOUNCE", 0) }

func routeDebounceMaxDelay() time.Duration { return envDur("KGW_ROUTE_DEBOUNCE_MAX", time.Second) }

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
