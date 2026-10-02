package proxy_syncer

import (
	"context"
	"time"

	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	envoyresourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

// dereferenceState is one client's record of when each published cluster left
// the emitted set. It lives under the publish gate's lock, alongside the other
// per-client timed publication state.
type dereferenceState struct {
	// since is keyed by cluster name. An entry exists only while the cluster is
	// published but no longer emitted; re-referencing deletes it, so a route
	// that flaps keeps its cluster continuously rather than oscillating.
	since map[string]time.Time
	// wrap is the latest snapshot for this client, re-resolved when a window
	// closes; nothing else recomputes it merely because time passed.
	wrap  XdsSnapWrapper
	timer *time.Timer
	// gen identifies the armed timer. A callback that fired but lost the lock
	// race to a re-arm (or to the state being replaced) sees a different gen
	// and does nothing, since Stop cannot recall it.
	gen uint64
}

// graceDereferencedClusters decides which of the currently-published clusters a
// build has stopped emitting should still be published, and reports when the
// oldest of them expires.
//
// Removing a de-referenced cluster the moment the last route stops naming it is
// unsafe in the opposite direction from adding one. Envoy is delivered CDS
// before RDS, so the cluster would go out before the route that still targets
// it, and requests in that window get 503 NC. Retaining it briefly makes the
// emitted set "referenced now, plus recently de-referenced", which puts the
// route update first without depending on delivery order at all.
//
// The window is the removal-side fix regardless of how CDS and RDS are ordered
// on the wire, which is why it is not replaced by ordered ADS: ordered ADS
// fixes CDS before RDS, and that is exactly the wrong order here.
//
// Callers must hold the gate lock. A zero grace disables retention, and a
// client with no state allocates none.
func (g *publishGate) graceDereferencedClustersLocked(
	proxyKey string,
	published envoycache.ResourceSnapshot,
	building envoycache.Resources,
	now time.Time,
) (map[string]struct{}, time.Duration) {
	if g.dereferenceGrace <= 0 {
		return nil, 0
	}

	publishedClusters := published.GetResourcesAndTTL(envoyresourcev3.ClusterType)
	state := g.dereferenced[proxyKey]

	var graced map[string]struct{}
	var soonest time.Duration
	var opened, pruned int
	var publishedNames map[string]struct{}
	for name := range publishedClusters {
		if _, stillEmitted := building.Items[name]; stillEmitted {
			// Re-referenced (or never de-referenced): drop any record, so the
			// next de-reference starts a fresh window rather than inheriting an
			// expired one.
			if state != nil {
				delete(state.since, name)
			}
			continue
		}

		if state == nil {
			state = &dereferenceState{since: make(map[string]time.Time)}
			g.dereferenced[proxyKey] = state
		}
		left, recorded := state.since[name]
		if recorded {
			// The window runs from when the published routing types stop
			// naming the cluster, not from when a build first omits it: while
			// a held flip keeps the old routes live, the clock must not run
			// down, or the cluster goes in the same snapshot as the release.
			if publishedNames == nil {
				publishedNames = publishedEmittedClusters(published)
			}
			if _, stillNamed := publishedNames[name]; stillNamed {
				left = now
				state.since[name] = left
			}
		} else {
			left = now
			state.since[name] = left
		}

		remaining := g.dereferenceGrace - now.Sub(left)
		if remaining <= 0 {
			// Grace elapsed: stop publishing it, and forget it so a later
			// re-reference is a fresh addition rather than an expired removal.
			delete(state.since, name)
			pruned++
			continue
		}
		if graced == nil {
			graced = make(map[string]struct{})
		}
		if !recorded {
			// Counted where the window opens, not on every publish that keeps
			// it open, so the counter measures de-reference events rather than
			// publish frequency.
			opened++
		}
		graced[name] = struct{}{}
		if soonest == 0 || remaining < soonest {
			soonest = remaining
		}
	}

	if state != nil {
		// A recorded cluster that is no longer published was not carried: it
		// errored (carrying is fail-closed) or another path removed it. Its
		// window protects nothing, and keeping the record would keep the state,
		// and a timer holding an older wrapper, alive past every later publish.
		for name := range state.since {
			if _, stillPublished := publishedClusters[name]; !stillPublished {
				delete(state.since, name)
			}
		}
		if len(state.since) == 0 {
			g.cancelDereferenceTimerLocked(proxyKey)
		}
	}
	recordClusterScopingTransition(proxyKey, transitionDereferenceGraced, opened)
	recordClusterScopingTransition(proxyKey, transitionDereferencePruned, pruned)
	return graced, soonest
}

// armDereferenceTimerLocked schedules the re-publish that actually removes a
// graced cluster. Without it, a cluster whose grace expires would linger until
// something else about the client changed: nothing recomputes a client's
// snapshot merely because time passed.
//
// The timer is re-armed to the soonest remaining expiry on every publish, so a
// second de-reference during an open window does not extend the first one's.
// Callers must hold the gate lock.
func (g *publishGate) armDereferenceTimerLocked(
	ctx context.Context,
	cache envoycache.SnapshotCache,
	proxyKey string,
	wrap XdsSnapWrapper,
	in time.Duration,
) {
	state := g.dereferenced[proxyKey]
	if state == nil || in <= 0 {
		return
	}
	state.wrap = wrap
	if state.timer != nil {
		state.timer.Stop()
	}
	state.gen++
	gen := state.gen
	state.timer = time.AfterFunc(in, func() {
		g.fireDereferencePrune(ctx, cache, proxyKey, state, gen)
	})
}

func (g *publishGate) cancelDereferenceTimerLocked(proxyKey string) {
	if state := g.dereferenced[proxyKey]; state != nil {
		if state.timer != nil {
			state.timer.Stop()
		}
		delete(g.dereferenced, proxyKey)
	}
}

// fireDereferencePrune re-publishes the client's latest snapshot now that a
// grace window has closed, under the same lock as every other publication, so
// pruning cannot race a coherent publish.
//
// It re-applies whatever hold is open rather than publishing the unheld build:
// closing a de-reference window must not also release a route flip held for an
// unready cluster, or a reference-ahead hold, before that hold's own bound.
func (g *publishGate) fireDereferencePrune(
	ctx context.Context,
	cache envoycache.SnapshotCache,
	proxyKey string,
	armed *dereferenceState,
	gen uint64,
) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if state := g.dereferenced[proxyKey]; state != armed || state.gen != gen {
		return // the window closed, or the timer was re-armed, while this callback waited
	}
	published, err := cache.GetSnapshot(proxyKey)
	if err != nil {
		g.cancelDereferenceTimerLocked(proxyKey)
		return
	}

	wrap := armed.wrap
	graced, soonest := g.graceDereferencedClustersLocked(
		proxyKey, published, wrap.snap.Resources[envoycachetypes.Cluster], time.Now())
	logger.Debug("de-reference grace elapsed; re-publishing without the expired clusters",
		"proxy_key", proxyKey, "still_graced", len(graced))
	// holdFlips follows the wrapper, as in resolveDeferred: a flip already
	// released is published and no longer flip-blocking, so this re-holds only
	// a flip still waiting on its cluster (budget or not).
	if err := g.publishResolvedLocked(ctx, cache, wrap, published, graced, wrap.deferred); err != nil {
		logger.Error("failed to set xds snapshot", "proxy_key", proxyKey, "error", err)
	}
	g.armDereferenceTimerLocked(ctx, cache, proxyKey, wrap, soonest)
}

// retainsDereferenced reports whether any cluster can outlive its last
// reference. False is the whole feature switched off, and the coherent publish
// path stays exactly as it was.
func (g *publishGate) retainsDereferenced() bool { return g.dereferenceGrace > 0 }

// publishWithTransitionGraces publishes a coherent snapshot with both emitted-set
// transitions made safe: clusters whose de-reference window is still open are
// carried forward, and a route update that retargets onto a cluster the client
// has never been sent is held back until that cluster has had time to land.
//
// Both transitions produce coherent builds -- nothing is missing, the emitted
// set simply changed -- so neither reaches the deferred resolution path that
// handles unready clusters. This is their equivalent.
func (g *publishGate) publishWithTransitionGraces(
	ctx context.Context,
	cache envoycache.SnapshotCache,
	snapWrap XdsSnapWrapper,
	hasPriorXDSVersion func(string) bool,
) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	published, err := cache.GetSnapshot(snapWrap.proxyKey)
	_, departed := g.departed[snapWrap.proxyKey]
	coldReconnect := departed && (hasPriorXDSVersion == nil || !hasPriorXDSVersion(snapWrap.proxyKey))
	if err != nil || coldReconnect {
		// Never published, or the cached snapshot belongs to a client that has
		// since departed and reconnected cold (the cache entry outlives it,
		// #14307): either way it is not what this client holds, so there is
		// nothing to carry forward or hold against.
		return g.publishLocked(ctx, cache, snapWrap.proxyKey, snapWrap.snap)
	}

	graced, soonest := g.graceDereferencedClustersLocked(
		snapWrap.proxyKey, published, snapWrap.snap.Resources[envoycachetypes.Cluster], time.Now())

	// The addition side rides the same path, for the same reason: a retarget
	// onto a cluster the client has never been sent is a coherent build too.
	// Holding routes, listeners and secrets at their published versions
	// publishes the new cluster's CDS by itself, and the existing flip-release
	// timer sends the route once the window closes.
	newlyEmitted := g.newlyEmittedLocked(snapWrap, published)
	if len(graced) == 0 && len(newlyEmitted) == 0 {
		return g.publishLocked(ctx, cache, snapWrap.proxyKey, snapWrap.snap)
	}

	publishErr := g.publishResolvedWithLocked(ctx, cache, snapWrap, published, graced, false, newlyEmitted)
	g.armDereferenceTimerLocked(ctx, cache, snapWrap.proxyKey, snapWrap, soonest)
	return publishErr
}
