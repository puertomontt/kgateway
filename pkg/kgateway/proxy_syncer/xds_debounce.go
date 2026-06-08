package proxy_syncer

import (
	"context"
	"time"

	"istio.io/istio/pkg/util/concurrent"
	"istio.io/istio/pkg/util/sets"

	kmetrics "github.com/kgateway-dev/kgateway/v2/pkg/krtcollections/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/utils/envutils"
)

const (
	// xdsDebounceAfterEnv configures the minimum quiet period that must elapse
	// after the last xDS-affecting event before a snapshot is pushed.
	xdsDebounceAfterEnv = "KGW_XDS_DEBOUNCE_AFTER"
	// xdsDebounceMaxEnv configures the maximum time a push may be deferred while
	// events keep arriving, bounding the worst-case staleness during a sustained
	// burst.
	xdsDebounceMaxEnv = "KGW_XDS_DEBOUNCE_MAX"

	// defaultXDSDebounceAfter and defaultXDSDebounceMax mirror the values used by
	// the agentgateway syncer. They are intentionally small: kgateway's KRT graph
	// has already coalesced most upstream churn by the time it reaches the xDS
	// push, so this is a final guard against pathological event storms (e.g. a
	// client repeatedly churning resources) overwhelming connected Envoys.
	defaultXDSDebounceAfter = 10 * time.Millisecond
	defaultXDSDebounceMax   = 1 * time.Second

	// xdsPushChannelBufferSize buffers resource names emitted by the KRT batch
	// handler so a transient burst does not block the KRT event loop while the
	// debouncer drains. The debouncer pushes asynchronously, so the channel is
	// drained continuously even while a push is in flight.
	xdsPushChannelBufferSize = 100
)

var (
	// xdsDebounceAfter is the resolved minimum quiet period. Set to 0 to push
	// effectively immediately (no debouncing).
	xdsDebounceAfter = envutils.GetDurationOrDefault(xdsDebounceAfterEnv, defaultXDSDebounceAfter)
	// xdsDebounceMax is the resolved maximum deferral.
	xdsDebounceMax = envutils.GetDurationOrDefault(xdsDebounceMaxEnv, defaultXDSDebounceMax)
)

// runXDSPushDebounce coalesces xDS push events arriving on ch and invokes flush
// once per quiet period (or at least every max during a sustained burst) with
// the deduplicated set of resource names. It blocks until stop is closed.
func runXDSPushDebounce(stop <-chan struct{}, ch chan string, after, max time.Duration, flush func(sets.Set[string])) {
	debouncer := &concurrent.Debouncer[string]{}
	debouncer.Run(ch, stop, after, max, flush)
}

// syncXdsForResource pushes the latest snapshot for the given per-client
// resource to the xDS cache. If the resource is no longer present in the
// collection (it was withdrawn or the client went away), the cache is left
// untouched so Envoy keeps serving its last coherent config — matching the
// prior EventDelete "retain last good" behavior.
func (s *ProxySyncer) syncXdsForResource(ctx context.Context, resourceName string) {
	cd := getDetailsFromXDSClientResourceName(resourceName)

	if snapWrap := s.perclientSnapCollection.GetKey(resourceName); snapWrap != nil {
		s.proxyTranslator.syncXds(ctx, *snapWrap)
	}

	kmetrics.EndResourceXDSSync(kmetrics.ResourceSyncDetails{
		Namespace:    cd.Namespace,
		Gateway:      cd.Gateway,
		ResourceName: cd.Gateway,
	})
}
