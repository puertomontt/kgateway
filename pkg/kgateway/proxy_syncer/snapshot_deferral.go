package proxy_syncer

import (
	"sync"

	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"

	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// snapshotDeferralTracker counts connected clients without a current snapshot
// row, including clients that have never received a snapshot. A deferred client
// may still have its previous snapshot in the cache.
//
// Client and snapshot events maintain the sets and per-gateway counts in O(1)
// per event. Gateway and namespace labels identify affected gateways without
// adding a metric series per client.
//
// The tracker also owns the lifetime of every per-client xDS snapshot series
// (resources, transforms, deferred clients): they are deleted once a gateway has
// no connected clients, so a deleted Gateway does not leave them behind. It
// publishes the resources gauge itself so that a snapshot event processed after
// the last disconnect cannot resurrect a series that was already deleted.
type snapshotDeferralTracker struct {
	mu sync.Mutex
	// clients is the set of connected client keys.
	clients map[string]struct{}
	// snapshots is the set of client keys that currently have a snapshot row.
	snapshots map[string]struct{}
	// deferred counts, per gateway label pair, the clients in clients but not
	// in snapshots. Kept incrementally so events are O(1).
	deferred map[resourceNameDetails]int
	// connected counts, per gateway label pair, the clients in clients.
	connected map[resourceNameDetails]int
}

func newSnapshotDeferralTracker() *snapshotDeferralTracker {
	return &snapshotDeferralTracker{
		clients:   make(map[string]struct{}),
		snapshots: make(map[string]struct{}),
		deferred:  make(map[resourceNameDetails]int),
		connected: make(map[resourceNameDetails]int),
	}
}

// register subscribes to client and snapshot events, replaying existing state.
func (d *snapshotDeferralTracker) register(clients krt.Collection[ir.UniquelyConnectedClient], snapshots krt.Collection[XdsSnapWrapper]) {
	clients.RegisterBatch(d.clientEvents, true)
	snapshots.RegisterBatch(d.snapshotEvents, true)
}

func (d *snapshotDeferralTracker) clientEvents(events []krt.Event[ir.UniquelyConnectedClient]) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range events {
		key := e.Latest().ResourceName()
		gw := gatewayOf(key)
		if e.Event == controllers.EventDelete {
			if _, known := d.clients[key]; known {
				delete(d.clients, key)
				if _, has := d.snapshots[key]; !has {
					d.adjust(key, -1)
				}
				if d.connected[gw]--; d.connected[gw] <= 0 {
					delete(d.connected, gw)
					deleteClientSnapshotMetrics(gw)
				}
			}
			continue
		}
		if _, known := d.clients[key]; !known {
			d.clients[key] = struct{}{}
			d.connected[gw]++
			if _, has := d.snapshots[key]; !has {
				d.adjust(key, +1)
			}
		}
	}
}

func (d *snapshotDeferralTracker) snapshotEvents(events []krt.Event[XdsSnapWrapper]) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range events {
		key := e.Latest().ResourceName()
		gw := gatewayOf(key)
		if e.Event == controllers.EventDelete {
			// A snapshot row disappears when the client disconnects, or when
			// its transform returned nil: the deferral this gauge exists for.
			if _, had := d.snapshots[key]; had {
				delete(d.snapshots, key)
				if _, known := d.clients[key]; known {
					d.adjust(key, +1)
				}
			}
			if d.connected[gw] == 0 {
				// The row's final event, after the gateway's last client left.
				// Anything recorded for it since that disconnect is stale.
				deleteClientSnapshotMetrics(gw)
			} else {
				setSnapshotResources(gw, nil)
			}
			continue
		}
		setSnapshotResources(gw, e.Latest().snap)
		if _, had := d.snapshots[key]; !had {
			d.snapshots[key] = struct{}{}
			if _, known := d.clients[key]; known {
				d.adjust(key, -1)
			}
		}
	}
}

// adjust moves one gateway's deferred count and publishes it. Called with mu held.
func (d *snapshotDeferralTracker) adjust(clientKey string, delta int) {
	labels := gatewayOf(clientKey)
	n := d.deferred[labels] + delta
	if n <= 0 {
		delete(d.deferred, labels)
		n = 0
	} else {
		d.deferred[labels] = n
	}
	snapshotDeferredClients.Set(float64(n),
		metrics.Label{Name: gatewayLabel, Value: labels.Gateway},
		metrics.Label{Name: namespaceLabel, Value: labels.Namespace},
	)
}

// collectTransformMetrics wraps collectXDSTransformMetrics for a client's snapshot
// transform so that a transform finishing after its client disconnected records nothing.
// The tracker deletes the gateway's series when its last client disconnects, and a
// transform that then returns nil emits no snapshot event that would delete them again.
//
// clients drops the client before the tracker's clientEvents sees its delete, and both
// run under mu, so either the transform records first and that delete removes the series,
// or the client is already gone and the transform records nothing.
func (d *snapshotDeferralTracker) collectTransformMetrics(clients krt.Collection[ir.UniquelyConnectedClient], clientKey string) func(error) {
	finish := collectXDSTransformMetrics(clientKey)
	if !metrics.Active() {
		return finish
	}
	return func(err error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if clients.GetKey(clientKey) == nil {
			return
		}
		finish(err)
	}
}

// gatewayOf returns the gateway label pair of a client key.
func gatewayOf(clientKey string) resourceNameDetails {
	cd := getDetailsFromXDSClientResourceName(clientKey)
	return resourceNameDetails{Namespace: cd.Namespace, Gateway: cd.Gateway}
}

// snapshotResourceTypes are the xDS types reported by the resources gauge, by label value.
var snapshotResourceTypes = []struct {
	label   string
	typeIdx envoycachetypes.ResponseType
}{
	{"Cluster", envoycachetypes.Cluster},
	{"Endpoint", envoycachetypes.Endpoint},
	{"Route", envoycachetypes.Route},
	{"Listener", envoycachetypes.Listener},
	{"Secret", envoycachetypes.Secret},
}

// setSnapshotResources publishes a gateway's snapshot resource counts; a nil
// snapshot publishes zeros. Called with mu held.
func setSnapshotResources(gw resourceNameDetails, snap *envoycache.Snapshot) {
	if !metrics.Active() {
		return
	}
	for _, rt := range snapshotResourceTypes {
		var n int
		if snap != nil {
			n = len(snap.Resources[rt.typeIdx].Items)
		}
		snapshotResources.Set(float64(n), snapshotResourcesMetricLabels{
			Gateway:   gw.Gateway,
			Namespace: gw.Namespace,
			Resource:  rt.label,
		}.toMetricsLabels()...)
	}
}

// deleteClientSnapshotMetrics deletes every per-client snapshot series of a
// gateway. Called with mu held once the gateway has no connected clients.
func deleteClientSnapshotMetrics(gw resourceNameDetails) {
	labels := []metrics.Label{
		{Name: gatewayLabel, Value: gw.Gateway},
		{Name: namespaceLabel, Value: gw.Namespace},
	}
	snapshotTransformsTotal.DeletePartialMatch(labels...)
	snapshotTransformDuration.DeletePartialMatch(labels...)
	snapshotResources.DeletePartialMatch(labels...)
	snapshotDeferredClients.DeletePartialMatch(labels...)
}

// deferredClients returns the keys of connected clients that currently have no
// snapshot row, for tests and diagnostics.
func (d *snapshotDeferralTracker) deferredClients() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for key := range d.clients {
		if _, has := d.snapshots[key]; !has {
			out = append(out, key)
		}
	}
	return out
}
