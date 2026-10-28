package proxy_syncer

import (
	"maps"
	"reflect"
	"slices"
	"sync"

	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"google.golang.org/protobuf/proto"
)

// edsDemand is what one CDS payload asks of EDS: the CLA each EDS cluster
// requests. It is stored as exceptions to the common case, an EDS cluster
// requesting the CLA of its own name, so a fleet of kube Services costs nothing
// beyond the payload, and answering reads no cluster proto.
type edsDemand struct {
	clusters map[string]envoycachetypes.ResourceWithTTL
	// unrequested are the clusters that request no CLA.
	unrequested map[string]struct{}
	// aliases map the EDS clusters that request another name, such as a
	// service_name, to that name; aliasTargets are those names.
	aliases      map[string]string
	aliasTargets map[string]struct{}
}

func newEDSDemand(clusters map[string]envoycachetypes.ResourceWithTTL) edsDemand {
	d := edsDemand{clusters: clusters}
	for name, item := range clusters {
		endpointName, requiresEndpointResource := endpointResourceNameForCluster(item)
		switch {
		case !requiresEndpointResource:
			if d.unrequested == nil {
				d.unrequested = make(map[string]struct{})
			}
			d.unrequested[name] = struct{}{}
		case endpointName != name:
			if d.aliases == nil {
				d.aliases = make(map[string]string)
				d.aliasTargets = make(map[string]struct{})
			}
			d.aliases[name] = endpointName
			d.aliasTargets[endpointName] = struct{}{}
		}
	}
	return d
}

// requestsOwn reports whether cluster is an EDS cluster requesting the CLA of
// its own name.
func (d edsDemand) requestsOwn(cluster string) bool {
	if _, ok := d.clusters[cluster]; !ok {
		return false
	}
	if _, ok := d.unrequested[cluster]; ok {
		return false
	}
	_, aliased := d.aliases[cluster]
	return !aliased
}

// requires reports whether some cluster requests the CLA endpointName.
func (d edsDemand) requires(endpointName string) bool {
	if _, ok := d.aliasTargets[endpointName]; ok {
		return true
	}
	return d.requestsOwn(endpointName)
}

// snapshotMatchMemo keeps what xdsSnapshotsForUcc last derived from one client's
// cluster row, its EDS set and its translation, keyed on the identity of what
// each derivation reads (the translation's clusters, rebuilt on every run, by
// content). A run redoes only the walks whose inputs changed: after a
// translation-only change, the common case, none walks every cluster or CLA.
//
// Identity is sound because the memo holds the inputs it is keyed on, so none
// can be freed and its address reused, and published payloads are never
// mutated. Each derivation is pure, so a skipped one drops no KRT dependency.
// The memo lives on the client's cluster row and is dropped with it. It holds
// only the last run's inputs and outputs, one version of each, which that
// run's snapshot mostly holds too.
//
// A nil memo derives everything afresh.
type snapshotMatchMemo struct {
	// mu is uncontended, since KRT runs a collection's transform serially; it
	// keeps the memo safe should another reader of the row use it.
	mu sync.Mutex

	mergedFrom       map[string]envoycachetypes.ResourceWithTTL
	mergedExtras     []envoycachetypes.ResourceWithTTL
	mergedExtrasHash uint64
	merged           map[string]envoycachetypes.ResourceWithTTL

	demand edsDemand

	endpointsSet         bool
	endpointsClusters    map[string]envoycachetypes.ResourceWithTTL
	endpointsIn          envoycache.Resources
	endpointsRowHashes   map[string]uint64
	endpointsBootstrap   string
	endpointsOut         envoycache.Resources
	endpointsHashes      map[string]uint64
	endpointsSynthesized map[string]struct{}

	missingSet        bool
	missingReferenced map[string]struct{}
	missingClusters   map[string]envoycachetypes.ResourceWithTTL
	missingErrored    []string
	missing           []string
}

// mergeClusters returns clusters plus the translation's own clusters, which win
// on a name collision. The translation rebuilds its clusters on every run, so
// they are compared by content; there are few, as they scale with plugins.
func (m *snapshotMatchMemo) mergeClusters(clusters map[string]envoycachetypes.ResourceWithTTL, extras []envoycachetypes.ResourceWithTTL, extrasHash uint64) map[string]envoycachetypes.ResourceWithTTL {
	if m == nil {
		return mergeTranslationClusters(clusters, extras)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(extras) == 0 {
		// Nothing to merge; drop the last merge rather than hold a copy of CDS.
		m.mergedFrom, m.mergedExtras, m.mergedExtrasHash, m.merged = nil, nil, 0, nil
		return clusters
	}
	if m.merged == nil || !sameMap(m.mergedFrom, clusters) || m.mergedExtrasHash != extrasHash || !slices.EqualFunc(m.mergedExtras, extras, resourcesEqual) {
		m.mergedFrom, m.mergedExtras, m.mergedExtrasHash = clusters, extras, extrasHash
		m.merged = mergeTranslationClusters(clusters, extras)
	}
	return m.merged
}

// edsDemand returns what clusters ask of EDS.
func (m *snapshotMatchMemo) edsDemand(clusters map[string]envoycachetypes.ResourceWithTTL) edsDemand {
	if m == nil {
		return newEDSDemand(clusters)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The zero demand is that of a nil map, so it needs no set flag.
	if !sameMap(m.demand.clusters, clusters) {
		m.demand = newEDSDemand(clusters)
	}
	return m.demand
}

// filterEndpoints is filterEndpointResources, memoized.
func (m *snapshotMatchMemo) filterEndpoints(demand edsDemand, endpoints envoycache.Resources, rowHashes map[string]uint64, bootstrapEndpoint string) (envoycache.Resources, map[string]uint64, map[string]struct{}) {
	if m == nil {
		return filterEndpointResources(demand, endpoints, rowHashes, bootstrapEndpoint)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.endpointsSet ||
		!sameMap(m.endpointsClusters, demand.clusters) ||
		!sameMap(m.endpointsIn.Items, endpoints.Items) ||
		m.endpointsIn.Version != endpoints.Version ||
		!sameMap(m.endpointsRowHashes, rowHashes) ||
		m.endpointsBootstrap != bootstrapEndpoint {
		m.endpointsOut, m.endpointsHashes, m.endpointsSynthesized = filterEndpointResources(demand, endpoints, rowHashes, bootstrapEndpoint)
		m.endpointsSet = true
		m.endpointsClusters, m.endpointsIn, m.endpointsRowHashes, m.endpointsBootstrap = demand.clusters, endpoints, rowHashes, bootstrapEndpoint
	}
	return m.endpointsOut, m.endpointsHashes, m.endpointsSynthesized
}

// missingReferencedClusters is findMissingReferencedClusters, memoized.
func (m *snapshotMatchMemo) missingReferencedClusters(referenced map[string]struct{}, clusters map[string]envoycachetypes.ResourceWithTTL, erroredClusters []string) []string {
	if m == nil {
		return findMissingReferencedClusters(referenced, clusters, erroredClusters)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.missingSet ||
		!sameMap(m.missingReferenced, referenced) ||
		!sameMap(m.missingClusters, clusters) ||
		!slices.Equal(m.missingErrored, erroredClusters) {
		// Clipped, since every snapshot built from this memo shares it.
		m.missing = slices.Clip(findMissingReferencedClusters(referenced, clusters, erroredClusters))
		m.missingSet = true
		m.missingReferenced, m.missingClusters, m.missingErrored = referenced, clusters, erroredClusters
	}
	return m.missing
}

func mergeTranslationClusters(clusters map[string]envoycachetypes.ResourceWithTTL, extras []envoycachetypes.ResourceWithTTL) map[string]envoycachetypes.ResourceWithTTL {
	if len(extras) == 0 {
		return clusters
	}
	merged := make(map[string]envoycachetypes.ResourceWithTTL, len(extras)+len(clusters))
	maps.Copy(merged, clusters)
	for _, item := range extras {
		merged[envoycache.GetResourceName(item.Resource)] = item
	}
	return merged
}

func resourcesEqual(a, b envoycachetypes.ResourceWithTTL) bool {
	if a.TTL != b.TTL && (a.TTL == nil || b.TTL == nil || *a.TTL != *b.TTL) {
		return false
	}
	return a.Resource == b.Resource || proto.Equal(a.Resource, b.Resource)
}

// sameMap reports whether a and b are one map, not merely equal ones.
func sameMap[M ~map[K]V, K comparable, V any](a, b M) bool {
	return reflect.ValueOf(a).UnsafePointer() == reflect.ValueOf(b).UnsafePointer()
}
