package proxy_syncer

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
)

// filterEndpointResourcesForClusters is filterEndpointResources for clusters
// whose demand is not yet derived.
func filterEndpointResourcesForClusters(clusters envoycache.Resources, endpoints envoycache.Resources, rowHashes map[string]uint64, bootstrapEndpoint string) (envoycache.Resources, map[string]uint64, map[string]struct{}) {
	return filterEndpointResources(newEDSDemand(clusters.Items), endpoints, rowHashes, bootstrapEndpoint)
}

// matchInputs are what xdsSnapshotsForUcc's matching reads, scoping aside: the
// cluster row, the EDS set, the translation, and the client's bootstrap cluster.
type matchInputs struct {
	clusters        envoycache.Resources
	clustersHash    uint64
	erroredClusters []string
	endpoints       envoycache.Resources
	rowHashes       map[string]uint64
	extras          []envoycachetypes.ResourceWithTTL
	extrasHash      uint64
	referenced      map[string]struct{}
	bootstrap       string
}

// memoMatch is xdsSnapshotsForUcc's matching as it now runs.
func memoMatch(m *snapshotMatchMemo, in matchInputs) matchBenchResult {
	clusterResources := in.clusters
	if len(in.extras) > 0 {
		clusterResources.Version = strconv.FormatUint(in.clustersHash^in.extrasHash, 10)
	}
	clusterResources.Items = m.mergeClusters(clusterResources.Items, in.extras, in.extrasHash)
	missingClusters := m.missingReferencedClusters(in.referenced, clusterResources.Items, in.erroredClusters)
	demand := m.edsDemand(clusterResources.Items)
	endpointRes, endpointHashes, synthesized := m.filterEndpoints(demand, in.endpoints, in.rowHashes, in.bootstrap)
	return matchBenchResult{
		clusters:                 clusterResources,
		endpoints:                endpointRes,
		endpointHashes:           endpointHashes,
		synthesized:              synthesized,
		missingClusters:          missingClusters,
		missingEndpointsClusters: findMissingReferencedEndpointResources(in.referenced, demand, synthesized, in.erroredClusters),
	}
}

// legacyMatch is xdsSnapshotsForUcc's matching before the memo, built on the
// helpers as they were (below), so the two can be compared output for output.
func legacyMatch(in matchInputs) matchBenchResult {
	clusterResources := in.clusters
	if len(in.extras) > 0 {
		clustersProto := make(map[string]envoycachetypes.ResourceWithTTL, len(in.extras)+len(clusterResources.Items))
		maps.Copy(clustersProto, clusterResources.Items)
		for _, item := range in.extras {
			clustersProto[envoycache.GetResourceName(item.Resource)] = item
		}
		clusterResources.Version = strconv.FormatUint(in.clustersHash^in.extrasHash, 10)
		clusterResources.Items = clustersProto
	}
	missingClusters := legacyFindMissingReferencedClusters(in.referenced, clusterResources.Items, in.erroredClusters)
	endpointRes, endpointHashes, synthesized := legacyFilterEndpointResourcesForClusters(clusterResources, in.endpoints, in.rowHashes, in.bootstrap)
	return matchBenchResult{
		clusters:                 clusterResources,
		endpoints:                endpointRes,
		endpointHashes:           endpointHashes,
		synthesized:              synthesized,
		missingClusters:          missingClusters,
		missingEndpointsClusters: legacyFindMissingReferencedEndpointResources(in.referenced, clusterResources.Items, synthesized, in.erroredClusters),
	}
}

func legacyFindMissingReferencedClusters(referencedClusters map[string]struct{}, clusters map[string]envoycachetypes.ResourceWithTTL, erroredClusters []string) []string {
	erroredClusterSet := stringSet(erroredClusters)
	missingClusters := make([]string, 0, len(referencedClusters))
	for name := range referencedClusters {
		if _, ok := clusters[name]; ok {
			continue
		}
		if _, ok := erroredClusterSet[name]; ok {
			continue
		}
		if name == wellknown.BlackholeClusterName {
			continue
		}
		missingClusters = append(missingClusters, name)
	}
	slices.Sort(missingClusters)
	return missingClusters
}

func legacyFindMissingReferencedEndpointResources(referencedClusters map[string]struct{}, clusters map[string]envoycachetypes.ResourceWithTTL, synthesizedEndpoints map[string]struct{}, erroredClusters []string) []string {
	erroredClusterSet := stringSet(erroredClusters)
	missingEndpointClusters := make([]string, 0, len(referencedClusters))
	for name := range referencedClusters {
		if _, ok := erroredClusterSet[name]; ok {
			continue
		}
		if name == wellknown.BlackholeClusterName {
			continue
		}
		clusterResource, ok := clusters[name]
		if !ok {
			continue
		}
		endpointResourceName, requiresEndpointResource := endpointResourceNameForCluster(clusterResource)
		if !requiresEndpointResource {
			continue
		}
		if _, synthesized := synthesizedEndpoints[endpointResourceName]; !synthesized {
			continue
		}
		missingEndpointClusters = append(missingEndpointClusters, name)
	}
	slices.Sort(missingEndpointClusters)
	return missingEndpointClusters
}

func legacyFilterEndpointResourcesForClusters(clusters envoycache.Resources, endpoints envoycache.Resources, rowHashes map[string]uint64, bootstrapEndpoint string) (envoycache.Resources, map[string]uint64, map[string]struct{}) {
	if legacyEndpointsMatchClusters(clusters, endpoints, bootstrapEndpoint) {
		return endpoints, rowHashes, nil
	}
	requiredEndpointNames := make(map[string]struct{}, len(clusters.Items)+1)
	for _, item := range clusters.Items {
		if endpointName, requiresEndpointResource := endpointResourceNameForCluster(item); requiresEndpointResource {
			requiredEndpointNames[endpointName] = struct{}{}
		}
	}
	if bootstrapEndpoint != "" {
		requiredEndpointNames[bootstrapEndpoint] = struct{}{}
	}
	covered := make(map[string]struct{}, len(requiredEndpointNames))
	filteredEndpoints := make([]envoycachetypes.ResourceWithTTL, 0, len(endpoints.Items))
	for _, item := range endpoints.Items {
		cla, ok := item.Resource.(*envoyendpointv3.ClusterLoadAssignment)
		if !ok {
			continue
		}
		if _, required := requiredEndpointNames[cla.GetClusterName()]; !required {
			continue
		}
		filteredEndpoints = append(filteredEndpoints, item)
		covered[cla.GetClusterName()] = struct{}{}
	}
	var synthesized map[string]struct{}
	for name := range requiredEndpointNames {
		if _, ok := covered[name]; ok {
			continue
		}
		if synthesized == nil {
			synthesized = make(map[string]struct{})
		}
		filteredEndpoints = append(filteredEndpoints, envoycachetypes.ResourceWithTTL{Resource: &envoyendpointv3.ClusterLoadAssignment{ClusterName: name}})
		synthesized[name] = struct{}{}
	}
	if len(synthesized) == 0 && len(filteredEndpoints) == len(endpoints.Items) {
		return endpoints, rowHashes, nil
	}
	filtered := envoycache.NewResourcesWithTTL("", filteredEndpoints)
	hashes := endpointSetHashes(filtered.Items, rowHashes, nil)
	return versionEndpointSet(filtered, hashes), hashes, synthesized
}

func legacyEndpointsMatchClusters(clusters envoycache.Resources, endpoints envoycache.Resources, bootstrapEndpoint string) bool {
	required := 0
	for name, item := range clusters.Items {
		endpointName, requiresEndpointResource := endpointResourceNameForCluster(item)
		if !requiresEndpointResource {
			continue
		}
		if endpointName != name || endpointName == bootstrapEndpoint {
			return false
		}
		required++
	}
	if bootstrapEndpoint != "" {
		required++
	}
	if len(endpoints.Items) != required {
		return false
	}
	for name, item := range endpoints.Items {
		if _, ok := item.Resource.(*envoyendpointv3.ClusterLoadAssignment); !ok {
			return false
		}
		if bootstrapEndpoint != "" && name == bootstrapEndpoint {
			continue
		}
		cluster, ok := clusters.Items[name]
		if !ok {
			return false
		}
		if _, requiresEndpointResource := endpointResourceNameForCluster(cluster); !requiresEndpointResource {
			return false
		}
	}
	return true
}

// expectSameMatch requires got to publish exactly what want does from in: the
// same versions, the same resources byte for byte, and the same gap
// classification, nil-ness included. Resources want passes through from its
// inputs must be the same objects, except the translation's clusters: after a
// rebuild of equal ones, a memo hit publishes the previous, equal, protos.
func expectSameMatch(g gomega.Gomega, in matchInputs, want, got matchBenchResult, desc string) {
	translated := make(map[string]struct{}, len(in.extras))
	for _, item := range in.extras {
		translated[envoycache.GetResourceName(item.Resource)] = struct{}{}
	}
	g.Expect(got.clusters.Version).To(gomega.Equal(want.clusters.Version), desc+": CDS version")
	expectSameResources(g, want.clusters.Items, got.clusters.Items, translated, desc+": CDS")
	g.Expect(got.endpoints.Version).To(gomega.Equal(want.endpoints.Version), desc+": EDS version")
	expectSameResources(g, want.endpoints.Items, got.endpoints.Items, want.synthesized, desc+": EDS")
	g.Expect(got.endpointHashes).To(gomega.Equal(want.endpointHashes), desc+": EDS hashes")
	g.Expect(got.synthesized == nil).To(gomega.Equal(want.synthesized == nil), desc+": synthesized nil-ness")
	g.Expect(got.synthesized).To(gomega.Equal(want.synthesized), desc+": synthesized")
	g.Expect(got.missingClusters).To(gomega.Equal(want.missingClusters), desc+": missing clusters")
	g.Expect(got.missingEndpointsClusters).To(gomega.Equal(want.missingEndpointsClusters), desc+": missing endpoint clusters")
}

// expectSameResources compares got to want byte for byte, and as objects for
// every name not in rebuilt.
func expectSameResources(g gomega.Gomega, want, got map[string]envoycachetypes.ResourceWithTTL, rebuilt map[string]struct{}, desc string) {
	g.Expect(slices.Sorted(maps.Keys(got))).To(gomega.Equal(slices.Sorted(maps.Keys(want))), desc+" names")
	marshal := proto.MarshalOptions{Deterministic: true}
	for name, w := range want {
		gr := got[name]
		g.Expect(gr.TTL).To(gomega.Equal(w.TTL), desc+" "+name+" TTL")
		wantBytes, err := marshal.Marshal(w.Resource)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		gotBytes, err := marshal.Marshal(gr.Resource)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(gotBytes).To(gomega.Equal(wantBytes), desc+" "+name+" bytes")
		if _, ok := rebuilt[name]; !ok {
			g.Expect(gr.Resource).To(gomega.BeIdenticalTo(w.Resource), desc+" "+name+" passed through")
		}
	}
}

func matchEDS(name, serviceName string) envoycachetypes.ResourceWithTTL {
	c := &envoyclusterv3.Cluster{Name: name, ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_EDS}}
	if serviceName != "" {
		c.EdsClusterConfig = &envoyclusterv3.Cluster_EdsClusterConfig{ServiceName: serviceName}
	}
	return envoycachetypes.ResourceWithTTL{Resource: c}
}

func matchStatic(name string) envoycachetypes.ResourceWithTTL {
	return envoycachetypes.ResourceWithTTL{Resource: &envoyclusterv3.Cluster{Name: name, ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_STATIC}}}
}

// matchCLA carries an endpoint, so it never marshals like a placeholder.
func matchCLA(name string) envoycachetypes.ResourceWithTTL {
	return envoycachetypes.ResourceWithTTL{Resource: matchBenchCLA(name, 8080)}
}

func matchClusters(items ...envoycachetypes.ResourceWithTTL) (envoycache.Resources, uint64) {
	var hash uint64
	for _, item := range items {
		hash ^= utils.HashProto(item.Resource)
	}
	return envoycache.NewResourcesWithTTL(strconv.FormatUint(hash, 10), items), hash
}

func matchEndpoints(items ...envoycachetypes.ResourceWithTTL) (envoycache.Resources, map[string]uint64) {
	rowHashes := make(map[string]uint64, len(items))
	var hash uint64
	for _, item := range items {
		h := utils.HashProto(item.Resource)
		rowHashes[envoycache.GetResourceName(item.Resource)] = h
		hash ^= h
	}
	return envoycache.NewResourcesWithTTL(strconv.FormatUint(hash, 10), items), rowHashes
}

func matchExtras(items ...envoycachetypes.ResourceWithTTL) ([]envoycachetypes.ResourceWithTTL, uint64) {
	var hash uint64
	for _, item := range items {
		hash ^= utils.HashProto(item.Resource)
	}
	return items, hash
}

func matchNames(names ...string) map[string]struct{} {
	return stringSet(names)
}

func newMatchInputs(clusters []envoycachetypes.ResourceWithTTL, endpoints []envoycachetypes.ResourceWithTTL, referenced map[string]struct{}) matchInputs {
	in := matchInputs{referenced: referenced}
	in.clusters, in.clustersHash = matchClusters(clusters...)
	in.endpoints, in.rowHashes = matchEndpoints(endpoints...)
	return in
}

// The memoized matching publishes what the walks it replaces did, for every
// shape the helpers special-case, on a fresh memo and on a warm one.
func TestSnapshotMatchMatchesLegacy(t *testing.T) {
	type tc struct {
		name string
		in   matchInputs
	}
	withBootstrap := func(in matchInputs, bootstrap string) matchInputs {
		in.bootstrap = bootstrap
		return in
	}
	withErrored := func(in matchInputs, errored ...string) matchInputs {
		in.erroredClusters = errored
		return in
	}
	withExtras := func(in matchInputs, extras ...envoycachetypes.ResourceWithTTL) matchInputs {
		in.extras, in.extrasHash = matchExtras(extras...)
		return in
	}
	ab := []envoycachetypes.ResourceWithTTL{matchEDS("a", ""), matchEDS("b", "")}
	cases := []tc{
		{name: "empty", in: newMatchInputs(nil, nil, nil)},
		{name: "matching", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "b"))},
		{name: "missing CLA is synthesized", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a", "b"))},
		{name: "unreferenced missing CLA is synthesized", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a"))},
		{name: "orphan CLA is dropped", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b"), matchCLA("orphan")}, matchNames("a"))},
		{name: "orphan in place of a CLA", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("orphan")}, matchNames("a", "b"))},
		{name: "static cluster CLA is dropped", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", ""), matchStatic("s")}, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("s")}, matchNames("a", "s"))},
		{name: "unnamed CLA", in: newMatchInputs(ab[:1], []envoycachetypes.ResourceWithTTL{matchCLA("")}, matchNames("a"))},
		{name: "non-CLA in the EDS set", in: newMatchInputs(ab[:1], []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchStatic("x")}, matchNames("a"))},
		{name: "referenced cluster absent from CDS", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "gone"))},
		{name: "blackhole referenced", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("b", wellknown.BlackholeClusterName))},
		{name: "blackhole as an EDS cluster without CLA", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS(wellknown.BlackholeClusterName, "")}, nil, matchNames(wellknown.BlackholeClusterName))},
		{name: "errored cluster referenced", in: withErrored(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "broken")), "broken")},
		{name: "errored name also translated", in: withErrored(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a", "b")), "b")},
		{name: "service name alias", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "svc-a")}, []envoycachetypes.ResourceWithTTL{matchCLA("svc-a")}, matchNames("a"))},
		{name: "service name alias without CLA", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "svc-a")}, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a"))},
		{name: "two aliases of one CLA", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "svc"), matchEDS("b", "svc"), matchEDS("c", "")}, []envoycachetypes.ResourceWithTTL{matchCLA("c")}, matchNames("a", "b", "c"))},
		{name: "alias of another cluster's name", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "b"), matchEDS("b", "")}, nil, matchNames("a", "b"))},
		{name: "alias of an unrequested cluster's name", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "s"), matchStatic("s")}, []envoycachetypes.ResourceWithTTL{matchCLA("s")}, matchNames("a", "s"))},
		{name: "bootstrap present", in: withBootstrap(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b"), matchCLA("local")}, matchNames("a")), "local")},
		{name: "bootstrap absent", in: withBootstrap(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "local")), "local")},
		{name: "bootstrap not subscribed", in: newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b"), matchCLA("local")}, matchNames("a"))},
		{name: "bootstrap is an EDS cluster", in: withBootstrap(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "b")), "b")},
		{name: "bootstrap is an alias target", in: withBootstrap(newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "local")}, nil, matchNames("a")), "local")},
		{name: "unnamed EDS cluster", in: newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("", "")}, []envoycachetypes.ResourceWithTTL{matchCLA("")}, matchNames(""))},
		{name: "translation adds a cluster", in: withExtras(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "x")), matchStatic("x"))},
		{name: "translation adds an EDS cluster without CLA", in: withExtras(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "x")), matchEDS("x", ""))},
		{name: "translation overrides a cluster", in: withExtras(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("a", "b")), matchStatic("b"))},
		{name: "translation repeats a name", in: withExtras(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a", "x")), matchStatic("x"), matchEDS("x", ""))},
		{name: "translation adds an errored name", in: withErrored(withExtras(newMatchInputs(ab, []envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("b")}, matchNames("x")), matchEDS("x", "")), "x")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			want := legacyMatch(c.in)
			memo := new(snapshotMatchMemo)
			expectSameMatch(g, c.in, want, memoMatch(memo, c.in), "fresh memo")
			expectSameMatch(g, c.in, want, memoMatch(memo, c.in), "warm memo")
			expectSameMatch(g, c.in, want, memoMatch(nil, c.in), "nil memo")
		})
	}
}

// One memo fed a random walk of input changes, one input at a time as the
// transform sees them, always publishes what the walks it replaces did.
func TestSnapshotMatchMatchesLegacyRandomized(t *testing.T) {
	g := gomega.NewWithT(t)
	rng := rand.New(rand.NewPCG(14, 2026)) //nolint:gosec // G404: a fixed seed keeps the walk reproducible
	names := []string{"a", "b", "c", "d", "e", "f", "", wellknown.BlackholeClusterName}
	pick := func() string { return names[rng.IntN(len(names))] }
	randomCluster := func() envoycachetypes.ResourceWithTTL {
		name := pick()
		switch rng.IntN(5) {
		case 0:
			return matchStatic(name)
		case 1:
			return matchEDS(name, "svc-"+pick())
		case 2:
			return matchEDS(name, pick())
		default:
			return matchEDS(name, "")
		}
	}
	randomClusters := func() []envoycachetypes.ResourceWithTTL {
		var out []envoycachetypes.ResourceWithTTL
		for range rng.IntN(7) {
			out = append(out, randomCluster())
		}
		return out
	}
	randomEndpoints := func() []envoycachetypes.ResourceWithTTL {
		var out []envoycachetypes.ResourceWithTTL
		for range rng.IntN(7) {
			name := pick()
			switch rng.IntN(6) {
			case 0:
				name = "svc-" + name
			case 1:
				out = append(out, matchStatic(name))
				continue
			}
			out = append(out, matchCLA(name))
		}
		return out
	}
	randomNames := func() []string {
		var out []string
		for range rng.IntN(5) {
			out = append(out, pick())
		}
		return out
	}

	memo := new(snapshotMatchMemo)
	in := newMatchInputs(randomClusters(), randomEndpoints(), matchNames(randomNames()...))
	for step := range 5000 {
		switch rng.IntN(9) {
		case 0:
			in.clusters, in.clustersHash = matchClusters(randomClusters()...)
		case 1:
			// A new cluster row: the transform stores a fresh memo with it.
			in.clusters, in.clustersHash = matchClusters(randomClusters()...)
			in.erroredClusters = randomNames()
			memo = new(snapshotMatchMemo)
		case 2:
			in.endpoints, in.rowHashes = matchEndpoints(randomEndpoints()...)
		case 3:
			in.referenced = matchNames(randomNames()...)
		case 4:
			if rng.IntN(2) == 0 {
				in.extras, in.extrasHash = nil, 0
			} else {
				in.extras, in.extrasHash = matchExtras(randomClusters()...)
			}
		case 5:
			// The next translation: equal clusters, rebuilt.
			rebuilt := make([]envoycachetypes.ResourceWithTTL, 0, len(in.extras))
			for _, item := range in.extras {
				rebuilt = append(rebuilt, envoycachetypes.ResourceWithTTL{Resource: proto.Clone(item.Resource)})
			}
			in.extras = rebuilt
			in.referenced = maps.Clone(in.referenced)
		case 6:
			in.bootstrap = []string{"", "local", pick(), "svc-" + pick()}[rng.IntN(4)]
		case 7:
			in.erroredClusters = randomNames()
		case 8:
			// Nothing changed: a run for another input, or a stale one.
		}
		expectSameMatch(g, in, legacyMatch(in), memoMatch(memo, in), fmt.Sprintf("step %d", step))
	}
}

// Changing any one input the memo is keyed on recomputes what reads it, and
// only that: an unchanged input's derivation is reused as the same object.
func TestSnapshotMatchMemoInvalidation(t *testing.T) {
	base := func() matchInputs {
		in := newMatchInputs(
			[]envoycachetypes.ResourceWithTTL{matchEDS("a", ""), matchEDS("b", ""), matchEDS("c", "svc-c")},
			[]envoycachetypes.ResourceWithTTL{matchCLA("a"), matchCLA("svc-c"), matchCLA("orphan")},
			matchNames("a", "b", "gone"),
		)
		in.extras, in.extrasHash = matchExtras(matchEDS("x", ""))
		in.erroredClusters = []string{"broken"}
		return in
	}
	for _, tc := range []struct {
		name   string
		change func(in *matchInputs)
		// Which derivations must be rebuilt rather than reused.
		merged, endpoints, missing bool
	}{
		{
			name: "cluster row",
			change: func(in *matchInputs) {
				in.clusters, in.clustersHash = matchClusters(matchEDS("a", ""), matchStatic("b"), matchEDS("c", "svc-c"))
			},
			merged: true, endpoints: true, missing: true,
		},
		{
			name:   "translation clusters",
			change: func(in *matchInputs) { in.extras, in.extrasHash = matchExtras(matchStatic("x")) },
			merged: true, endpoints: true, missing: true,
		},
		{
			name: "translation clusters with a colliding hash",
			change: func(in *matchInputs) {
				in.extras, _ = matchExtras(matchStatic("x"))
			},
			merged: true, endpoints: true, missing: true,
		},
		{
			name: "translation clusters hash alone",
			change: func(in *matchInputs) {
				in.extrasHash++
			},
			merged: true, endpoints: true, missing: true,
		},
		{
			name: "translation clusters rebuilt equal",
			change: func(in *matchInputs) {
				in.extras = []envoycachetypes.ResourceWithTTL{{Resource: proto.Clone(in.extras[0].Resource)}}
			},
		},
		{
			name: "translation clusters TTL",
			change: func(in *matchInputs) {
				ttl := time.Minute
				in.extras = []envoycachetypes.ResourceWithTTL{{Resource: in.extras[0].Resource, TTL: &ttl}}
			},
			merged: true, endpoints: true, missing: true,
		},
		{
			name:      "EDS set",
			change:    func(in *matchInputs) { in.endpoints, in.rowHashes = matchEndpoints(matchCLA("a"), matchCLA("b")) },
			endpoints: true,
		},
		{
			name:      "EDS version alone",
			change:    func(in *matchInputs) { in.endpoints.Version = "other" },
			endpoints: true,
		},
		{
			name: "row hashes alone",
			change: func(in *matchInputs) {
				in.rowHashes = maps.Clone(in.rowHashes)
				in.rowHashes["a"]++
			},
			endpoints: true,
		},
		{
			name:      "bootstrap",
			change:    func(in *matchInputs) { in.bootstrap = "local" },
			endpoints: true,
		},
		{
			name:    "referenced",
			change:  func(in *matchInputs) { in.referenced = matchNames("a", "b", "c", "gone", "x") },
			missing: true,
		},
		{
			name:    "referenced rebuilt equal",
			change:  func(in *matchInputs) { in.referenced = maps.Clone(in.referenced) },
			missing: true,
		},
		{
			name:    "errored clusters",
			change:  func(in *matchInputs) { in.erroredClusters = []string{"gone"} },
			missing: true,
		},
		{
			name:   "nothing",
			change: func(*matchInputs) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			in := base()
			memo := new(snapshotMatchMemo)
			before := memoMatch(memo, in)
			tc.change(&in)
			after := memoMatch(memo, in)
			expectSameMatch(g, in, legacyMatch(in), after, "after the change")

			g.Expect(sameMap(after.clusters.Items, before.clusters.Items)).To(gomega.Equal(!tc.merged), "merged CDS reused")
			g.Expect(sameMap(memo.demand.clusters, before.clusters.Items)).To(gomega.Equal(!tc.merged), "EDS demand reused")
			g.Expect(sameMap(after.endpoints.Items, before.endpoints.Items) && sameMap(after.endpointHashes, before.endpointHashes)).
				To(gomega.Equal(!tc.endpoints), "filtered EDS reused")
			g.Expect(sameSlice(after.missingClusters, before.missingClusters)).To(gomega.Equal(!tc.missing), "missing clusters reused")
		})
	}
}

// A translation that stops adding clusters drops the memo's merged copy of CDS,
// which would otherwise stay alive with the row.
func TestSnapshotMatchMemoDropsUnusedMerge(t *testing.T) {
	g := gomega.NewWithT(t)
	in := newMatchInputs([]envoycachetypes.ResourceWithTTL{matchEDS("a", "")}, []envoycachetypes.ResourceWithTTL{matchCLA("a")}, matchNames("a"))
	in.extras, in.extrasHash = matchExtras(matchStatic("x"))
	memo := new(snapshotMatchMemo)
	memoMatch(memo, in)
	g.Expect(memo.merged).To(gomega.HaveKey("x"), "merged while the translation adds clusters")

	in.extras, in.extrasHash = nil, 0
	expectSameMatch(g, in, legacyMatch(in), memoMatch(memo, in), "without translation clusters")
	g.Expect(memo.merged).To(gomega.BeNil())
	g.Expect(memo.mergedFrom).To(gomega.BeNil())
	g.Expect(memo.mergedExtras).To(gomega.BeNil())
}

func sameSlice[S ~[]E, E any](a, b S) bool {
	return len(a) == len(b) && cap(a) == cap(b) && (cap(a) == 0 || &a[:cap(a)][0] == &b[:cap(b)][0])
}

// The memo rides on the row the cluster transform builds, so every client's
// snapshot gets one, and the transform needs no other per-client state.
func TestAssemblePerClientClustersCarriesMatchMemo(t *testing.T) {
	g := gomega.NewWithT(t)
	row := assemblePerClientClusters(benchClient("role", "pod"), nil)
	g.Expect(row.matchMemo).NotTo(gomega.BeNil())
	other := assemblePerClientClusters(benchClient("role", "pod"), nil)
	g.Expect(other.matchMemo).NotTo(gomega.BeIdenticalTo(row.matchMemo), "each row has its own memo")
	g.Expect(other.Equals(*row)).To(gomega.BeTrue(), "the memo is not compared")
}
