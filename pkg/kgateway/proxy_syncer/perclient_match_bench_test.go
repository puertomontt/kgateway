package proxy_syncer

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/proxy_syncer/sharedproto"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// The match benchmarks price the cross-type part of xdsSnapshotsForUcc at route
// scale (~10k clusters and CLAs, half of them routed to), for the three inputs
// that rerun it. Each alternates between two versions of the changing input, as
// a real change hands the transform new payloads:
//
//   - translation: the gateway's translation changed; the cluster row and EDS
//     set are the same objects as last run.
//   - endpoint: one CLA changed, so the EDS set is new.
//   - cluster: one cluster changed, so the cluster row is new.
//
// Shapes: steady (every EDS cluster has its CLA), extras (the translation adds a
// plugin cluster, merged into CDS), and gaps (1% of clusters have no CLA yet, so
// placeholders are synthesized and half of them hold routes).
const (
	matchBenchClusters = 10_000
	matchBenchGapEvery = 100
)

type matchBenchFixture struct {
	rows [2]*clustersWithErrors
	eds  [2]*endpointsWithUccName
	gws  [2]*GatewayXdsResources
}

type matchBenchResult struct {
	clusters                 envoycache.Resources
	endpoints                envoycache.Resources
	endpointHashes           map[string]uint64
	synthesized              map[string]struct{}
	missingClusters          []string
	missingEndpointsClusters []string
}

var matchBenchSink matchBenchResult

func matchBenchCluster(name string, timeout time.Duration) *envoyclusterv3.Cluster {
	return &envoyclusterv3.Cluster{
		Name:                 name,
		ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_EDS},
		EdsClusterConfig: &envoyclusterv3.Cluster_EdsClusterConfig{
			EdsConfig: &envoycorev3.ConfigSource{ConfigSourceSpecifier: &envoycorev3.ConfigSource_Ads{Ads: &envoycorev3.AggregatedConfigSource{}}},
		},
		ConnectTimeout: durationpb.New(timeout),
	}
}

func matchBenchCLA(name string, port uint32) *envoyendpointv3.ClusterLoadAssignment {
	return &envoyendpointv3.ClusterLoadAssignment{
		ClusterName: name,
		Endpoints: []*envoyendpointv3.LocalityLbEndpoints{{
			LbEndpoints: []*envoyendpointv3.LbEndpoint{{
				HostIdentifier: &envoyendpointv3.LbEndpoint_Endpoint{Endpoint: &envoyendpointv3.Endpoint{
					Address: &envoycorev3.Address{Address: &envoycorev3.Address_SocketAddress{SocketAddress: &envoycorev3.SocketAddress{
						Address:       "10.0.0.1",
						PortSpecifier: &envoycorev3.SocketAddress_PortValue{PortValue: port},
					}}},
				}},
			}},
		}},
	}
}

func matchBenchEndpoints(ucc ir.UniquelyConnectedClient, names []string, changedPort uint32) *endpointsWithUccName {
	items := make([]envoycachetypes.ResourceWithTTL, 0, len(names))
	rowHashes := make(map[string]uint64, len(names))
	var hash uint64
	for i, name := range names {
		port := uint32(8080)
		if i == 0 {
			port = changedPort
		}
		cla := matchBenchCLA(name, port)
		rowHash := utils.HashProto(cla)
		items = append(items, envoycachetypes.ResourceWithTTL{Resource: cla})
		rowHashes[name] = rowHash
		hash ^= rowHash
	}
	return &endpointsWithUccName{
		endpoints:    envoycache.NewResourcesWithTTL(strconv.FormatUint(hash, 10), items),
		rowHashes:    rowHashes,
		resourceName: ucc.ResourceName(),
	}
}

func matchBenchRow(ucc ir.UniquelyConnectedClient, names []string, changedTimeout time.Duration) *clustersWithErrors {
	rows := make([]uccWithCluster, 0, len(names))
	for i, name := range names {
		timeout := time.Second
		if i == 0 {
			timeout = changedTimeout
		}
		cluster := matchBenchCluster(name, timeout)
		version := utils.HashProto(cluster)
		rows = append(rows, uccWithCluster{
			Client:         ucc,
			Cluster:        sharedproto.WrapPrehashed(cluster, version),
			ClusterVersion: version,
			Name:           name,
		})
	}
	return assemblePerClientClusters(ucc, rows)
}

func matchBenchTranslation(names []string, extras bool) *GatewayXdsResources {
	referenced := make(map[string]struct{}, len(names)/2)
	for i, name := range names {
		if i%2 == 0 {
			referenced[name] = struct{}{}
		}
	}
	gw := &GatewayXdsResources{ReferencedClusters: referenced}
	if extras {
		gw.Clusters, gw.ClustersHash = sliceToResourcesHash([]*envoyclusterv3.Cluster{{
			Name:                 "metadata-server",
			ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_STRICT_DNS},
			ConnectTimeout:       durationpb.New(time.Second),
		}})
	}
	return gw
}

func newMatchBenchFixture(extras, gaps bool) *matchBenchFixture {
	ucc := benchClient("role", "pod")
	names := make([]string, 0, matchBenchClusters)
	claNames := make([]string, 0, matchBenchClusters)
	for i := range matchBenchClusters {
		name := fmt.Sprintf("kube_default_svc-%d_8080", i)
		names = append(names, name)
		if gaps && i%matchBenchGapEvery == matchBenchGapEvery-1 {
			continue
		}
		claNames = append(claNames, name)
	}
	f := &matchBenchFixture{}
	for i := range 2 {
		f.rows[i] = matchBenchRow(ucc, names, time.Duration(i+2)*time.Second)
		f.eds[i] = matchBenchEndpoints(ucc, claNames, uint32(9000+i))
		f.gws[i] = matchBenchTranslation(names, extras)
	}
	return f
}

// matchBenchAssemble runs what xdsSnapshotsForUcc derives from its three inputs
// once they are fetched, with scoping off and no bootstrap local cluster.
func matchBenchAssemble(row *clustersWithErrors, eds *endpointsWithUccName, gw *GatewayXdsResources) matchBenchResult {
	clusterResources := row.clusters
	memo := row.matchMemo
	if len(gw.Clusters) > 0 {
		clusterResources.Version = strconv.FormatUint(row.clustersHash^gw.ClustersHash, 10)
	}
	clusterResources.Items = memo.mergeClusters(clusterResources.Items, gw.Clusters, gw.ClustersHash)
	referenced := withBackendListenerRoutingTargets(gw.ReferencedClusters, row.backendListeners)
	missingClusters := memo.missingReferencedClusters(referenced, clusterResources.Items, row.erroredClusters)
	demand := memo.edsDemand(clusterResources.Items)
	endpointRes, endpointHashes, synthesized := memo.filterEndpoints(demand, eds.endpoints, eds.rowHashes, "")
	missingEndpointClusters := findMissingReferencedEndpointResources(referenced, demand, synthesized, row.erroredClusters)
	return matchBenchResult{
		clusters:                 clusterResources,
		endpoints:                endpointRes,
		endpointHashes:           endpointHashes,
		synthesized:              synthesized,
		missingClusters:          missingClusters,
		missingEndpointsClusters: missingEndpointClusters,
	}
}

// matchBenchFreshRow stands in for the cluster transform storing a new row.
func matchBenchFreshRow(row *clustersWithErrors) *clustersWithErrors {
	row.matchMemo = new(snapshotMatchMemo)
	return row
}

func BenchmarkSnapshotMatch(b *testing.B) {
	for _, shape := range []struct {
		name         string
		extras, gaps bool
	}{
		{name: "steady"},
		{name: "extras", extras: true},
		{name: "gaps", gaps: true},
	} {
		f := newMatchBenchFixture(shape.extras, shape.gaps)
		for _, scenario := range []struct {
			name string
			run  func(i int) matchBenchResult
		}{
			{name: "translation", run: func(i int) matchBenchResult {
				return matchBenchAssemble(f.rows[0], f.eds[0], f.gws[i%2])
			}},
			{name: "endpoint", run: func(i int) matchBenchResult {
				return matchBenchAssemble(f.rows[0], f.eds[i%2], f.gws[0])
			}},
			{name: "cluster", run: func(i int) matchBenchResult {
				return matchBenchAssemble(matchBenchFreshRow(f.rows[i%2]), f.eds[0], f.gws[0])
			}},
		} {
			b.Run(scenario.name+"/"+shape.name, func(b *testing.B) {
				disarmTripwire(b)
				// Warm up so the first measured run sees a previous one.
				matchBenchSink = scenario.run(1)
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					matchBenchSink = scenario.run(i)
					i++
				}
			})
		}
	}
}
