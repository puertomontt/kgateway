package proxy_syncer

import (
	"istio.io/istio/pkg/kube/krt"

	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// testClusterCols keeps the static collection backing a test-built
// PerClientEnvoyClusters alive, along with the client snapshot its rows were
// built against, so tests can swap rows in place while every client stays
// evaluated.
type testClusterCols struct {
	clusters krt.StaticCollection[backendClusters]
	clients  *clientInputSnapshot
}

// updateBase replaces one backend's row with a resolved shared base carrying no
// overrides.
func (c *testClusterCols) updateBase(cluster uccWithCluster) {
	c.clusters.UpdateObject(resolvedBase(cluster, c.clients))
}

// resolvedBase projects a flat cluster entry onto a completed backend row that
// has evaluated clients and found no overrides.
func resolvedBase(cluster uccWithCluster, clients *clientInputSnapshot) backendClusters {
	return backendClusters{
		Name:              cluster.Name,
		Cluster:           cluster.Cluster,
		ClusterVersion:    cluster.ClusterVersion,
		Error:             cluster.Error,
		BackendSource:     cluster.BackendSource,
		BackendGeneration: cluster.BackendGeneration,
		Clients:           clients,
	}
}

// newTestPerClientClustersRaw builds a PerClientEnvoyClusters directly from
// base entries and sparse deltas, every row evaluated against clients. clients
// must include every UCC that the caller will query; passing them explicitly
// models the production client-resolution fence.
func newTestPerClientClustersRaw(
	bases []baseEnvoyCluster,
	deltas []uccClusterDelta,
	clients ...ir.UniquelyConnectedClient,
) PerClientEnvoyClusters {
	clientSnapshot := newClientInputSnapshot(clients)
	rows := make([]backendClusters, 0, len(bases))
	for _, base := range bases {
		row := backendClusters{
			Name:              base.Name,
			Cluster:           base.Cluster,
			ClusterVersion:    base.ClusterVersion,
			Error:             base.Error,
			BackendSource:     base.BackendSource,
			BackendGeneration: base.BackendGeneration,
			Clients:           clientSnapshot,
		}
		for _, delta := range deltas {
			if delta.Name != base.Name {
				continue
			}
			if row.Deltas == nil {
				row.Deltas = make(map[string]uccClusterDelta)
			}
			row.Deltas[delta.Client.ResourceName()] = delta
		}
		rows = append(rows, row)
	}
	return PerClientEnvoyClusters{
		clusters: krt.NewStaticCollection[backendClusters](nil, rows),
	}
}

// newTestPerClientClusters builds a PerClientEnvoyClusters from flat cluster
// entries. These snapshot tests do not exercise overlays, so each entry is a
// resolved shared base and each distinct client is included in the client set.
func newTestPerClientClusters(initial []uccWithCluster) (PerClientEnvoyClusters, *testClusterCols) {
	clientsByName := make(map[string]ir.UniquelyConnectedClient)
	for _, cluster := range initial {
		clientsByName[cluster.Client.ResourceName()] = cluster.Client
	}
	clients := make([]ir.UniquelyConnectedClient, 0, len(clientsByName))
	for _, client := range clientsByName {
		clients = append(clients, client)
	}
	clientSnapshot := newClientInputSnapshot(clients)

	rowsByName := make(map[string]backendClusters)
	for _, cluster := range initial {
		rowsByName[cluster.Name] = resolvedBase(cluster, clientSnapshot)
	}
	rows := make([]backendClusters, 0, len(rowsByName))
	for _, row := range rowsByName {
		rows = append(rows, row)
	}

	clusterCol := krt.NewStaticCollection[backendClusters](nil, rows)
	pcc := PerClientEnvoyClusters{clusters: clusterCol}
	return pcc, &testClusterCols{clusters: clusterCol, clients: clientSnapshot}
}
