package proxy_syncer

import (
	"errors"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/proxy_syncer/sharedproto"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

func clusterNamed(name string) *envoyclusterv3.Cluster {
	return &envoyclusterv3.Cluster{Name: name}
}

func uccWithClusterByName(got []uccWithCluster) map[string]uccWithCluster {
	m := make(map[string]uccWithCluster, len(got))
	for _, c := range got {
		m[c.Name] = c
	}
	return m
}

func waitSynced(t *testing.T, pcc PerClientEnvoyClusters) {
	t.Helper()
	require.Eventually(t, pcc.HasSynced, time.Second, 10*time.Millisecond)
}

// TestFetchClustersForClient_Merge exercises resolution of a row for one client:
// a base with no override passes through unchanged, while the client's override
// replaces the base of the same name (winning on cluster + version).
func TestFetchClustersForClient_Merge(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"k": "a"}, ir.PodLocality{})

	baseOnly := clusterNamed("base-only")
	overlaidBase := clusterNamed("overlaid")
	overlaidDelta := clusterNamed("overlaid")

	bases := []baseEnvoyCluster{
		{Name: "base-only", Cluster: sharedproto.Wrap(baseOnly), ClusterVersion: 1},
		{Name: "overlaid", Cluster: sharedproto.Wrap(overlaidBase), ClusterVersion: 2},
	}
	deltas := []uccClusterDelta{
		{Client: ucc, Name: "overlaid", Cluster: sharedproto.Wrap(overlaidDelta), ClusterVersion: 99},
	}

	pcc := newTestPerClientClustersRaw(bases, deltas, ucc)
	waitSynced(t, pcc)

	rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
	require.Equal(t, deferralNone, deferral, "a fully evaluated client must not report a deferral")
	got := uccWithClusterByName(rows)
	require.Len(t, got, 2)

	// base with no override passes through unchanged
	require.True(t, got["base-only"].Cluster.Is(baseOnly), "base with no override must alias the base proto")
	require.Equal(t, uint64(1), got["base-only"].ClusterVersion)

	// the override replaces the base of the same name, winning on cluster + version
	require.True(t, got["overlaid"].Cluster.Is(overlaidDelta), "override must win over the base of the same name")
	require.Equal(t, uint64(99), got["overlaid"].ClusterVersion)
}

// TestFetchClustersForClient_DeltaErrorWinsOverBaseError documents the error
// precedence in resolution: a per-UCC override error is the more specific signal
// and takes precedence over a base error of the same name.
func TestFetchClustersForClient_DeltaErrorWinsOverBaseError(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"k": "a"}, ir.PodLocality{})
	baseErr := errors.New("base boom")
	deltaErr := errors.New("delta boom")

	bases := []baseEnvoyCluster{{Name: "c", Cluster: sharedproto.Wrap(clusterNamed("c")), ClusterVersion: 1, Error: baseErr}}
	deltas := []uccClusterDelta{{Client: ucc, Name: "c", Cluster: sharedproto.Wrap(clusterNamed("c")), ClusterVersion: 2, Error: deltaErr}}

	pcc := newTestPerClientClustersRaw(bases, deltas, ucc)
	waitSynced(t, pcc)

	got, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
	require.Len(t, got, 1)
	require.Equal(t, deltaErr, got[0].Error, "override error should win over base error")
}

// TestFetchClustersForClient_FiltersByClient confirms an override is scoped to
// its own UCC: the owning client sees it while another client falls back to the
// shared base.
func TestFetchClustersForClient_FiltersByClient(t *testing.T) {
	uccA := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"k": "a"}, ir.PodLocality{})
	uccB := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"k": "b"}, ir.PodLocality{})

	base := clusterNamed("c")
	bases := []baseEnvoyCluster{{Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1}}
	deltas := []uccClusterDelta{{Client: uccA, Name: "c", Cluster: sharedproto.Wrap(clusterNamed("c-a")), ClusterVersion: 50}}

	pcc := newTestPerClientClustersRaw(bases, deltas, uccA, uccB)
	waitSynced(t, pcc)

	// uccA sees its override
	gotA, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, uccA)
	require.Len(t, gotA, 1)
	require.Equal(t, uint64(50), gotA[0].ClusterVersion)

	// uccB has no override, sees the shared base proto
	gotB, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, uccB)
	require.Len(t, gotB, 1)
	require.True(t, gotB[0].Cluster.Is(base), "client without an override must alias the shared base proto")
	require.Equal(t, uint64(1), gotB[0].ClusterVersion)
}

// TestFetchClustersForClient_BaseAndOverridesMoveTogether pins the property
// that replaced the base-generation fence: a row carries the base its overrides
// were cloned from, so a reader observes each update as one atomic step —
// there is no state in which a newer base is visible next to an older override.
func TestFetchClustersForClient_BaseAndOverridesMoveTogether(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"k": "a"}, ir.PodLocality{})
	clientSnapshot := newClientInputSnapshot([]ir.UniquelyConnectedClient{ucc})
	oldBase, oldDelta := clusterNamed("c"), clusterNamed("c")
	newBase, newDelta := clusterNamed("c"), clusterNamed("c")

	clusterCol := krt.NewStaticCollection(nil, []backendClusters{{
		Name:           "c",
		Cluster:        sharedproto.Wrap(oldBase),
		ClusterVersion: 1,
		Clients:        clientSnapshot,
		Deltas: map[string]uccClusterDelta{
			ucc.ResourceName(): {Client: ucc, Name: "c", Cluster: sharedproto.Wrap(oldDelta), ClusterVersion: 99},
		},
	}})
	pcc := PerClientEnvoyClusters{clusters: clusterCol}
	waitSynced(t, pcc)

	got, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
	require.Equal(t, deferralNone, deferral)
	require.Len(t, got, 1)
	require.True(t, got[0].Cluster.Is(oldDelta))

	// Base change that removes the override: the client lands on the new base
	// in one step, never on the new base with the old override.
	clusterCol.UpdateObject(backendClusters{
		Name:           "c",
		Cluster:        sharedproto.Wrap(newBase),
		ClusterVersion: 2,
		Clients:        clientSnapshot,
	})
	require.Eventually(t, func() bool {
		got, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		return len(got) == 1 && got[0].Cluster.Is(newBase) && got[0].ClusterVersion == 2
	}, time.Second, 10*time.Millisecond)

	// Base change that adds an override: the override arrives with its base.
	clusterCol.UpdateObject(backendClusters{
		Name:           "c",
		Cluster:        sharedproto.Wrap(newBase),
		ClusterVersion: 2,
		Clients:        clientSnapshot,
		Deltas: map[string]uccClusterDelta{
			ucc.ResourceName(): {Client: ucc, Name: "c", Cluster: sharedproto.Wrap(newDelta), ClusterVersion: 100},
		},
	})
	require.Eventually(t, func() bool {
		got, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		return len(got) == 1 && got[0].Cluster.Is(newDelta) && got[0].ClusterVersion == 100
	}, time.Second, 10*time.Millisecond)
}

// TestFetchClustersForClient_WaitsForCurrentClientSet proves that an empty
// sparse result from before a client connected is pending, not an affirmative
// "no overlay" decision for that client.
func TestFetchClustersForClient_WaitsForCurrentClientSet(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", nil, ir.PodLocality{})
	base := clusterNamed("c")
	clusterCol := krt.NewStaticCollection(nil, []backendClusters{{
		Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1, Clients: newClientInputSnapshot(nil),
	}})
	pcc := PerClientEnvoyClusters{clusters: clusterCol}
	waitSynced(t, pcc)

	rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
	require.Empty(t, rows)
	require.Equal(t, deferralUnresolvedClient, deferral)

	clusterCol.UpdateObject(backendClusters{
		Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1,
		Clients: newClientInputSnapshot([]ir.UniquelyConnectedClient{ucc}),
	})
	require.Eventually(t, func() bool {
		got, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		return len(got) == 1 && got[0].Cluster.Is(base)
	}, time.Second, 10*time.Millisecond)
}

// TestFetchClustersForClient_UnrelatedClientChurnIsNotAReadinessBarrier proves
// that an established client can keep using a backend row evaluated before
// another client connected. The old snapshot is authoritative for the stable
// client because it contains that client's exact current inputs; only the new
// client must wait for this backend to evaluate it.
func TestFetchClustersForClient_UnrelatedClientChurnIsNotAReadinessBarrier(t *testing.T) {
	stable := ir.NewUniquelyConnectedClient("stable", "ns", nil, ir.PodLocality{})
	late := ir.NewUniquelyConnectedClient("late", "ns", nil, ir.PodLocality{})
	base := clusterNamed("c")

	clusterCol := krt.NewStaticCollection(nil, []backendClusters{{
		Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1,
		Clients: newClientInputSnapshot([]ir.UniquelyConnectedClient{stable}),
	}})
	pcc := PerClientEnvoyClusters{clusters: clusterCol}
	waitSynced(t, pcc)

	stableClusters, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, stable)
	require.Len(t, stableClusters, 1, "unrelated client addition must not defer the established client")
	require.True(t, stableClusters[0].Cluster.Is(base))
	lateClusters, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, late)
	require.Empty(t, lateClusters, "the newly connected client must wait until this backend evaluates it")
	require.Equal(t, deferralUnresolvedClient, deferral)
}

// TestFetchClustersForClient_WaitsForCurrentLocalClusterCapability proves that
// an empty sparse result from before a client advertised local-cluster support
// is pending, not an affirmative "no overlay" decision for the changed client.
func TestFetchClustersForClient_WaitsForCurrentLocalClusterCapability(t *testing.T) {
	oldUcc := ir.NewUniquelyConnectedClient("role", "ns", nil, ir.PodLocality{})
	currentUcc := oldUcc
	currentUcc.KnowsLocalCluster = true
	stable := ir.NewUniquelyConnectedClient("stable", "ns", nil, ir.PodLocality{})

	base := clusterNamed("c")
	clusterCol := krt.NewStaticCollection(nil, []backendClusters{{
		Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1,
		Clients: newClientInputSnapshot([]ir.UniquelyConnectedClient{oldUcc, stable}),
	}})
	pcc := PerClientEnvoyClusters{clusters: clusterCol}
	waitSynced(t, pcc)

	rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, currentUcc)
	require.Empty(t, rows)
	require.Equal(t, deferralUnresolvedClient, deferral,
		"a client whose non-key field changed is unresolved by the old snapshot")
	stableClusters, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, stable)
	require.Len(t, stableClusters, 1, "another client's capability transition must not defer the stable client")
	require.True(t, stableClusters[0].Cluster.Is(base))

	clusterCol.UpdateObject(backendClusters{
		Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1,
		Clients: newClientInputSnapshot([]ir.UniquelyConnectedClient{currentUcc, stable}),
	})
	require.Eventually(t, func() bool {
		got, _ := pcc.FetchClustersForClient(krt.TestingDummyContext{}, currentUcc)
		return len(got) == 1 && got[0].Cluster.Is(base)
	}, time.Second, 10*time.Millisecond)
}

// TestFetchClustersForClient_NamesTheFenceThatFailed covers the deferral reasons
// the scenario tests above do not reach. The reason is what the deferral counter
// is labelled by, so each fence must report its own name and nothing else.
func TestFetchClustersForClient_NamesTheFenceThatFailed(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", nil, ir.PodLocality{})
	base := clusterNamed("c")

	t.Run("no backends", func(t *testing.T) {
		pcc := newTestPerClientClustersRaw(nil, nil, ucc)
		waitSynced(t, pcc)
		rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		require.Empty(t, rows)
		require.Equal(t, deferralNoBackends, deferral)
	})

	t.Run("zero value", func(t *testing.T) {
		pcc := PerClientEnvoyClusters{}
		rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		require.Empty(t, rows)
		require.Equal(t, deferralNoBackends, deferral)
	})

	// An override built for a previous version of the client is not proof the
	// current version was evaluated: the row's snapshot holds the old identity,
	// so the client is unresolved, exactly as if it had no override at all.
	t.Run("override built for an older version of the client", func(t *testing.T) {
		older := ucc
		older.KnowsLocalCluster = true
		clusterCol := krt.NewStaticCollection(nil, []backendClusters{{
			Name: "c", Cluster: sharedproto.Wrap(base), ClusterVersion: 1,
			Clients: newClientInputSnapshot([]ir.UniquelyConnectedClient{older}),
			Deltas: map[string]uccClusterDelta{
				ucc.ResourceName(): {Client: older, Name: "c", Cluster: sharedproto.Wrap(clusterNamed("c")), ClusterVersion: 2},
			},
		}})
		pcc := PerClientEnvoyClusters{clusters: clusterCol}
		waitSynced(t, pcc)
		rows, deferral := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		require.Empty(t, rows)
		require.Equal(t, deferralUnresolvedClient, deferral)
	})
}
