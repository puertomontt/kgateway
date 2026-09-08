package proxy_syncer

import (
	"context"
	"errors"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/proxy_syncer/sharedproto"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/translator/irtranslator"
	sdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
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

// altStatNameOverlayTranslator returns a translator whose only overlay sets
// AltStatName to mark for clients that applies accepts, and declines the rest.
func altStatNameOverlayTranslator(mark string, applies func(ir.UniquelyConnectedClient) bool) *irtranslator.BackendTranslator {
	return &irtranslator.BackendTranslator{
		ContributedPolicies: map[schema.GroupKind]sdk.PolicyPlugin{
			{Group: "test", Kind: "Overlay"}: {
				PerClientClusterOverlay: func(_ krt.HandlerContext, _ context.Context, ucc ir.UniquelyConnectedClient, _ ir.BackendObjectIR) *sdk.ClusterOverlay {
					if !applies(ucc) {
						return nil
					}
					return &sdk.ClusterOverlay{Mutate: func(out *envoyclusterv3.Cluster) {
						out.AltStatName = mark
					}}
				},
			},
		},
	}
}

// evaluableBase is a base row that carries the inputs a reader needs to
// evaluate a client itself: a non-errored Base and a backend IR.
func evaluableBase(name string, cluster *envoyclusterv3.Cluster, version uint64) baseEnvoyCluster {
	backend := ir.NewBackendObjectIR(ir.ObjectSource{Namespace: "ns", Name: name}, 80, "", "")
	return baseEnvoyCluster{
		Name:           name,
		Cluster:        sharedproto.Wrap(cluster),
		ClusterVersion: version,
		Backend:        &backend,
		Base:           &irtranslator.BaseCluster{},
	}
}

func rowFromBase(b baseEnvoyCluster, clients *clientInputSnapshot, deltas ...uccClusterDelta) backendClusters {
	row := backendClusters{
		Name:              b.Name,
		Cluster:           b.Cluster,
		ClusterVersion:    b.ClusterVersion,
		Error:             b.Error,
		BackendSource:     b.BackendSource,
		BackendGeneration: b.BackendGeneration,
		Clients:           clients,
		Backend:           b.Backend,
		Base:              b.Base,
	}
	for _, d := range deltas {
		if row.Deltas == nil {
			row.Deltas = make(map[string]uccClusterDelta)
		}
		row.Deltas[d.Client.ResourceName()] = d
	}
	return row
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

	got := uccWithClusterByName(pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc))
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

	got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
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
	gotA := pcc.FetchClustersForClient(krt.TestingDummyContext{}, uccA)
	require.Len(t, gotA, 1)
	require.Equal(t, uint64(50), gotA[0].ClusterVersion)

	// uccB has no override, sees the shared base proto
	gotB := pcc.FetchClustersForClient(krt.TestingDummyContext{}, uccB)
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
	pcc := testPerClientClusters(clusterCol, clustersTestTranslator())
	waitSynced(t, pcc)

	got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
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
		got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
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
		got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		return len(got) == 1 && got[0].Cluster.Is(newDelta) && got[0].ClusterVersion == 100
	}, time.Second, 10*time.Millisecond)
}

// TestFetchClustersForClient_UnevaluatedClientIsServedOnFirstPass pins the
// property that replaced the client-resolution fence: a client the row has not
// evaluated is evaluated by the reader, from the row's own base and inputs, and
// receives its complete cluster immediately rather than nothing. When the row
// catches up, its override is byte-identical, so the version does not move.
func TestFetchClustersForClient_UnevaluatedClientIsServedOnFirstPass(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", map[string]string{"match": "yes"}, ir.PodLocality{})
	translator := altStatNameOverlayTranslator("overlaid", func(c ir.UniquelyConnectedClient) bool {
		return c.Labels["match"] == "yes"
	})
	base := evaluableBase("c", clusterNamed("c"), 1)
	clusterCol := krt.NewStaticCollection(nil, []backendClusters{rowFromBase(base, newClientInputSnapshot(nil))})
	pcc := testPerClientClusters(clusterCol, translator)
	waitSynced(t, pcc)

	got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
	require.Len(t, got, 1, "a client the row has not evaluated must still be served")
	require.NoError(t, got[0].Error)
	require.Equal(t, "overlaid", got[0].Cluster.Clone().GetAltStatName(),
		"the reader must apply the overlay itself, not fall back to the base")
	inlineVersion := got[0].ClusterVersion
	require.NotEqual(t, base.ClusterVersion, inlineVersion)

	// The row catches up with the connected client and carries the override
	// it computed for it; the reader now serves that and the version is stable.
	perClient := clusterNamed("c")
	perClient.AltStatName = "overlaid"
	rowDelta := uccClusterDelta{Client: ucc, Name: "c", Cluster: sharedproto.Wrap(perClient), ClusterVersion: inlineVersion}
	clusterCol.UpdateObject(rowFromBase(base, newClientInputSnapshot([]ir.UniquelyConnectedClient{ucc}), rowDelta))
	require.Eventually(t, func() bool {
		got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)
		return len(got) == 1 && got[0].Cluster.Is(perClient)
	}, time.Second, 10*time.Millisecond, "once the row has evaluated the client, its override must be served")
	require.Equal(t, inlineVersion, pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc)[0].ClusterVersion,
		"the reader's own evaluation and the row's must agree on the version")
}

// TestFetchClustersForClient_UnrelatedClientChurnIsNotAReadinessBarrier proves
// that an established client keeps using a backend row evaluated before another
// client connected, and that the late client is served too — from the reader's
// own evaluation — rather than waiting for the row.
func TestFetchClustersForClient_UnrelatedClientChurnIsNotAReadinessBarrier(t *testing.T) {
	stable := ir.NewUniquelyConnectedClient("stable", "ns", nil, ir.PodLocality{})
	late := ir.NewUniquelyConnectedClient("late", "ns", nil, ir.PodLocality{})
	base := evaluableBase("c", clusterNamed("c"), 1)

	clusterCol := krt.NewStaticCollection(nil, []backendClusters{
		rowFromBase(base, newClientInputSnapshot([]ir.UniquelyConnectedClient{stable})),
	})
	pcc := testPerClientClusters(clusterCol, clustersTestTranslator())
	waitSynced(t, pcc)

	stableClusters := pcc.FetchClustersForClient(krt.TestingDummyContext{}, stable)
	require.Len(t, stableClusters, 1, "unrelated client addition must not disturb the established client")
	require.True(t, stableClusters[0].Cluster.Is(base.Cluster.BorrowForRead()))
	lateClusters := pcc.FetchClustersForClient(krt.TestingDummyContext{}, late)
	require.Len(t, lateClusters, 1, "the newly connected client must be served without waiting for the row")
	require.True(t, lateClusters[0].Cluster.Is(base.Cluster.BorrowForRead()),
		"with no overlay applying, the reader serves the shared base proto itself")
}

// TestFetchClustersForClient_IdentityChangeIsEvaluatedAgainstTheNewIdentity
// proves that an override built for a previous version of the client is not
// served to the current one: the row's snapshot holds the old identity, so the
// reader evaluates the current identity itself and gets the current answer.
func TestFetchClustersForClient_IdentityChangeIsEvaluatedAgainstTheNewIdentity(t *testing.T) {
	oldUcc := ir.NewUniquelyConnectedClient("role", "ns", nil, ir.PodLocality{})
	currentUcc := oldUcc
	currentUcc.KnowsLocalCluster = true
	translator := altStatNameOverlayTranslator("local-cluster-capable", func(c ir.UniquelyConnectedClient) bool {
		return c.KnowsLocalCluster
	})
	base := evaluableBase("c", clusterNamed("c"), 1)

	// Row evaluated the old identity, for which no overlay applied.
	clusterCol := krt.NewStaticCollection(nil, []backendClusters{
		rowFromBase(base, newClientInputSnapshot([]ir.UniquelyConnectedClient{oldUcc})),
	})
	pcc := testPerClientClusters(clusterCol, translator)
	waitSynced(t, pcc)

	got := pcc.FetchClustersForClient(krt.TestingDummyContext{}, currentUcc)
	require.Len(t, got, 1)
	require.Equal(t, "local-cluster-capable", got[0].Cluster.Clone().GetAltStatName(),
		"the current identity must be evaluated, not read off the stale snapshot")

	// And the other direction: the row holds an override for the old identity
	// that must not be served to the current one.
	stale := clusterNamed("c")
	stale.AltStatName = "local-cluster-capable"
	clusterCol.UpdateObject(rowFromBase(base, newClientInputSnapshot([]ir.UniquelyConnectedClient{currentUcc}),
		uccClusterDelta{Client: currentUcc, Name: "c", Cluster: sharedproto.Wrap(stale), ClusterVersion: 7}))
	waitSynced(t, pcc)
	got = pcc.FetchClustersForClient(krt.TestingDummyContext{}, oldUcc)
	require.Len(t, got, 1)
	require.True(t, got[0].Cluster.Is(base.Cluster.BorrowForRead()),
		"an override built for another identity of this client must not be served")
}

// TestFetchClustersForClient_NoRowsReturnsNothing covers the one case the
// reader has nothing to say: no backend rows at all.
func TestFetchClustersForClient_NoRowsReturnsNothing(t *testing.T) {
	ucc := ir.NewUniquelyConnectedClient("role", "ns", nil, ir.PodLocality{})
	pcc := newTestPerClientClustersRaw(nil, nil, ucc)
	waitSynced(t, pcc)
	require.Empty(t, pcc.FetchClustersForClient(krt.TestingDummyContext{}, ucc))
	require.Empty(t, (&PerClientEnvoyClusters{}).FetchClustersForClient(krt.TestingDummyContext{}, ucc))
}
