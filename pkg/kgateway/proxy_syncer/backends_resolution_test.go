package proxy_syncer

import (
	"context"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/translator/irtranslator"
	sdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
)

// A row's stored client snapshot is what lets FetchClustersForClient read a
// sparse absence as "no overlay applies" rather than "not evaluated yet". If
// Equals accepted a stale snapshot, KRT would keep the old row, the requesting
// client would never appear in it, and its CDS would be withheld with no event
// left to recover it. Snapshots are therefore compared by identity: the interner
// hands out one pointer per distinct client set, so a changed set is always a
// different pointer and an unchanged set is always the same one.
func TestBackendClustersEqualsComparesSnapshotIdentity(t *testing.T) {
	clientA := ir.NewUniquelyConnectedClient("a", "ns", map[string]string{"id": "a"}, ir.PodLocality{})
	clientB := ir.NewUniquelyConnectedClient("b", "ns", map[string]string{"id": "b"}, ir.PodLocality{})

	interner := &clientInputSnapshotInterner{}
	before := interner.intern([]ir.UniquelyConnectedClient{clientA})
	after := interner.intern([]ir.UniquelyConnectedClient{clientA, clientB})
	require.NotSame(t, before, after, "a genuinely different client set must get its own snapshot")
	assert.False(t, before.ContainsCurrent(clientB),
		"the stale snapshot is exactly the one that would withhold clientB forever")

	rowFor := func(snapshot *clientInputSnapshot) backendClusters {
		return backendClusters{Name: "cluster", ClusterVersion: 1, Clients: snapshot}
	}
	assert.False(t, rowFor(before).Equals(rowFor(after)),
		"a row whose client snapshot moved must not compare equal")

	// Re-interning an unchanged set must return the same snapshot, or every
	// backend would republish on every recompute.
	same := interner.intern([]ir.UniquelyConnectedClient{clientA, clientB})
	require.Same(t, after, same, "an unchanged client set must reuse its snapshot")
	assert.True(t, rowFor(after).Equals(rowFor(same)))

	// A client whose non-key identity moved is a different set.
	moved := clientB
	moved.KnowsLocalCluster = true
	next := interner.intern([]ir.UniquelyConnectedClient{clientA, moved})
	require.NotSame(t, after, next, "an in-place identity change must produce a new snapshot")
	assert.False(t, next.ContainsCurrent(clientB))
	assert.True(t, next.ContainsCurrent(moved))
}

// Routes reference a backend by its memoized ClusterName, so a cluster renamed
// during translation would be published under a name nothing can reach. Nothing
// in tree renames it, so this pins the containment: the renamed backend alone is
// dropped, and the clients keep serving every other backend.
func TestNewPerClientEnvoyClusters_RenamedClusterDropsOnlyThatBackend(t *testing.T) {
	ctx := t.Context()
	krtopts := krtutil.NewKrtOptions(ctx.Done(), nil)
	backendGK := schema.GroupKind{Group: "group", Kind: "kind"}

	translator := &irtranslator.BackendTranslator{
		ContributedBackends: map[schema.GroupKind]ir.BackendInit{
			backendGK: {
				InitEnvoyBackend: func(ctx context.Context, in ir.BackendObjectIR, out *envoyclusterv3.Cluster) *ir.EndpointsForBackend {
					out.ClusterDiscoveryType = &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_EDS}
					if in.GetName() == "renamed" {
						out.Name = "something-else"
					}
					return nil
				},
			},
		},
		ContributedPolicies: map[schema.GroupKind]sdk.PolicyPlugin{},
	}

	good := ir.NewBackendObjectIR(ir.ObjectSource{Group: "group", Kind: "kind", Namespace: "ns", Name: "good"}, 80, "", "")
	renamed := ir.NewBackendObjectIR(ir.ObjectSource{Group: "group", Kind: "kind", Namespace: "ns", Name: "renamed"}, 80, "", "")
	finalBackends := krt.NewStaticCollection(nil, []*ir.BackendObjectIR{&good, &renamed},
		krtopts.ToOptions("FinalBackends")...)
	client := ir.NewUniquelyConnectedClient("c", "ns", nil, ir.PodLocality{})
	uccs := krt.NewStaticCollection(nil, []ir.UniquelyConnectedClient{client}, krtopts.ToOptions("UCCs")...)

	pcc := NewPerClientEnvoyClusters(ctx, krtopts, translator, finalBackends, uccs)
	require.Eventually(t, pcc.HasSynced, time.Second, 10*time.Millisecond)

	var got []uccWithCluster
	require.Eventually(t, func() bool {
		got = pcc.FetchClustersForClient(krt.TestingDummyContext{}, client)
		return len(got) > 0
	}, 2*time.Second, 20*time.Millisecond,
		"a renamed cluster must not withhold the client's entire CDS")

	require.Len(t, got, 1, "only the renamed backend should be dropped")
	assert.Equal(t, good.ClusterName(), got[0].Name)
}
