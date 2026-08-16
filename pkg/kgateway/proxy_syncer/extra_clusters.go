package proxy_syncer

import (
	"fmt"
	"slices"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	plug "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	krtutil "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
)

// policyExtraClusters is the set of non-backend clusters contributed by a single
// policy, keyed by that policy so the collection has one entry per policy.
type policyExtraClusters struct {
	name     string
	clusters []*envoyclusterv3.Cluster
	hash     uint64
}

func (p policyExtraClusters) ResourceName() string { return p.name }

func (p policyExtraClusters) Equals(in policyExtraClusters) bool {
	return p.name == in.name && p.hash == in.hash
}

// extraClusterSet is the deduplicated union of every policy's extra clusters.
type extraClusterSet struct {
	clusters []*envoyclusterv3.Cluster
	hash     uint64
}

func (e extraClusterSet) ResourceName() string { return "extra-clusters" }

func (e extraClusterSet) Equals(in extraClusterSet) bool { return e.hash == in.hash }

// newPolicyExtraClusters collects the clusters that policies declare via
// ir.ExtraClustersIR, deduplicated by cluster name across every policy kind.
//
// These clusters are not backends, so backend translation never produces them,
// yet a policy's output can refer to one — an SDS transport cluster is the case
// this exists for. They are gathered once rather than per gateway: the set only
// changes when a policy adds or drops a reference, which is rare, and gathering
// per gateway would make every gateway translation walk every policy.
//
// The consequence is that a proxy may receive a cluster for an SDS server only
// reachable through a policy that does not apply to it. That is bounded by the
// number of distinct SDS servers in the cluster, and an unreferenced static
// cluster costs Envoy nothing beyond its own definition.
func newPolicyExtraClusters(
	krtopts krtutil.KrtOptions,
	plugins plug.Plugin,
) krt.Singleton[extraClusterSet] {
	perPolicy := make([]krt.Collection[policyExtraClusters], 0, len(plugins.ContributesPolicies))
	for gk, plugin := range plugins.ContributesPolicies {
		if plugin.Policies == nil {
			continue
		}
		kind := gk.String()
		perPolicy = append(perPolicy, krt.NewCollection(plugin.Policies, func(_ krt.HandlerContext, pol ir.PolicyWrapper) *policyExtraClusters {
			contributor, ok := pol.PolicyIR.(ir.ExtraClustersIR)
			if !ok {
				return nil
			}
			clusters := contributor.ExtraClusters()
			if len(clusters) == 0 {
				return nil
			}
			var hash uint64
			for _, c := range clusters {
				hash ^= utils.HashProto(c)
			}
			return &policyExtraClusters{
				// Namespaced by kind so two policy kinds sharing a name cannot
				// collide on a key, which krt treats as a fatal duplicate.
				name:     fmt.Sprintf("%s/%s", kind, pol.ResourceName()),
				clusters: clusters,
				hash:     hash,
			}
		}, krtopts.ToOptions("PolicyExtraClusters/"+kind)...))
	}

	joined := krt.JoinCollection(perPolicy, krtopts.ToOptions("PolicyExtraClustersJoined")...)

	return krt.NewSingleton(func(kctx krt.HandlerContext) *extraClusterSet {
		byName := map[string]*envoyclusterv3.Cluster{}
		for _, contribution := range krt.Fetch(kctx, joined) {
			for _, c := range contribution.clusters {
				// Policies pointing at the same SDS server produce an identical
				// cluster; first one wins, and duplicate names in a CDS response
				// would be rejected outright by Envoy.
				if _, seen := byName[c.GetName()]; !seen {
					byName[c.GetName()] = c
				}
			}
		}
		if len(byName) == 0 {
			return &extraClusterSet{}
		}

		// Sorted so an unchanged set always hashes and serializes identically.
		names := sets.List(sets.KeySet(byName))
		clusters := make([]*envoyclusterv3.Cluster, 0, len(names))
		var hash uint64
		for _, n := range names {
			clusters = append(clusters, byName[n])
			hash ^= utils.HashProto(byName[n])
		}
		return &extraClusterSet{clusters: clusters, hash: hash}
	}, krtopts.ToOptions("PolicyExtraClusterSet")...)
}

// appendPolicyExtraClusters adds the policy-contributed clusters to a gateway's
// translated output, skipping any the gateway already produced for itself.
func appendPolicyExtraClusters(existing []*envoyclusterv3.Cluster, extra *extraClusterSet) []*envoyclusterv3.Cluster {
	if extra == nil || len(extra.clusters) == 0 {
		return existing
	}
	present := sets.New[string]()
	for _, c := range existing {
		present.Insert(c.GetName())
	}
	out := slices.Clone(existing)
	for _, c := range extra.clusters {
		if !present.Has(c.GetName()) {
			out = append(out, c)
		}
	}
	return out
}
