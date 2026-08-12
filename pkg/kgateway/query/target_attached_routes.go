package query

import (
	"maps"

	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/collections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

// TargetAttachedRoutes is the attached-route count for one status target (a Gateway
// or ListenerSet), broken out per listener name. It is derived independently of the
// target's own translation: a route attaching or detaching recomputes only the
// targets it references, rather than forcing a full gateway retranslation just to
// refresh AttachedRoutes.
type TargetAttachedRoutes struct {
	Target           reports.StatusKey
	CountsByListener map[string]uint
}

func (t TargetAttachedRoutes) ResourceName() string {
	return t.Target.String()
}

func (t TargetAttachedRoutes) Equals(other TargetAttachedRoutes) bool {
	return t.Target == other.Target && maps.Equal(t.CountsByListener, other.CountsByListener)
}

// NewTargetAttachedRoutes builds the attached-route counters for every Gateway and
// ListenerSet, keyed by reports.StatusKey so a status writer can look counts up the
// same way it looks up a StatusReport.
//
// Counting goes through RoutesIndex.RoutesFor, which is index-backed per target, so
// a route change recomputes only the target(s) it actually references rather than
// every Gateway that happens to share a translation pass with it.
//
// This applies uniformly to every Gateway in GatewayIndex.Gateways, regardless of
// which sdk.KGwTranslator ultimately renders its xDS (core or plugin-supplied, e.g.
// waypoint's): Listeners and their attached routes are standard Gateway API
// semantics, resolved the same way (GatewaysForEnvoyTransformationFunc,
// RoutesIndex.RoutesFor) no matter which plugin later consumes them.
func NewTargetAttachedRoutes(krtopts krtutil.KrtOptions, commoncol *collections.CommonCollections) krt.Collection[TargetAttachedRoutes] {
	r := &gatewayQueries{collections: commoncol}
	return krt.NewManyCollection(commoncol.GatewayIndex.Gateways, func(kctx krt.HandlerContext, gw ir.Gateway) []TargetAttachedRoutes {
		return r.attachedRoutesForGateway(kctx, &gw)
	}, krtopts.ToOptions("TargetAttachedRoutes")...)
}

// listenerTarget groups the listeners of one gw.Listeners entry by their actual
// owner (the Gateway itself, or one of its allowed ListenerSets).
type listenerTarget struct {
	key       reports.StatusKey
	parent    client.Object
	listeners []gwv1.Listener
}

// attachedRoutesForGateway computes one TargetAttachedRoutes per distinct listener
// parent referenced by gw.Listeners -- the Gateway itself, plus each ListenerSet it
// allows. A ListenerSet belongs to exactly one Gateway's AllowedListenerSets, so no
// two Gateways ever emit a record for the same target.
func (r *gatewayQueries) attachedRoutesForGateway(kctx krt.HandlerContext, gw *ir.Gateway) []TargetAttachedRoutes {
	order := make([]reports.StatusKey, 0, 1+len(gw.AllowedListenerSets))
	targets := map[reports.StatusKey]*listenerTarget{}
	for _, l := range gw.Listeners {
		key := statusKeyForParent(l.Parent)
		t, ok := targets[key]
		if !ok {
			t = &listenerTarget{key: key, parent: l.Parent}
			targets[key] = t
			order = append(order, key)
		}
		t.listeners = append(t.listeners, l.Listener)
	}

	out := make([]TargetAttachedRoutes, 0, len(order))
	for _, key := range order {
		out = append(out, r.attachedRoutesForTarget(kctx, targets[key]))
	}
	return out
}

func (r *gatewayQueries) attachedRoutesForTarget(kctx krt.HandlerContext, t *listenerTarget) TargetAttachedRoutes {
	counts := make(map[string]uint, len(t.listeners))
	for _, l := range t.listeners {
		counts[string(l.Name)] = 0
	}

	nns := types.NamespacedName{Namespace: t.parent.GetNamespace(), Name: t.parent.GetName()}
	routes := r.collections.Routes.RoutesFor(kctx, nns, t.key.Group, t.key.Kind)
	for _, route := range routes {
		for _, ref := range getParentRefsForResource(t.parent, route) {
			for _, l := range t.listeners {
				outcome, err := r.attachOutcomeForListener(kctx, t.parent, route, ref, &l)
				if err != nil || !outcome.attaches() {
					continue
				}
				counts[string(l.Name)]++
			}
		}
	}

	return TargetAttachedRoutes{Target: t.key, CountsByListener: counts}
}

// statusKeyForParent returns the reports.StatusKey identifying a listener's parent
// (a Gateway or ListenerSet), the same identity resourceGVK/isParentRefForResource
// use to match a route's parentRef against this resource.
func statusKeyForParent(parent client.Object) reports.StatusKey {
	return reports.StatusKey{
		GroupKind:      resourceGVK(parent).GroupKind(),
		NamespacedName: types.NamespacedName{Namespace: parent.GetNamespace(), Name: parent.GetName()},
	}
}
