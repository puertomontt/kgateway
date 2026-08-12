package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/krt/krttest"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/util/smallset"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	gwv1b1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	apisettings "github.com/kgateway-dev/kgateway/v2/api/settings"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/query"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/krtcollections"
	sdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/collections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

// newTargetAttachedRoutesCollection wires up a full CommonCollections (Routes +
// GatewayIndex) against the given fake objects, matching the shape production
// wiring builds, and returns the resulting TargetAttachedRoutes collection once
// synced.
func newTargetAttachedRoutesCollection(t test.Failer, initObjs ...client.Object) krt.Collection[query.TargetAttachedRoutes] {
	var anys []any
	for _, obj := range initObjs {
		anys = append(anys, obj)
	}
	mock := krttest.NewMock(t, anys)

	refgrants := krtcollections.NewRefGrantIndex(krttest.GetMockCollection[*gwv1b1.ReferenceGrant](mock), apisettings.ReferenceGrantPermissive)
	policies := krtcollections.NewPolicyIndex(krtutil.KrtOptions{}, sdk.ContributesPolicies{}, apisettings.Settings{})
	backends := krtcollections.NewBackendIndex(krtutil.KrtOptions{}, policies, refgrants)

	httproutes := krttest.GetMockCollection[*gwv1.HTTPRoute](mock)
	grpcroutes := krttest.GetMockCollection[*gwv1.GRPCRoute](mock)
	tcproutes := krttest.GetMockCollection[*gwv1a2.TCPRoute](mock)
	tlsroutes := krttest.GetMockCollection[*gwv1a2.TLSRoute](mock)
	rtidx := krtcollections.NewRoutesIndex(krtutil.KrtOptions{}, httproutes, grpcroutes, tcproutes, tlsroutes, policies, backends, refgrants, apisettings.Settings{})

	gateways := krttest.GetMockCollection[*gwv1.Gateway](mock)
	listenerSets := krttest.GetMockCollection[*gwv1.ListenerSet](mock)
	gatewayClasses := krttest.GetMockCollection[*gwv1.GatewayClass](mock)
	nsCol := krtcollections.NewNamespaceCollectionFromCol(context.Background(), krttest.GetMockCollection[*corev1.Namespace](mock), krtutil.KrtOptions{})

	gwIdx := krtcollections.NewGatewayIndex(krtcollections.GatewayIndexConfig{
		ControllerNames:     smallset.New(wellknown.DefaultGatewayControllerName),
		EnvoyControllerName: wellknown.DefaultGatewayControllerName,
		PolicyIndex:         policies,
		Gateways:            gateways,
		ListenerSets:        listenerSets,
		GatewayClasses:      gatewayClasses,
		Namespaces:          nsCol,
	})

	commoncol := &collections.CommonCollections{
		Routes:       rtidx,
		GatewayIndex: gwIdx,
	}

	for !rtidx.HasSynced() || !gwIdx.Gateways.HasSynced() || !nsCol.HasSynced() {
		time.Sleep(time.Second / 10)
	}

	col := query.NewTargetAttachedRoutes(krtutil.KrtOptions{}, commoncol)
	col.WaitUntilSynced(nil)
	return col
}

func gatewayClass() *gwv1.GatewayClass {
	return &gwv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-class"},
		Spec:       gwv1.GatewayClassSpec{ControllerName: gwv1.GatewayController(wellknown.DefaultGatewayControllerName)},
	}
}

func targetAttachedRoutesFor(col krt.Collection[query.TargetAttachedRoutes], gk schema.GroupKind, nn types.NamespacedName) *query.TargetAttachedRoutes {
	key := reports.StatusKey{GroupKind: gk, NamespacedName: nn}
	return col.GetKey(key.String())
}

func TestNewTargetAttachedRoutesCountsRoutesAttachedToGatewayListener(t *testing.T) {
	testGw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "test-class",
			Listeners: []gwv1.Listener{
				{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
				{Name: "empty", Protocol: gwv1.HTTPProtocolType, Port: 81},
			},
		},
	}
	testGw.SetGroupVersionKind(wellknown.GatewayGVK)

	attached := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "attached"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{{Name: "gw", SectionName: new(gwv1.SectionName("http"))}},
			},
		},
	}
	other := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "other"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{{Name: "some-other-gateway"}},
			},
		},
	}

	col := newTargetAttachedRoutesCollection(t, testGw, gatewayClass(), attached, other)

	tar := targetAttachedRoutesFor(col, wellknown.GatewayGVK.GroupKind(), types.NamespacedName{Namespace: "default", Name: "gw"})
	require.NotNil(t, tar)
	require.Equal(t, map[string]uint{"http": 1, "empty": 0}, tar.CountsByListener)
}

func TestNewTargetAttachedRoutesCountsAgainstListenerSetSeparatelyFromGateway(t *testing.T) {
	testGw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "test-class",
			Listeners:        []gwv1.Listener{{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80}},
			AllowedListeners: &gwv1.AllowedListeners{
				Namespaces: &gwv1.ListenerNamespaces{From: new(gwv1.FromNamespaces(gwv1.NamespacesFromAll))},
			},
		},
	}
	testGw.SetGroupVersionKind(wellknown.GatewayGVK)

	ls := &gwv1.ListenerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ls"},
		Spec: gwv1.ListenerSetSpec{
			ParentRef: gwv1.ParentGatewayReference{Name: gwv1.ObjectName("gw")},
			Listeners: []gwv1.ListenerEntry{{Name: "ls-http", Protocol: gwv1.HTTPProtocolType, Port: 8080}},
		},
	}

	// attaches to the Gateway's own listener
	gwRoute := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw-route"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{{Name: "gw"}},
			},
		},
	}
	// attaches to the ListenerSet's listener, not the Gateway's
	lsRoute := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ls-route"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{{
					Group: new(gwv1.Group(wellknown.GatewayGVK.Group)),
					Kind:  new(gwv1.Kind(wellknown.ListenerSetGVK.Kind)),
					Name:  "ls",
				}},
			},
		},
	}

	col := newTargetAttachedRoutesCollection(t, testGw, ls, gatewayClass(), gwRoute, lsRoute)

	gwTar := targetAttachedRoutesFor(col, wellknown.GatewayGVK.GroupKind(), types.NamespacedName{Namespace: "default", Name: "gw"})
	require.NotNil(t, gwTar)
	require.Equal(t, map[string]uint{"http": 1}, gwTar.CountsByListener)

	lsTar := targetAttachedRoutesFor(col, wellknown.ListenerSetGVK.GroupKind(), types.NamespacedName{Namespace: "default", Name: "ls"})
	require.NotNil(t, lsTar)
	require.Equal(t, map[string]uint{"ls-http": 1}, lsTar.CountsByListener)
}

// TestNewTargetAttachedRoutesCountsCustomProtocolListeners pins the waypoint fix:
// counting must not depend on the listener's protocol, or on whether a plugin (e.g.
// waypoint) rather than the core translator ultimately renders this Gateway's xDS --
// AttachedRoutes is standard Gateway API status, resolved the same way regardless.
// Before this fix, Gateways handled by a plugin-supplied sdk.KGwTranslator never
// went through the (now-deleted) setAttachedRoutes at all, so their AttachedRoutes
// silently stayed at zero however many routes actually attached.
func TestNewTargetAttachedRoutesCountsCustomProtocolListeners(t *testing.T) {
	testGw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw"},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "test-class",
			Listeners:        []gwv1.Listener{{Name: "proxy", Protocol: "istio.io/PROXY", Port: 15088}},
		},
	}
	testGw.SetGroupVersionKind(wellknown.GatewayGVK)

	attached := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "attached"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{{Name: "gw"}},
			},
		},
	}

	col := newTargetAttachedRoutesCollection(t, testGw, gatewayClass(), attached)

	tar := targetAttachedRoutesFor(col, wellknown.GatewayGVK.GroupKind(), types.NamespacedName{Namespace: "default", Name: "gw"})
	require.NotNil(t, tar)
	require.Equal(t, map[string]uint{"proxy": 1}, tar.CountsByListener)
}
