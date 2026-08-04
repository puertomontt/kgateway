package proxy_syncer

import (
	"sync"
	"testing"
	"time"

	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

type countedRouteStatus struct {
	types.NamespacedName
	conditionReason string
	parentCount     int
}

func (c countedRouteStatus) ResourceName() string {
	return c.NamespacedName.String()
}

func (c countedRouteStatus) Equals(other countedRouteStatus) bool {
	return c == other
}

func TestStatusReportFragmentsMergeAndInvalidateOnlyAffectedRoute(t *testing.T) {
	ctx := t.Context()
	krtopts := krtutil.NewKrtOptions(ctx.Done(), nil)

	gwA := types.NamespacedName{Namespace: "default", Name: "gateway-a"}
	gwB := types.NamespacedName{Namespace: "default", Name: "gateway-b"}
	routeA := types.NamespacedName{Namespace: "default", Name: "route-a"}
	routeB := types.NamespacedName{Namespace: "default", Name: "route-b"}

	snapshotA := gatewaySnapshotWithRouteReport(gwA, routeA, "initial-a")
	snapshotB := gatewaySnapshotWithRouteReport(gwB, routeB, "initial-b")
	snapshotB.reports.HTTPRoutes[routeA] = gatewaySnapshotWithRouteReport(gwB, routeA, "initial-b").reports.HTTPRoutes[routeA]
	snapshots := krt.NewStaticCollection[GatewayXdsResources](nil, []GatewayXdsResources{snapshotA, snapshotB}, krtopts.ToOptions("TestSnapshots")...)
	fragments, fragmentIndex := newStatusReportFragments(snapshots, krtopts)

	routes := krt.NewStaticCollection[*gwv1.HTTPRoute](nil, []*gwv1.HTTPRoute{
		{ObjectMeta: metav1.ObjectMeta{Namespace: routeA.Namespace, Name: routeA.Name}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: routeB.Namespace, Name: routeB.Name}},
	}, krtopts.ToOptions("TestRoutes")...)

	var mu sync.Mutex
	recomputes := map[string]int{}
	statuses := krt.NewCollection(routes, func(kctx krt.HandlerContext, route *gwv1.HTTPRoute) *countedRouteStatus {
		mu.Lock()
		recomputes[route.Name]++
		mu.Unlock()

		nn := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
		rm := fetchStatusReport(kctx, fragments, fragmentIndex, statusReportKey{
			GroupKind:      wellknown.HTTPRouteGVK.GroupKind(),
			NamespacedName: nn,
		})
		if rm == nil {
			return nil
		}
		report := rm.HTTPRoutes[nn]
		for _, parent := range report.Parents {
			if len(parent.Conditions) > 0 {
				return &countedRouteStatus{
					NamespacedName:  nn,
					conditionReason: parent.Conditions[0].Reason,
					parentCount:     len(report.Parents),
				}
			}
		}
		return &countedRouteStatus{NamespacedName: nn, parentCount: len(report.Parents)}
	}, krtopts.ToOptions("TestStatuses")...)

	if !statuses.WaitUntilSynced(ctx.Done()) {
		t.Fatal("status collection did not sync")
	}
	requireRecomputes(t, &mu, recomputes, routeA.Name, 1)
	requireRecomputes(t, &mu, recomputes, routeB.Name, 1)
	statusA := krt.FetchOne(krt.TestingDummyContext{}, statuses, krt.FilterObjectName(routeA))
	if statusA == nil || statusA.parentCount != 2 {
		t.Fatalf("route A status has %v parents, want merged reports from both Gateways", statusA)
	}

	updatedA := gatewaySnapshotWithRouteReport(gwA, routeA, "updated-a")
	snapshots.UpdateObject(updatedA)

	eventually(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return recomputes[routeA.Name] == 2
	})
	// Give any accidental broad invalidation enough time to arrive.
	time.Sleep(100 * time.Millisecond)
	requireRecomputes(t, &mu, recomputes, routeB.Name, 1)
}

func gatewaySnapshotWithRouteReport(
	gateway types.NamespacedName,
	route types.NamespacedName,
	reason string,
) GatewayXdsResources {
	rm := reports.NewReportMap()
	rm.HTTPRoutes[route] = &reports.RouteReport{
		Parents: map[reports.ParentRefKey]*reports.ParentRefReport{
			{NamespacedName: gateway}: {
				Conditions: []metav1.Condition{{
					Type:   string(gwv1.RouteConditionAccepted),
					Status: metav1.ConditionTrue,
					Reason: reason,
				}},
			},
		},
	}
	return GatewayXdsResources{NamespacedName: gateway, reports: rm}
}

func requireRecomputes(t *testing.T, mu *sync.Mutex, recomputes map[string]int, route string, want int) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if got := recomputes[route]; got != want {
		t.Fatalf("route %s recomputed %d times, want %d", route, got, want)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
