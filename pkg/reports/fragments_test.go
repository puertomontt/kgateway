package reports_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	reportssdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/reporter"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

func routeReportWithParent(t *testing.T, routeName, gatewayName string, accepted metav1.ConditionStatus) *reports.RouteReport {
	t.Helper()
	route := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: "default", Generation: 1},
	}
	parentRef := gwv1.ParentReference{Name: gwv1.ObjectName(gatewayName)}

	rm := reports.NewReportMap()
	reports.NewReporter(&rm).Route(route).ParentRef(&parentRef).SetCondition(reportssdk.RouteCondition{
		Type:   gwv1.RouteConditionAccepted,
		Status: accepted,
		Reason: gwv1.RouteReasonAccepted,
	})
	report := rm.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: routeName}]
	require.NotNil(t, report)
	return report
}

func buildStatusFromReport(t *testing.T, routeName string, report *reports.RouteReport) *gwv1.RouteStatus {
	t.Helper()
	rm := reports.NewReportMap()
	rm.HTTPRoutes[types.NamespacedName{Namespace: "default", Name: routeName}] = report
	route := &gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: "default"}}
	return rm.BuildRouteStatus(t.Context(), route, "test-controller")
}

func TestMergeRouteReportsCombinesParentsAcrossGateways(t *testing.T) {
	reportA := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	reportB := routeReportWithParent(t, "route", "gw-b", metav1.ConditionTrue)

	merged := reports.MergeRouteReports(reportA, reportB)
	require.NotNil(t, merged)

	status := buildStatusFromReport(t, "route", merged)
	require.NotNil(t, status)
	require.Len(t, status.Parents, 2, "parents from both gateways' reports must be present")
}

func TestMergeRouteReportsDoesNotAliasInputs(t *testing.T) {
	reportA := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	reportB := routeReportWithParent(t, "route", "gw-b", metav1.ConditionTrue)

	merged := reports.MergeRouteReports(reportA, reportB)
	require.NotNil(t, merged)

	// Mutating the merged report must not leak back into the inputs.
	extraRef := gwv1.ParentReference{Name: gwv1.ObjectName("gw-c")}
	merged.ParentRef(&extraRef)

	statusA := buildStatusFromReport(t, "route", reportA)
	require.NotNil(t, statusA)
	require.Len(t, statusA.Parents, 1, "input report must be unaffected by mutations of the merged report")
}

func TestMergeRouteReportsSkipsNil(t *testing.T) {
	require.Nil(t, reports.MergeRouteReports(nil, nil))

	report := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	merged := reports.MergeRouteReports(nil, report, nil)
	require.NotNil(t, merged)
	require.True(t, reports.RouteReportsEqual(report, merged))
}

func TestRouteReportsEqual(t *testing.T) {
	reportA := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	reportSame := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	reportDiff := routeReportWithParent(t, "route", "gw-a", metav1.ConditionFalse)

	require.True(t, reports.RouteReportsEqual(reportA, reportSame))
	require.False(t, reports.RouteReportsEqual(reportA, reportDiff), "differing condition status must not compare equal")
	require.True(t, reports.RouteReportsEqual(nil, nil))
	require.False(t, reports.RouteReportsEqual(reportA, nil))
}

func TestCloneRouteReportIsDeep(t *testing.T) {
	report := routeReportWithParent(t, "route", "gw-a", metav1.ConditionTrue)
	clone := reports.CloneRouteReport(report)
	require.True(t, reports.RouteReportsEqual(report, clone))

	extraRef := gwv1.ParentReference{Name: gwv1.ObjectName("gw-b")}
	clone.ParentRef(&extraRef)
	require.False(t, reports.RouteReportsEqual(report, clone), "mutating the clone must not affect the original")

	statusOrig := buildStatusFromReport(t, "route", report)
	require.NotNil(t, statusOrig)
	require.Len(t, statusOrig.Parents, 1)
}
