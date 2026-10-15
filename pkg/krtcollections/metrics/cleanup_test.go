package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	. "github.com/kgateway-dev/kgateway/v2/pkg/krtcollections/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics/metricstest"
)

// hasSeries reports whether the named metric has a series with exactly these label values.
func hasSeries(t *testing.T, name string, labels ...metrics.Label) bool {
	t.Helper()
	families, err := metrics.Registry().Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			match := true
			for _, l := range labels {
				if got[l.Name] != l.Value {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func route(name, parent string) *gwv1.HTTPRoute {
	return &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: gwv1.HTTPRouteSpec{CommonRouteSpec: gwv1.CommonRouteSpec{
			ParentRefs: []gwv1.ParentReference{{Name: gwv1.ObjectName(parent)}},
		}},
	}
}

func TestResourcesManagedDeletedWhenParentHasNoResources(t *testing.T) {
	setupTest()
	handle := GetResourceMetricEventHandler[*gwv1.HTTPRoute]()
	a, b := route("a", "gw"), route("b", "gw")
	labels := []metrics.Label{
		{Name: "namespace", Value: "ns"},
		{Name: "parent", Value: "gw"},
		{Name: "resource", Value: "HTTPRoute"},
	}

	handle(krt.Event[*gwv1.HTTPRoute]{New: &a, Event: controllers.EventAdd})
	handle(krt.Event[*gwv1.HTTPRoute]{New: &b, Event: controllers.EventAdd})
	metricstest.MustGatherMetrics(t).AssertMetric("kgateway_resources_managed",
		&metricstest.ExpectedMetric{Labels: labels, Value: 2})

	handle(krt.Event[*gwv1.HTTPRoute]{Old: &a, Event: controllers.EventDelete})
	metricstest.MustGatherMetrics(t).AssertMetric("kgateway_resources_managed",
		&metricstest.ExpectedMetric{Labels: labels, Value: 1})

	handle(krt.Event[*gwv1.HTTPRoute]{Old: &b, Event: controllers.EventDelete})
	require.False(t, hasSeries(t, "kgateway_resources_managed", labels...),
		"a parent with no resources left must not keep a zero series")
}

func TestGatewayDeletePurgesSyncMetrics(t *testing.T) {
	setupTest()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartResourceSyncMetricsProcessing(ctx)

	handle := GetResourceMetricEventHandler[*gwv1.Gateway]()
	gw := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "ns"}}
	details := ResourceSyncDetails{Gateway: "gw", Namespace: "ns", ResourceType: "Gateway", ResourceName: "gw"}
	labels := []metrics.Label{{Name: "gateway", Value: "gw"}, {Name: "namespace", Value: "ns"}}

	handle(krt.Event[*gwv1.Gateway]{New: &gw, Event: controllers.EventAdd})
	EndResourceStatusSync(details)
	EndResourceXDSSync(details)
	require.Eventually(t, func() bool {
		return hasSeries(t, "kgateway_resources_status_syncs_completed_total", labels...) &&
			hasSeries(t, "kgateway_xds_snapshot_syncs_total", labels...)
	}, 2*time.Second, 10*time.Millisecond)

	// The delete starts an XDS sync that will never end, then purges.
	handle(krt.Event[*gwv1.Gateway]{Old: &gw, Event: controllers.EventDelete})
	require.Eventually(t, func() bool {
		for _, name := range []string{
			"kgateway_resources_status_syncs_started_total",
			"kgateway_resources_status_syncs_completed_total",
			"kgateway_resources_status_sync_duration_seconds",
			"kgateway_xds_snapshot_syncs_total",
			"kgateway_xds_snapshot_sync_duration_seconds",
		} {
			if hasSeries(t, name, labels...) {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "a deleted Gateway's sync series must be purged")

	// Nothing pending remains for it: a late XDS sync end records nothing.
	EndResourceXDSSync(details)
	time.Sleep(50 * time.Millisecond)
	require.False(t, hasSeries(t, "kgateway_xds_snapshot_syncs_total", labels...),
		"the purge must drop the pending XDS sync the delete started")
}

func TestNamespaceDeleteRemovesSyncMetrics(t *testing.T) {
	setupTest()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartResourceSyncMetricsProcessing(ctx)

	details := ResourceSyncDetails{Gateway: "gw", Namespace: "ns", ResourceType: "HTTPRoute", ResourceName: "r"}
	labels := []metrics.Label{{Name: "gateway", Value: "gw"}, {Name: "namespace", Value: "ns"}}
	StartResourceStatusSync(details)
	EndResourceStatusSync(details)
	require.Eventually(t, func() bool {
		return hasSeries(t, "kgateway_resources_status_syncs_completed_total", labels...)
	}, 2*time.Second, 10*time.Millisecond)

	metrics.DeleteNamespaceSeries("ns")
	require.False(t, hasSeries(t, "kgateway_resources_status_syncs_started_total", labels...))
	require.False(t, hasSeries(t, "kgateway_resources_status_syncs_completed_total", labels...))
	require.False(t, hasSeries(t, "kgateway_resources_status_sync_duration_seconds", labels...))
}

func TestResourcesManagedUpdateKeepsSeries(t *testing.T) {
	setupTest()
	handle := GetResourceMetricEventHandler[*gwv1.HTTPRoute]()
	a := route("a", "gw")
	moved := route("a", "gw2")
	labels := func(parent string) []metrics.Label {
		return []metrics.Label{
			{Name: "namespace", Value: "ns"},
			{Name: "parent", Value: parent},
			{Name: "resource", Value: "HTTPRoute"},
		}
	}

	handle(krt.Event[*gwv1.HTTPRoute]{New: &a, Event: controllers.EventAdd})
	// An update that keeps the parent leaves the count at 1.
	handle(krt.Event[*gwv1.HTTPRoute]{Old: &a, New: &a, Event: controllers.EventUpdate})
	metricstest.MustGatherMetrics(t).AssertMetric("kgateway_resources_managed",
		&metricstest.ExpectedMetric{Labels: labels("gw"), Value: 1})

	// Moving the route to another parent deletes the old parent's series.
	handle(krt.Event[*gwv1.HTTPRoute]{Old: &a, New: &moved, Event: controllers.EventUpdate})
	require.False(t, hasSeries(t, "kgateway_resources_managed", labels("gw")...))
	metricstest.MustGatherMetrics(t).AssertMetric("kgateway_resources_managed",
		&metricstest.ExpectedMetric{Labels: labels("gw2"), Value: 1})
}
