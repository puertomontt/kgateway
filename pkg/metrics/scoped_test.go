package metrics_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/kgateway-dev/kgateway/v2/pkg/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics/metricstest"
)

func TestDeleteGatewaySeries(t *testing.T) {
	setupTestRegistry()
	ResetScopedMetrics()

	counter := NewCounter(CounterOpts{Name: "scoped_total", Help: "test"}, []string{"gw_ns", "gw", "result"})
	gauge := NewGauge(GaugeOpts{Name: "scoped_current", Help: "test"}, []string{"namespace", "name"})
	RegisterGatewayScoped(counter, "gw_ns", "gw")
	RegisterGatewayScoped(gauge, "namespace", "name")

	counter.Inc(Label{Name: "gw_ns", Value: "ns"}, Label{Name: "gw", Value: "a"}, Label{Name: "result", Value: "success"})
	counter.Inc(Label{Name: "gw_ns", Value: "ns"}, Label{Name: "gw", Value: "a"}, Label{Name: "result", Value: "error"})
	counter.Inc(Label{Name: "gw_ns", Value: "ns"}, Label{Name: "gw", Value: "b"}, Label{Name: "result", Value: "success"})
	counter.Inc(Label{Name: "gw_ns", Value: "other"}, Label{Name: "gw", Value: "a"}, Label{Name: "result", Value: "success"})
	gauge.Set(3, Label{Name: "namespace", Value: "ns"}, Label{Name: "name", Value: "a"})

	require.Equal(t, 3, DeleteGatewaySeries("ns", "a"), "every series of ns/a across both metrics")

	gathered := metricstest.MustGatherMetrics(t)
	gathered.AssertMetricsLabels("kgateway_scoped_total", [][]Label{
		{{Name: "gw_ns", Value: "other"}, {Name: "gw", Value: "a"}, {Name: "result", Value: "success"}},
		{{Name: "gw_ns", Value: "ns"}, {Name: "gw", Value: "b"}, {Name: "result", Value: "success"}},
	})
	gathered.AssertMetricNotExists("kgateway_scoped_current")

	require.Zero(t, DeleteGatewaySeries("ns", "a"), "deleting again is a no-op")
}

func TestDeleteNamespaceSeries(t *testing.T) {
	setupTestRegistry()
	ResetScopedMetrics()

	counter := NewCounter(CounterOpts{Name: "scoped_total", Help: "test"}, []string{"namespace", "name"})
	RegisterNamespaceScoped(counter, "namespace")

	counter.Inc(Label{Name: "namespace", Value: "ns"}, Label{Name: "name", Value: "a"})
	counter.Inc(Label{Name: "namespace", Value: "ns"}, Label{Name: "name", Value: "b"})
	counter.Inc(Label{Name: "namespace", Value: "kept"}, Label{Name: "name", Value: "a"})

	require.Equal(t, 2, DeleteNamespaceSeries("ns"))

	gathered := metricstest.MustGatherMetrics(t)
	gathered.AssertMetricsLabels("kgateway_scoped_total", [][]Label{
		{{Name: "namespace", Value: "kept"}, {Name: "name", Value: "a"}},
	})
}

func TestRegisterScopedRejectsUnknownLabel(t *testing.T) {
	setupTestRegistry()
	ResetScopedMetrics()

	counter := NewCounter(CounterOpts{Name: "scoped_total", Help: "test"}, []string{"namespace", "name"})

	require.PanicsWithValue(t, "metric does not have label gateway", func() {
		RegisterGatewayScoped(counter, "namespace", "gateway")
	})
	require.PanicsWithValue(t, "metric does not have label gw_ns", func() {
		RegisterNamespaceScoped(counter, "gw_ns")
	})
}
