package proxy_syncer

import (
	"context"
	"testing"

	envoylistenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	envoycachetypes "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	envoycache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	tmetrics "github.com/kgateway-dev/kgateway/v2/pkg/kgateway/translator/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/xds"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
)

// seriesCount returns how many series of the named metric carry all of labels.
func seriesCount(t *testing.T, name string, labels ...metrics.Label) int {
	t.Helper()
	families, err := metrics.Registry().Gather()
	require.NoError(t, err)
	n := 0
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
				n++
			}
		}
	}
	return n
}

var clientSnapshotMetrics = []string{
	"kgateway_xds_snapshot_transforms_total",
	"kgateway_xds_snapshot_transform_duration_seconds",
	"kgateway_xds_snapshot_resources",
	"kgateway_xds_snapshot_deferred_clients",
}

func clientSeries(t *testing.T, gateway, namespace string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, name := range clientSnapshotMetrics {
		if n := seriesCount(t, name,
			metrics.Label{Name: gatewayLabel, Value: gateway},
			metrics.Label{Name: namespaceLabel, Value: namespace},
		); n > 0 {
			out[name] = n
		}
	}
	return out
}

func snapWithListener() *envoycache.Snapshot {
	s := &envoycache.Snapshot{}
	s.Resources[envoycachetypes.Listener] = sliceToResources([]*envoylistenerv3.Listener{{Name: "l"}})
	return s
}

func TestClientSnapshotMetricsDeletedWithLastClient(t *testing.T) {
	setupTest()
	role := xds.OwnerNamespaceNameID(wellknown.GatewayApiProxyValue, "ns", "gw")
	a := ir.NewUniquelyConnectedClient(role, "ns", map[string]string{"pod": "a"}, ir.PodLocality{})
	b := ir.NewUniquelyConnectedClient(role, "ns", map[string]string{"pod": "b"}, ir.PodLocality{})
	snapA := XdsSnapWrapper{proxyKey: a.ResourceName(), snap: snapWithListener()}
	snapB := XdsSnapWrapper{proxyKey: b.ResourceName(), snap: snapWithListener()}

	d := newSnapshotDeferralTracker()
	d.clientEvents([]krt.Event[ir.UniquelyConnectedClient]{
		{New: &a, Event: controllers.EventAdd},
		{New: &b, Event: controllers.EventAdd},
	})
	collectXDSTransformMetrics(a.ResourceName())(nil)
	d.snapshotEvents([]krt.Event[XdsSnapWrapper]{
		{New: &snapA, Event: controllers.EventAdd},
		{New: &snapB, Event: controllers.EventAdd},
	})
	require.Equal(t, float64(1), metricsValue(t, "kgateway_xds_snapshot_resources", "gw", "ns", "Listener"))
	require.Len(t, clientSeries(t, "gw", "ns"), 4, "every per-client family has a series for gw")

	// One replica leaving keeps the gateway's series.
	d.clientEvents([]krt.Event[ir.UniquelyConnectedClient]{{Old: &a, Event: controllers.EventDelete}})
	d.snapshotEvents([]krt.Event[XdsSnapWrapper]{{Old: &snapA, Event: controllers.EventDelete}})
	require.Len(t, clientSeries(t, "gw", "ns"), 4)

	// The last one leaving deletes them all.
	d.clientEvents([]krt.Event[ir.UniquelyConnectedClient]{{Old: &b, Event: controllers.EventDelete}})
	require.Empty(t, clientSeries(t, "gw", "ns"), "no per-client series may outlive the gateway's last client")

	// A snapshot update processed after the disconnect re-creates the gauge, and the
	// row's delete that must follow it removes it again.
	d.snapshotEvents([]krt.Event[XdsSnapWrapper]{{Old: &snapB, New: &snapB, Event: controllers.EventUpdate}})
	d.snapshotEvents([]krt.Event[XdsSnapWrapper]{{Old: &snapB, Event: controllers.EventDelete}})
	require.Empty(t, clientSeries(t, "gw", "ns"), "a stale snapshot event must not leave a series behind")
}

func metricsValue(t *testing.T, name, gateway, namespace, resource string) float64 {
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
			if got[gatewayLabel] == gateway && got[namespaceLabel] == namespace && got[resourceLabel] == resource {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no %s series for %s/%s %s", name, namespace, gateway, resource)
	return 0
}

func TestGatewayMetricsCleanupSkipsLiveGateways(t *testing.T) {
	setupTest()
	tmetrics.ResetMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	krtopts := krtutil.NewKrtOptions(ctx.Done(), nil)

	live := ir.Gateway{
		ObjectSource: ir.ObjectSource{Group: gwv1.GroupVersion.Group, Kind: wellknown.GatewayKind, Namespace: "ns", Name: "live"},
		Obj:          &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "live"}},
	}
	gateways := krt.NewStaticCollection[ir.Gateway](nil, []ir.Gateway{live}, krtopts.ToOptions("Gateways")...)
	cleanup := gatewayMetricsCleanup(gateways)

	record := func(name string) {
		tmetrics.CollectTranslationMetrics(tmetrics.TranslatorMetricLabels{Name: name, Namespace: "ns", Translator: "TranslateGateway"})(nil)
	}
	output := func(name string) *gatewayTranslationOutput {
		return &gatewayTranslationOutput{Xds: GatewayXdsResources{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}}}
	}
	count := func(name string) int {
		return seriesCount(t, "kgateway_translator_translations_total",
			metrics.Label{Name: "namespace", Value: "ns"}, metrics.Label{Name: "name", Value: name})
	}
	record("live")
	record("gone")

	cleanup(krt.Event[gatewayTranslationOutput]{Old: output("live"), Event: controllers.EventDelete})
	cleanup(krt.Event[gatewayTranslationOutput]{Old: output("gone"), Event: controllers.EventDelete})

	require.Equal(t, 1, count("live"), "a Gateway whose translation returned nil keeps its series")
	require.Zero(t, count("gone"), "a deleted Gateway's series are removed")
}

func TestGatewayDeleteMetricsCleanupWithoutOutput(t *testing.T) {
	setupTest()
	tmetrics.ResetMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	krtopts := krtutil.NewKrtOptions(ctx.Done(), nil)

	translated := gatewayTranslationOutput{Xds: GatewayXdsResources{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "translated"}}}
	outputs := krt.NewStaticCollection[gatewayTranslationOutput](nil, []gatewayTranslationOutput{translated}, krtopts.ToOptions("Outputs")...)
	cleanup := gatewayDeleteMetricsCleanup(outputs)

	gateway := func(name string) *ir.Gateway {
		return &ir.Gateway{
			ObjectSource: ir.ObjectSource{Group: gwv1.GroupVersion.Group, Kind: wellknown.GatewayKind, Namespace: "ns", Name: name},
			Obj:          &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}},
		}
	}
	count := func(name string) int {
		return seriesCount(t, "kgateway_translator_translations_total",
			metrics.Label{Name: "namespace", Value: "ns"}, metrics.Label{Name: "name", Value: name})
	}
	for _, name := range []string{"translated", "untranslated"} {
		tmetrics.CollectTranslationMetrics(tmetrics.TranslatorMetricLabels{Name: name, Namespace: "ns", Translator: "TranslateGateway"})(nil)
	}

	cleanup(krt.Event[ir.Gateway]{Old: gateway("translated"), Event: controllers.EventDelete})
	cleanup(krt.Event[ir.Gateway]{Old: gateway("untranslated"), Event: controllers.EventDelete})

	require.Equal(t, 1, count("translated"), "a Gateway with an output is cleaned up by that output's delete event")
	require.Zero(t, count("untranslated"), "a deleted Gateway whose last translation returned nil loses its series")
}
