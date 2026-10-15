package proxy_syncer

import (
	"strings"
	"time"

	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

const (
	snapshotSubsystem = "xds_snapshot"
	gatewayLabel      = "gateway"
	nameLabel         = "name"
	namespaceLabel    = "namespace"
	resultLabel       = "result"
	resourceLabel     = "resource"
)

var (
	transformsHistogramBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	snapshotTransformsTotal    = metrics.NewCounter(
		metrics.CounterOpts{
			Subsystem: snapshotSubsystem,
			Name:      "transforms_total",
			Help:      "Total number of XDS snapshot transforms",
		},
		[]string{gatewayLabel, namespaceLabel, resultLabel},
	)
	snapshotTransformDuration = metrics.NewHistogram(
		metrics.HistogramOpts{
			Subsystem:                       snapshotSubsystem,
			Name:                            "transform_duration_seconds",
			Help:                            "XDS snapshot transform duration",
			Buckets:                         transformsHistogramBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		},
		[]string{gatewayLabel, namespaceLabel},
	)
	snapshotResources = metrics.NewGauge(
		metrics.GaugeOpts{
			Subsystem: snapshotSubsystem,
			Name:      "resources",
			Help:      "Current number of resources in XDS snapshot",
		},
		[]string{gatewayLabel, namespaceLabel, resourceLabel},
	)
	// snapshotDeferredClients counts connected clients without a current snapshot
	// row, including clients that have never received a snapshot. Brief increases
	// occur during convergence; a sustained count indicates deferred configuration.
	snapshotDeferredClients = metrics.NewGauge(
		metrics.GaugeOpts{
			Subsystem: snapshotSubsystem,
			Name:      "deferred_clients",
			Help:      "Connected xDS clients whose snapshot is currently withheld because per-client inputs are not ready",
		},
		[]string{gatewayLabel, namespaceLabel},
	)
)

func init() {
	// The per-client series are deleted by snapshotDeferralTracker when a gateway's
	// last client disconnects; the namespace cleanup catches a transform that ran
	// after that.
	metrics.RegisterNamespaceScoped(snapshotTransformsTotal, namespaceLabel)
	metrics.RegisterNamespaceScoped(snapshotTransformDuration, namespaceLabel)
}

// gatewayMetricsCleanup returns a handler for translation output events that deletes a
// Gateway's gateway-scoped metric series once it is gone. The series are recorded while
// the Gateway is translated, and KRT delivers the output's delete event only after the
// last translation has finished, so nothing re-creates them afterwards. A delete while the
// Gateway is still present means translation returned nil and will run again; its series
// are kept.
func gatewayMetricsCleanup(gateways krt.Collection[ir.Gateway]) func(krt.Event[gatewayTranslationOutput]) {
	return func(e krt.Event[gatewayTranslationOutput]) {
		if e.Event != controllers.EventDelete {
			return
		}
		nn := e.Latest().Xds.NamespacedName
		key := ir.ObjectSource{
			Group:     gwv1.GroupVersion.Group,
			Kind:      wellknown.GatewayKind,
			Namespace: nn.Namespace,
			Name:      nn.Name,
		}.ResourceName()
		if gateways.GetKey(key) != nil {
			return
		}
		metrics.DeleteGatewaySeries(nn.Namespace, nn.Name)
	}
}

// gatewayDeleteMetricsCleanup returns a handler for Gateway events that deletes a deleted
// Gateway's gateway-scoped series when it has no translation output. Its last translation
// then returned nil, so gatewayMetricsCleanup kept its series and gets no further event for
// it. When an output is still present, its delete event triggers gatewayMetricsCleanup.
func gatewayDeleteMetricsCleanup(outputs krt.Collection[gatewayTranslationOutput]) func(krt.Event[ir.Gateway]) {
	return func(e krt.Event[ir.Gateway]) {
		if e.Event != controllers.EventDelete {
			return
		}
		gw := e.Latest()
		nn := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		if outputs.GetKey(GatewayXdsResources{NamespacedName: nn}.ResourceName()) != nil {
			return
		}
		metrics.DeleteGatewaySeries(nn.Namespace, nn.Name)
	}
}

// snapshotResourcesMetricLabels defines the labels for XDS snapshot resources metrics.
type snapshotResourcesMetricLabels struct {
	Gateway   string
	Namespace string
	Resource  string
}

func (r snapshotResourcesMetricLabels) toMetricsLabels() []metrics.Label {
	return []metrics.Label{
		{Name: gatewayLabel, Value: r.Gateway},
		{Name: namespaceLabel, Value: r.Namespace},
		{Name: resourceLabel, Value: r.Resource},
	}
}

// collectXDSTransformMetrics is called at the start of a transform function to
// begin metrics collection and returns a function called at the end to complete
// metrics recording.
func collectXDSTransformMetrics(clientKey string) func(error) {
	if !metrics.Active() {
		return func(err error) {}
	}

	start := time.Now()

	cd := getDetailsFromXDSClientResourceName(clientKey)
	return func(err error) {
		result := "success"
		if err != nil {
			result = "error"
		}

		snapshotTransformsTotal.Inc(
			metrics.Label{Name: gatewayLabel, Value: cd.Gateway},
			metrics.Label{Name: namespaceLabel, Value: cd.Namespace},
			metrics.Label{Name: resultLabel, Value: result},
		)

		duration := time.Since(start)

		snapshotTransformDuration.Observe(duration.Seconds(),
			metrics.Label{Name: gatewayLabel, Value: cd.Gateway},
			metrics.Label{Name: namespaceLabel, Value: cd.Namespace},
		)
	}
}

type resourceNameDetails struct {
	Role      string
	Namespace string
	Gateway   string
}

// getDetailsFromXDSClientResourceName extracts details from an XDS client resource name.
func getDetailsFromXDSClientResourceName(resourceName string) resourceNameDetails {
	res := resourceNameDetails{
		Role:      "unknown",
		Namespace: "unknown",
		Gateway:   "unknown",
	}

	pks := strings.SplitN(resourceName, "~", 5)

	if len(pks) > 0 {
		res.Role = pks[0]
	}

	if len(pks) > 1 {
		res.Namespace = pks[1]
	}

	if len(pks) > 2 {
		res.Gateway = pks[2]
	}

	return res
}

// ResetMetrics resets the metrics from this package.
// This is provided for testing purposes only.
func ResetMetrics() {
	snapshotTransformsTotal.Reset()
	snapshotTransformDuration.Reset()
	snapshotResources.Reset()
	snapshotDeferredClients.Reset()
}
