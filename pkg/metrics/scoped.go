package metrics

import (
	"slices"
	"sync"
)

// PartialDeleter is a metric whose series can be removed by partial label match.
// Counter, Histogram, and Gauge all satisfy it.
type PartialDeleter interface {
	Metric
	DeletePartialMatch(...Label) int
}

type gatewayScopedMetric struct {
	m              PartialDeleter
	namespaceLabel string
	nameLabel      string
}

type namespaceScopedMetric struct {
	m              PartialDeleter
	namespaceLabel string
}

var (
	scopedLock      sync.RWMutex
	gatewayScoped   []gatewayScopedMetric
	namespaceScoped []namespaceScopedMetric
)

// RegisterGatewayScoped marks m as carrying one series set per Gateway, identified by
// namespaceLabel and nameLabel, so DeleteGatewaySeries removes them when the Gateway goes
// away. Only register metrics that nothing records for a Gateway after it has left
// translation: a gauge maintained with Add/Sub here would reappear as a negative series.
// It panics if m does not have both labels, which is a programming error caught at init.
func RegisterGatewayScoped(m PartialDeleter, namespaceLabel, nameLabel string) {
	mustHaveLabels(m, namespaceLabel, nameLabel)

	scopedLock.Lock()
	defer scopedLock.Unlock()

	gatewayScoped = append(gatewayScoped, gatewayScopedMetric{m: m, namespaceLabel: namespaceLabel, nameLabel: nameLabel})
}

// RegisterNamespaceScoped marks m as carrying series identified by namespaceLabel, so
// DeleteNamespaceSeries removes them when the namespace is deleted. Kubernetes removes a
// Namespace only after everything in it is gone, so this is safe for any metric whose
// label names the namespace of the object it describes.
// It panics if m does not have namespaceLabel.
func RegisterNamespaceScoped(m PartialDeleter, namespaceLabel string) {
	mustHaveLabels(m, namespaceLabel)

	scopedLock.Lock()
	defer scopedLock.Unlock()

	namespaceScoped = append(namespaceScoped, namespaceScopedMetric{m: m, namespaceLabel: namespaceLabel})
}

// DeleteGatewaySeries removes every series of the gateway-scoped metrics that belongs to
// the named Gateway, returning the number of series removed.
func DeleteGatewaySeries(namespace, name string) int {
	scopedLock.RLock()
	defer scopedLock.RUnlock()

	deleted := 0
	for _, s := range gatewayScoped {
		deleted += s.m.DeletePartialMatch(
			Label{Name: s.namespaceLabel, Value: namespace},
			Label{Name: s.nameLabel, Value: name},
		)
	}
	return deleted
}

// DeleteNamespaceSeries removes every series of the namespace-scoped metrics that belongs
// to the namespace, returning the number of series removed.
func DeleteNamespaceSeries(namespace string) int {
	scopedLock.RLock()
	defer scopedLock.RUnlock()

	deleted := 0
	for _, s := range namespaceScoped {
		deleted += s.m.DeletePartialMatch(Label{Name: s.namespaceLabel, Value: namespace})
	}
	return deleted
}

func mustHaveLabels(m Metric, labels ...string) {
	for _, l := range labels {
		if !slices.Contains(m.Labels(), l) {
			panic("metric does not have label " + l)
		}
	}
}
