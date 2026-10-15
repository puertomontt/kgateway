package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/config/schema/gvr"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/test"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/apiclient/fake"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
)

func TestReconcileDeletesMetricsForMissingGateway(t *testing.T) {
	ResetMetrics()
	cli := fake.NewClient(t)
	r := &gatewayReconciler{
		gwClient: kclient.NewFilteredDelayed[*gwv1.Gateway](cli, gvr.KubernetesGateway, kclient.Filter{}),
	}
	cli.RunAndWait(test.NewStop(t))

	req := types.NamespacedName{Namespace: "ns", Name: "gw"}
	other := types.NamespacedName{Namespace: "ns", Name: "other"}
	// Series left by reconciles from before the Gateway was deleted.
	collectReconciliationMetrics("gateway", req)(nil)
	collectReconciliationMetrics("gateway", other)(nil)

	require.NoError(t, r.Reconcile(req))

	families, err := metrics.Registry().Gather()
	require.NoError(t, err)
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				require.False(t, lp.GetName() == nameLabel && lp.GetValue() == req.Name,
					"%s still has a series for the deleted Gateway", mf.GetName())
			}
		}
	}
	require.Equal(t, 1, reconciliationsTotal.DeletePartialMatch(metrics.Label{Name: nameLabel, Value: other.Name}),
		"other Gateways keep their series")
}
