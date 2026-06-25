package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

func TestReportInvalidGatewayParametersEnqueuesGatewayReport(t *testing.T) {
	ctx := context.Background()
	queue := utils.NewAsyncQueue[reports.ReportMap]()
	gateway := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "gw",
			Generation: 2,
		},
	}
	reconciler := &gatewayReconciler{
		gatewayStatusReportQueue: queue,
	}

	require.NoError(t, reconciler.reportInvalidGatewayParameters(ctx, gateway, errors.New("invalid parameters")))

	reportMap, err := queue.Dequeue(ctx)
	require.NoError(t, err)
	gatewayReport := reportMap.GatewayNamespaceName(types.NamespacedName{
		Namespace: gateway.Namespace,
		Name:      gateway.Name,
	})
	require.NotNil(t, gatewayReport)
	accepted := apimeta.FindStatusCondition(gatewayReport.GetConditions(), string(gwv1.GatewayConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, metav1.ConditionFalse, accepted.Status)
	require.Equal(t, string(gwv1.GatewayReasonInvalidParameters), accepted.Reason)
	require.Equal(t, "invalid parameters", accepted.Message)
	require.Equal(t, gateway.Generation, gatewayReport.GetObservedGeneration())
}
