package proxy_syncer

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/reporter"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

// TestSyncGatewayStatusMergesControllerInvalidParametersReport verifies that
// syncGatewayStatus merges an Accepted=False/InvalidParameters condition
// reported by the controller (via the controller report queue) with the
// translated report when the latter only carries a Programmed condition. This
// exercises the full write path through the client, complementing the
// builder-level coverage in pkg/reports.
func TestSyncGatewayStatusMergesControllerInvalidParametersReport(t *testing.T) {
	ctx := context.Background()
	const controllerName = "kgateway.dev/kgateway"
	gatewayKey := types.NamespacedName{Namespace: "default", Name: "gw"}

	gateway := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  gatewayKey.Namespace,
			Name:       gatewayKey.Name,
			Generation: 2,
		},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "kgateway",
			Listeners: []gwv1.Listener{{
				Name:     "http",
				Port:     80,
				Protocol: gwv1.HTTPProtocolType,
			}},
		},
	}
	gatewayClass := &gwv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "kgateway"},
		Spec: gwv1.GatewayClassSpec{
			ControllerName: gwv1.GatewayController(controllerName),
		},
	}

	kubeClient := newFakeGatewayStatusClient(t, gateway, gatewayClass)

	rm := reports.NewReportMap()
	translatedGateway := gateway.DeepCopy()
	translatedGateway.Status = gwv1.GatewayStatus{}
	gwReporter := reports.NewReporter(&rm).Gateway(translatedGateway)
	gwReporter.SetCondition(reporter.GatewayCondition{
		Type:    gwv1.GatewayConditionProgrammed,
		Status:  metav1.ConditionTrue,
		Reason:  gwv1.GatewayReasonProgrammed,
		Message: reports.GatewayProgrammedMessage,
	})

	syncer := &StatusSyncer{
		mgr:                      statusSyncerTestManager{client: kubeClient},
		controllerName:           controllerName,
		gatewayControllerReports: make(map[types.NamespacedName]*reports.GatewayReport),
	}
	controllerReport := gatewayStatusReport(gateway, reporter.GatewayCondition{
		Type:    gwv1.GatewayConditionAccepted,
		Status:  metav1.ConditionFalse,
		Reason:  gwv1.GatewayReasonInvalidParameters,
		Message: "invalid gateway parameters",
	})
	syncer.gatewayStatusReportForControllerReport(controllerReport)
	mergedReport := syncer.gatewayStatusReportForTranslation(rm)
	syncer.syncGatewayStatus(ctx, slog.New(slog.DiscardHandler), mergedReport)

	updated := &gwv1.Gateway{}
	require.NoError(t, kubeClient.Get(ctx, gatewayKey, updated))

	accepted := apimeta.FindStatusCondition(updated.Status.Conditions, string(gwv1.GatewayConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, metav1.ConditionFalse, accepted.Status)
	require.Equal(t, string(gwv1.GatewayReasonInvalidParameters), accepted.Reason)
	require.Equal(t, "invalid gateway parameters", accepted.Message)

	programmed := apimeta.FindStatusCondition(updated.Status.Conditions, string(gwv1.GatewayConditionProgrammed))
	require.NotNil(t, programmed)
	require.Equal(t, metav1.ConditionTrue, programmed.Status)
	require.Equal(t, string(gwv1.GatewayReasonProgrammed), programmed.Reason)
}

// TestSyncGatewayStatusClearsControllerInvalidParametersReport verifies that a
// later controller report flipping Accepted back to True clears the stored
// InvalidParameters overlay so the gateway no longer reports the stale
// condition.
func TestSyncGatewayStatusClearsControllerInvalidParametersReport(t *testing.T) {
	ctx := context.Background()
	const controllerName = "kgateway.dev/kgateway"
	gatewayKey := types.NamespacedName{Namespace: "default", Name: "gw"}

	gateway := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  gatewayKey.Namespace,
			Name:       gatewayKey.Name,
			Generation: 2,
		},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "kgateway",
			Listeners: []gwv1.Listener{{
				Name:     "http",
				Port:     80,
				Protocol: gwv1.HTTPProtocolType,
			}},
		},
	}
	gatewayClass := &gwv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "kgateway"},
		Spec: gwv1.GatewayClassSpec{
			ControllerName: gwv1.GatewayController(controllerName),
		},
	}

	kubeClient := newFakeGatewayStatusClient(t, gateway, gatewayClass)
	syncer := &StatusSyncer{
		mgr:                      statusSyncerTestManager{client: kubeClient},
		controllerName:           controllerName,
		gatewayControllerReports: make(map[types.NamespacedName]*reports.GatewayReport),
	}

	translationReport := reports.NewReportMap()
	reports.NewReporter(&translationReport).Gateway(gateway)
	syncer.gatewayStatusReportForControllerReport(gatewayStatusReport(gateway, reporter.GatewayCondition{
		Type:    gwv1.GatewayConditionAccepted,
		Status:  metav1.ConditionFalse,
		Reason:  gwv1.GatewayReasonInvalidParameters,
		Message: "invalid gateway parameters",
	}))

	syncer.gatewayStatusReportForControllerReport(gatewayStatusReport(gateway, reporter.GatewayCondition{
		Type:    gwv1.GatewayConditionAccepted,
		Status:  metav1.ConditionTrue,
		Reason:  gwv1.GatewayReasonAccepted,
		Message: reports.GatewayAcceptedMessage,
	}))
	mergedReport := syncer.gatewayStatusReportForTranslation(translationReport)
	syncer.syncGatewayStatus(ctx, slog.New(slog.DiscardHandler), mergedReport)

	updated := &gwv1.Gateway{}
	require.NoError(t, kubeClient.Get(ctx, gatewayKey, updated))
	accepted := apimeta.FindStatusCondition(updated.Status.Conditions, string(gwv1.GatewayConditionAccepted))
	require.NotNil(t, accepted)
	require.Equal(t, metav1.ConditionTrue, accepted.Status)
	require.Equal(t, string(gwv1.GatewayReasonAccepted), accepted.Reason)
}

func gatewayStatusReport(gateway *gwv1.Gateway, condition reporter.GatewayCondition) reports.ReportMap {
	rm := reports.NewReportMap()
	reports.NewReporter(&rm).Gateway(gateway).SetCondition(condition)
	return rm
}

func newFakeGatewayStatusClient(t *testing.T, objs ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, gwv1.Install(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&gwv1.Gateway{}).
		WithObjects(objs...).
		Build()
}

type statusSyncerTestManager struct {
	manager.Manager
	client ctrlclient.Client
}

func (m statusSyncerTestManager) GetClient() ctrlclient.Client {
	return m.client
}
