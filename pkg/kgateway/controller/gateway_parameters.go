package controller

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"istio.io/istio/pkg/config/schema/gvr"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/kclient"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilretry "k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/api/v1alpha1/kgateway"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
	"github.com/kgateway-dev/kgateway/v2/pkg/utils/kubeutils"
)

var _ manager.LeaderElectionRunnable = (*gatewayParametersReconciler)(nil)

const (
	gatewayParametersConditionReferenced = "Referenced"
	gatewayParametersReasonDirectRef     = "DirectReference"
)

type gatewayParametersReconciler struct {
	gwParamClient             kclient.Client[*kgateway.GatewayParameters]
	gwClient                  kclient.Client[*gwv1.Gateway]
	gwClassClient             kclient.Client[*gwv1.GatewayClass]
	gatewaysByDirectParamsRef kclient.Index[types.NamespacedName, *gwv1.Gateway]
	gatewayClassesByParamsRef kclient.Index[types.NamespacedName, *gwv1.GatewayClass]
	controllerName            string
	queue                     controllers.Queue
}

func newGatewayParametersReconciler(cfg GatewayConfig) *gatewayParametersReconciler {
	filter := kclient.Filter{ObjectFilter: cfg.Client.ObjectFilter()}
	r := &gatewayParametersReconciler{
		gwParamClient:  kclient.NewFilteredDelayed[*kgateway.GatewayParameters](cfg.Client, wellknown.GatewayParametersGVR, filter),
		gwClient:       kclient.NewFilteredDelayed[*gwv1.Gateway](cfg.Client, gvr.KubernetesGateway, filter),
		gwClassClient:  kclient.NewFilteredDelayed[*gwv1.GatewayClass](cfg.Client, wellknown.GatewayClassGVR, filter),
		controllerName: cfg.ControllerName,
	}
	r.queue = controllers.NewQueue(
		"GatewayParametersController",
		controllers.WithReconciler(r.reconcile),
		controllers.WithMaxAttempts(math.MaxInt),
		controllers.WithRateLimiter(rateLimiter),
	)
	r.gatewaysByDirectParamsRef = kclient.CreateIndex(r.gwClient, "gatewayparameters-direct-ref", func(o *gwv1.Gateway) []types.NamespacedName {
		ref := fetchValidGatewayParametersRefFromGateway(o)
		if ref == nil {
			return nil
		}
		return []types.NamespacedName{*ref}
	})
	r.gatewayClassesByParamsRef = kclient.CreateIndex(r.gwClassClient, "gatewayparameters-gatewayclass-ref", func(o *gwv1.GatewayClass) []types.NamespacedName {
		ref := fetchValidGatewayParametersRefFromGatewayClass(o)
		if ref == nil {
			return nil
		}
		return []types.NamespacedName{*ref}
	})

	r.gwParamClient.AddEventHandler(controllers.ObjectHandler(func(o controllers.Object) {
		logger.Debug("reconciling GatewayParameters due to GatewayParameters event", "ref", kubeutils.NamespacedNameFrom(o))
		r.queue.AddObject(o)
	}))
	r.gwClient.AddEventHandler(controllers.FromEventHandler(func(o controllers.Event) {
		switch o.Event {
		case controllers.EventAdd:
			r.enqueueGatewayReference(o.New.(*gwv1.Gateway))
		case controllers.EventUpdate:
			r.enqueueGatewayReference(o.Old.(*gwv1.Gateway))
			r.enqueueGatewayReference(o.New.(*gwv1.Gateway))
		case controllers.EventDelete:
			r.enqueueGatewayReference(o.Old.(*gwv1.Gateway))
		}
	}))
	r.gwClassClient.AddEventHandler(controllers.FromEventHandler(func(o controllers.Event) {
		switch o.Event {
		case controllers.EventAdd:
			r.enqueueGatewayClassReference(o.New.(*gwv1.GatewayClass))
		case controllers.EventUpdate:
			r.enqueueGatewayClassReference(o.Old.(*gwv1.GatewayClass))
			r.enqueueGatewayClassReference(o.New.(*gwv1.GatewayClass))
		case controllers.EventDelete:
			r.enqueueGatewayClassReference(o.Old.(*gwv1.GatewayClass))
		}
	}))

	return r
}

func (r *gatewayParametersReconciler) NeedLeaderElection() bool {
	return true
}

func (r *gatewayParametersReconciler) Start(ctx context.Context) error {
	kube.WaitForCacheSync(
		"GatewayParametersController",
		ctx.Done(),
		r.gwParamClient.HasSynced,
		r.gwClient.HasSynced,
		r.gwClassClient.HasSynced,
	)
	r.queue.Run(ctx.Done())
	controllers.ShutdownAll(r.gwParamClient, r.gwClient, r.gwClassClient)
	return nil
}

func (r *gatewayParametersReconciler) reconcile(req types.NamespacedName) (rErr error) {
	finishMetrics := collectReconciliationMetrics("gatewayparameters", req)
	defer func() {
		finishMetrics(rErr)
	}()

	gwp := r.gwParamClient.Get(req.Name, req.Namespace)
	if gwp == nil || gwp.GetDeletionTimestamp() != nil {
		logger.Debug("gatewayparameters not found, skipping reconciliation", "ref", req)
		return nil
	}

	desired := r.desiredStatus(gwp)
	return r.updateStatusWithRetry(req, desired)
}

func (r *gatewayParametersReconciler) enqueueGatewayReference(gw *gwv1.Gateway) {
	ref := fetchValidGatewayParametersRefFromGateway(gw)
	if ref == nil {
		return
	}
	logger.Debug("reconciling GatewayParameters due to Gateway event", "gateway", kubeutils.NamespacedNameFrom(gw), "gatewayparameters", *ref)
	r.queue.Add(*ref)
}

func (r *gatewayParametersReconciler) enqueueGatewayClassReference(gwc *gwv1.GatewayClass) {
	ref := fetchValidGatewayParametersRefFromGatewayClass(gwc)
	if ref == nil {
		return
	}
	logger.Debug("reconciling GatewayParameters due to GatewayClass event", "gatewayclass", gwc.Name, "gatewayparameters", *ref)
	r.queue.Add(*ref)
}

func (r *gatewayParametersReconciler) desiredStatus(gwp *kgateway.GatewayParameters) kgateway.GatewayParametersStatus {
	parents := make([]gwv1.RouteParentStatus, 0)
	existingByKey := make(map[string]gwv1.RouteParentStatus, len(gwp.Status.Parents))
	for _, parent := range gwp.Status.Parents {
		existingByKey[reports.ParentString(parent.ParentRef)] = parent
	}

	for _, gw := range r.gatewaysByDirectParamsRef.Lookup(clientObjectKey(gwp)) {
		parentRef := gatewayParentReference(gw)
		key := reports.ParentString(parentRef)
		parent := gwv1.RouteParentStatus{
			ParentRef:      parentRef,
			ControllerName: gwv1.GatewayController(r.controllerName),
			Conditions:     buildParentConditions(existingByKey[key].Conditions, gwp.Generation, "Gateway", gw.Namespace, gw.Name),
		}
		parents = append(parents, parent)
	}

	for _, gwc := range r.gatewayClassesByParamsRef.Lookup(clientObjectKey(gwp)) {
		parentRef := gatewayClassParentReference(gwc)
		key := reports.ParentString(parentRef)
		parent := gwv1.RouteParentStatus{
			ParentRef:      parentRef,
			ControllerName: gwv1.GatewayController(r.controllerName),
			Conditions:     buildParentConditions(existingByKey[key].Conditions, gwp.Generation, "GatewayClass", "", gwc.Name),
		}
		parents = append(parents, parent)
	}

	slices.SortStableFunc(parents, func(a, b gwv1.RouteParentStatus) int {
		return strings.Compare(reports.ParentString(a.ParentRef), reports.ParentString(b.ParentRef))
	})
	parents = slices.CompactFunc(parents, func(a, b gwv1.RouteParentStatus) bool {
		return reports.ParentString(a.ParentRef) == reports.ParentString(b.ParentRef)
	})
	if parents == nil {
		parents = []gwv1.RouteParentStatus{}
	}
	return kgateway.GatewayParametersStatus{Parents: parents}
}

func (r *gatewayParametersReconciler) updateStatusWithRetry(req types.NamespacedName, desired kgateway.GatewayParametersStatus) error {
	err := utilretry.RetryOnConflict(utilretry.DefaultRetry, func() error {
		gwp := r.gwParamClient.Get(req.Name, req.Namespace)
		if gwp == nil {
			return nil
		}
		if slices.EqualFunc(gwp.Status.Parents, desired.Parents, routeParentStatusEqual) {
			return nil
		}
		_, err := r.gwParamClient.UpdateStatus(&kgateway.GatewayParameters{
			ObjectMeta: pluginsdk.CloneObjectMetaForStatus(gwp.ObjectMeta),
			Status:     desired,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to update GatewayParameters status: %w", err)
	}
	return nil
}

func routeParentStatusEqual(a, b gwv1.RouteParentStatus) bool {
	if !parentReferenceEqual(a.ParentRef, b.ParentRef) {
		return false
	}
	if a.ControllerName != b.ControllerName {
		return false
	}
	return slices.EqualFunc(a.Conditions, b.Conditions, conditionEqual)
}

func parentReferenceEqual(a, b gwv1.ParentReference) bool {
	return ptr.Equal(a.Group, b.Group) &&
		ptr.Equal(a.Kind, b.Kind) &&
		ptr.Equal(a.Namespace, b.Namespace) &&
		ptr.Equal(a.SectionName, b.SectionName) &&
		ptr.Equal(a.Port, b.Port) &&
		a.Name == b.Name
}

func conditionEqual(a, b metav1.Condition) bool {
	return a.Type == b.Type &&
		a.Status == b.Status &&
		a.ObservedGeneration == b.ObservedGeneration &&
		a.Reason == b.Reason &&
		a.Message == b.Message
}

func buildParentConditions(existing []metav1.Condition, observedGeneration int64, kind, namespace, name string) []metav1.Condition {
	conds := slices.Clone(existing)
	refName := name
	if namespace != "" {
		refName = fmt.Sprintf("%s/%s", namespace, name)
	}
	meta.SetStatusCondition(&conds, metav1.Condition{
		Type:               gatewayParametersConditionReferenced,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: observedGeneration,
		Reason:             gatewayParametersReasonDirectRef,
		Message:            fmt.Sprintf("%s %s directly references this GatewayParameters", kind, refName),
	})
	return conds
}

func fetchValidGatewayParametersRefFromGateway(gw *gwv1.Gateway) *types.NamespacedName {
	if gw.Spec.Infrastructure == nil || gw.Spec.Infrastructure.ParametersRef == nil {
		return nil
	}
	ref := gw.Spec.Infrastructure.ParametersRef
	if ref.Name == "" || ref.Group != kgateway.GroupName || ref.Kind != gwv1.Kind(wellknown.GatewayParametersGVK.Kind) {
		return nil
	}
	return &types.NamespacedName{
		Namespace: gw.Namespace,
		Name:      ref.Name,
	}
}

func fetchValidGatewayParametersRefFromGatewayClass(gwc *gwv1.GatewayClass) *types.NamespacedName {
	ref := gwc.Spec.ParametersRef
	if ref == nil || ref.Namespace == nil {
		return nil
	}
	if ref.Name == "" || ref.Group != gwv1.Group(wellknown.GatewayParametersGVK.Group) || ref.Kind != gwv1.Kind(wellknown.GatewayParametersGVK.Kind) {
		return nil
	}
	return &types.NamespacedName{
		Namespace: string(*ref.Namespace),
		Name:      string(ref.Name),
	}
}

func gatewayParentReference(gw *gwv1.Gateway) gwv1.ParentReference {
	return gwv1.ParentReference{
		Group:     ptr.To(gwv1.Group(wellknown.GatewayGVK.Group)),
		Kind:      ptr.To(gwv1.Kind(wellknown.GatewayGVK.Kind)),
		Namespace: ptr.To(gwv1.Namespace(gw.Namespace)),
		Name:      gwv1.ObjectName(gw.Name),
	}
}

func gatewayClassParentReference(gwc *gwv1.GatewayClass) gwv1.ParentReference {
	return gwv1.ParentReference{
		Group: ptr.To(gwv1.Group(wellknown.GatewayClassGVK.Group)),
		Kind:  ptr.To(gwv1.Kind(wellknown.GatewayClassGVK.Kind)),
		Name:  gwv1.ObjectName(gwc.Name),
	}
}

func clientObjectKey(obj metav1.Object) types.NamespacedName {
	return types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
}
