package proxy_syncer

import (
	"fmt"
	"slices"
	"strings"

	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
	krtpkg "github.com/kgateway-dev/kgateway/v2/pkg/utils/krtutil"
)

// statusReportKey identifies the status report for one Kubernetes object. API
// version is intentionally excluded: promoted and pre-promotion route versions
// share storage and status ownership.
type statusReportKey struct {
	schema.GroupKind
	types.NamespacedName
}

func (k statusReportKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Group, k.Kind, k.Namespace, k.Name)
}

// statusReportFragment is one Gateway translation's contribution to one
// object's status. A route may have multiple fragments when it is attached to
// multiple Gateways.
type statusReportFragment struct {
	Target  statusReportKey
	Gateway types.NamespacedName
	Report  reports.ReportMap
}

func (f statusReportFragment) ResourceName() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		f.Target.Group, f.Target.Kind,
		f.Target.Namespace, f.Target.Name,
		f.Gateway.Namespace, f.Gateway.Name,
	)
}

func (f statusReportFragment) Equals(other statusReportFragment) bool {
	return f.Target == other.Target &&
		f.Gateway == other.Gateway &&
		reports.EqualReportMaps(f.Report, other.Report)
}

func newStatusReportFragments(
	snapshots krt.Collection[GatewayXdsResources],
	krtopts krtutil.KrtOptions,
) (krt.Collection[statusReportFragment], krt.Index[statusReportKey, statusReportFragment]) {
	fragments := krt.NewManyCollection(snapshots, func(_ krt.HandlerContext, snapshot GatewayXdsResources) []statusReportFragment {
		return splitStatusReport(snapshot.NamespacedName, snapshot.reports)
	}, krtopts.ToOptions("StatusReportFragments")...)

	index := krtpkg.UnnamedIndex(fragments, func(fragment statusReportFragment) []statusReportKey {
		return []statusReportKey{fragment.Target}
	})
	return fragments, index
}

func splitStatusReport(gateway types.NamespacedName, reportMap reports.ReportMap) []statusReportFragment {
	listenerSetCount := 0
	for _, byName := range reportMap.ListenerSets {
		listenerSetCount += len(byName)
	}
	fragments := make([]statusReportFragment, 0,
		len(reportMap.Gateways)+listenerSetCount+
			len(reportMap.HTTPRoutes)+len(reportMap.GRPCRoutes)+
			len(reportMap.TCPRoutes)+len(reportMap.TLSRoutes),
	)

	appendFragment := func(target statusReportKey, report reports.ReportMap) {
		fragments = append(fragments, statusReportFragment{
			Target:  target,
			Gateway: gateway,
			Report:  report,
		})
	}

	for nn, report := range reportMap.Gateways {
		if report != nil {
			appendFragment(statusReportKey{GroupKind: wellknown.GatewayGVK.GroupKind(), NamespacedName: nn}, reports.ReportMap{
				Gateways: map[types.NamespacedName]*reports.GatewayReport{nn: report},
			})
		}
	}
	for gvk, byName := range reportMap.ListenerSets {
		for nn, report := range byName {
			if report != nil {
				appendFragment(statusReportKey{GroupKind: gvk.GroupKind(), NamespacedName: nn}, reports.ReportMap{
					ListenerSets: map[schema.GroupVersionKind]map[types.NamespacedName]*reports.ListenerSetReport{
						gvk: {nn: report},
					},
				})
			}
		}
	}
	appendRouteFragments := func(gvk schema.GroupVersionKind, routeReports map[types.NamespacedName]*reports.RouteReport) {
		for nn, report := range routeReports {
			if report == nil {
				continue
			}
			fragmentReport := reports.ReportMap{}
			switch gvk.Kind {
			case wellknown.HTTPRouteKind:
				fragmentReport.HTTPRoutes = map[types.NamespacedName]*reports.RouteReport{nn: report}
			case wellknown.GRPCRouteKind:
				fragmentReport.GRPCRoutes = map[types.NamespacedName]*reports.RouteReport{nn: report}
			case wellknown.TCPRouteKind:
				fragmentReport.TCPRoutes = map[types.NamespacedName]*reports.RouteReport{nn: report}
			case wellknown.TLSRouteKind:
				fragmentReport.TLSRoutes = map[types.NamespacedName]*reports.RouteReport{nn: report}
			}
			appendFragment(statusReportKey{GroupKind: gvk.GroupKind(), NamespacedName: nn}, fragmentReport)
		}
	}
	appendRouteFragments(wellknown.HTTPRouteGVK, reportMap.HTTPRoutes)
	appendRouteFragments(wellknown.GRPCRouteGVK, reportMap.GRPCRoutes)
	appendRouteFragments(wellknown.TCPRouteGVK, reportMap.TCPRoutes)
	appendRouteFragments(wellknown.TLSRouteGVK, reportMap.TLSRoutes)

	return fragments
}

func fetchStatusReport(
	kctx krt.HandlerContext,
	fragments krt.Collection[statusReportFragment],
	index krt.Index[statusReportKey, statusReportFragment],
	key statusReportKey,
) *reports.ReportMap {
	matches := krt.Fetch(kctx, fragments, krt.FilterIndex(index, key))
	if len(matches) == 0 {
		return nil
	}
	slices.SortFunc(matches, func(a, b statusReportFragment) int {
		return strings.Compare(a.Gateway.String(), b.Gateway.String())
	})
	inputs := make([]reports.ReportMap, 0, len(matches))
	for _, fragment := range matches {
		inputs = append(inputs, fragment.Report)
	}
	merged := reports.MergeReportMaps(inputs...)
	return &merged
}
