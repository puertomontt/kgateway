package proxy_syncer

import (
	"slices"
	"strings"

	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
	krtpkg "github.com/kgateway-dev/kgateway/v2/pkg/utils/krtutil"
)

// Report fragments explode each gateway translation's ReportMap into keyed
// per-resource pieces so the derived status collections can depend on exactly
// the reports that concern them, instead of fetching the fully merged report
// singleton. With the singleton, any translation change recomputed the desired
// status of every resource; with fragments, a gateway translation only
// recomputes its own fragments, and krt equality stops propagation for the
// resources whose reports did not change.

// routeReportFragment is one gateway translation's report for one route.
// A route attached to multiple gateways has one fragment per gateway; they are
// merged when building the route's desired status.
type routeReportFragment struct {
	Kind    string
	Route   types.NamespacedName
	Gateway types.NamespacedName
	Report  *reports.RouteReport
}

func (f routeReportFragment) ResourceName() string {
	return f.Kind + "/" + f.Route.String() + "/" + f.Gateway.String()
}

func (f routeReportFragment) Equals(o routeReportFragment) bool {
	return f.Kind == o.Kind &&
		f.Route == o.Route &&
		f.Gateway == o.Gateway &&
		reports.RouteReportsEqual(f.Report, o.Report)
}

// routeFragmentKey indexes route fragments by the route they report on.
type routeFragmentKey struct {
	Kind  string
	Route types.NamespacedName
}

// gatewayReportFragment is one gateway translation's report for its own Gateway.
type gatewayReportFragment struct {
	Gateway types.NamespacedName
	Report  *reports.GatewayReport
}

func (f gatewayReportFragment) ResourceName() string {
	return f.Gateway.String()
}

func (f gatewayReportFragment) Equals(o gatewayReportFragment) bool {
	return f.Gateway == o.Gateway && reports.GatewayReportsEqual(f.Report, o.Report)
}

// newRouteReportFragments explodes each gateway translation's route reports into
// per-(route, gateway) fragments. Reports are cloned so fragments do not alias the
// per-translation report maps.
func newRouteReportFragments(
	snapshots krt.Collection[GatewayXdsResources],
	krtopts krtutil.KrtOptions,
) (krt.Collection[routeReportFragment], krt.Index[routeFragmentKey, routeReportFragment]) {
	fragments := krt.NewManyCollection(snapshots, func(kctx krt.HandlerContext, gwXds GatewayXdsResources) []routeReportFragment {
		rm := gwXds.reports
		out := make([]routeReportFragment, 0, len(rm.HTTPRoutes)+len(rm.GRPCRoutes)+len(rm.TCPRoutes)+len(rm.TLSRoutes))
		appendFragments := func(kind string, routeReports map[types.NamespacedName]*reports.RouteReport) {
			for nn, rr := range routeReports {
				out = append(out, routeReportFragment{
					Kind:    kind,
					Route:   nn,
					Gateway: gwXds.NamespacedName,
					Report:  reports.CloneRouteReport(rr),
				})
			}
		}
		appendFragments(wellknown.HTTPRouteKind, rm.HTTPRoutes)
		appendFragments(wellknown.GRPCRouteKind, rm.GRPCRoutes)
		appendFragments(wellknown.TCPRouteKind, rm.TCPRoutes)
		appendFragments(wellknown.TLSRouteKind, rm.TLSRoutes)
		return out
	}, krtopts.ToOptions("RouteReportFragments")...)

	index := krtpkg.UnnamedIndex(fragments, func(f routeReportFragment) []routeFragmentKey {
		return []routeFragmentKey{{Kind: f.Kind, Route: f.Route}}
	})
	return fragments, index
}

// newGatewayReportFragments extracts each gateway translation's report for its own
// Gateway, keyed by the gateway's namespaced name.
func newGatewayReportFragments(
	snapshots krt.Collection[GatewayXdsResources],
	krtopts krtutil.KrtOptions,
) krt.Collection[gatewayReportFragment] {
	return krt.NewCollection(snapshots, func(kctx krt.HandlerContext, gwXds GatewayXdsResources) *gatewayReportFragment {
		report := gwXds.reports.Gateways[gwXds.NamespacedName]
		if report == nil {
			return nil
		}
		return &gatewayReportFragment{
			Gateway: gwXds.NamespacedName,
			Report:  reports.CloneGatewayReport(report),
		}
	}, krtopts.ToOptions("GatewayReportFragments")...)
}

// mergeRouteFragments merges a route's fragments (one per gateway) into a single
// route report, in deterministic gateway order.
func mergeRouteFragments(fragments []routeReportFragment) *reports.RouteReport {
	slices.SortFunc(fragments, func(a, b routeReportFragment) int {
		return strings.Compare(a.Gateway.String(), b.Gateway.String())
	})
	routeReports := make([]*reports.RouteReport, 0, len(fragments))
	for _, f := range fragments {
		routeReports = append(routeReports, f.Report)
	}
	return reports.MergeRouteReports(routeReports...)
}
