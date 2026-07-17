package reports

// This file exposes per-report clone/merge/equality helpers so callers can build
// keyed report fragment collections (one report per resource per translation)
// instead of fetching a fully merged ReportMap. Fragments enable precise krt
// dependency tracking: a change to one translation only recomputes the statuses
// of the resources whose fragments actually changed.

// CloneRouteReport returns a deep copy of the given route report. The returned
// report does not alias the input, so the caller may retain it across translations.
func CloneRouteReport(in *RouteReport) *RouteReport {
	return cloneRouteReport(in)
}

// RouteReportsEqual reports whether two route reports are semantically equal,
// ignoring condition LastTransitionTime.
func RouteReportsEqual(a, b *RouteReport) bool {
	return routeReportEqual(a, b)
}

// MergeRouteReports merges the given route reports (parent reports are combined
// across inputs) into a new report owned by the caller. Nil inputs are skipped;
// returns nil if all inputs are nil.
func MergeRouteReports(inputs ...*RouteReport) *RouteReport {
	var merged *RouteReport
	for _, in := range inputs {
		if in == nil {
			continue
		}
		if merged == nil {
			merged = cloneRouteReport(in)
			continue
		}
		mergeParentReports(merged, in)
	}
	return merged
}

// CloneGatewayReport returns a deep copy of the given gateway report. The returned
// report does not alias the input, so the caller may retain it across translations.
func CloneGatewayReport(in *GatewayReport) *GatewayReport {
	return cloneGatewayReport(in)
}

// GatewayReportsEqual reports whether two gateway reports are semantically equal,
// ignoring condition LastTransitionTime.
func GatewayReportsEqual(a, b *GatewayReport) bool {
	return gatewayReportEqual(a, b)
}
