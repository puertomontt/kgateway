#!/usr/bin/env python3
"""Aggregate test/perf/xdsscale result JSON files into an A/B comparison.

Reports the median of each metric per side plus the per-rep spread, so a small
median delta can be judged against the run-to-run noise instead of being read as a
result on its own.

The headline metric is `perClientHeapBytes`: the fitted slope of live heap against
connected client count. Totals (steady heap, peak RSS) are reported too, but they
include everything that does not scale with the fleet, so a large relative win on
the slope shows up diluted there.
"""

import argparse
import glob
import json
import os
import statistics
import sys

# (label, json path, unit, lower_is_better). None marks an informational metric for
# which the report must not infer better/worse.
METRICS = [
    ("per-client live heap (slope)", ("perClientHeapBytes",), "bytes", True),
    ("per-client per-backend", ("perClientPerBackendBytes",), "bytes", True),
    ("per-client live heap (drop-back)", ("perClientHeapFromDrain",), "bytes", True),
    ("slope fit R2 (>=0.9 to trust)", ("slopeR2",), "ratio", None),
    ("fleet live heap (steady-fixture)", ("__fleet_heap",), "bytes", True),
    ("steady live heap (HeapAlloc)", ("steadyHeap", "heapAllocBytes"), "bytes", True),
    ("steady heap objects", ("steadyHeap", "heapObjects"), "count", True),
    ("fixture live heap (no clients)", ("fixtureHeap", "heapAllocBytes"), "bytes", None),
    ("drained live heap (0 clients)", ("drainedHeap", "heapAllocBytes"), "bytes", True),
    ("retained after drain (drained-fixture)", ("__retained",), "bytes", True),
    ("heap span inuse (secondary)", ("steadyHeap", "heapInuseBytes"), "bytes", None),
    ("peak rss (secondary)", ("maxRSSBytes",), "bytes", None),
    ("total alloc (whole run)", ("totalAllocBytes",), "bytes", True),
    ("connect fleet alloc", ("connect", "allocBytes"), "bytes", True),
    ("connect fleet cpu", ("connect", "cpuSeconds"), "seconds", True),
    ("endpoint churn alloc", ("churnEndpoints", "allocBytes"), "bytes", True),
    ("endpoint churn cpu", ("churnEndpoints", "cpuSeconds"), "seconds", True),
    ("reconnect alloc", ("churnReconnect", "allocBytes"), "bytes", True),
    ("reconnect cpu", ("churnReconnect", "cpuSeconds"), "seconds", True),
    ("goroutines", ("steadyHeap", "goroutines"), "count", None),
    # Delivery guard: both sides must serve the same resources for any of the above
    # to mean anything.
    ("clusters per client (must match)", ("xds", "minClustersPerClient"), "count", None),
    ("endpoints per client (must match)", ("xds", "minEndpointsPerClient"), "count", None),
    ("resources delivered (whole run)", ("xds", "resourcesDelivered"), "count", None),
]


def dig(obj, path):
    if path == ("__fleet_heap",):
        return obj["steadyHeap"]["heapAllocBytes"] - obj["fixtureHeap"]["heapAllocBytes"]
    if path == ("__retained",):
        return obj["drainedHeap"]["heapAllocBytes"] - obj["fixtureHeap"]["heapAllocBytes"]
    for key in path:
        obj = obj[key]
    return obj


def load_side(out_dir, side):
    runs = []
    for path in sorted(glob.glob(os.path.join(out_dir, side, "rep*", "result-*.json"))):
        with open(path) as f:
            data = json.load(f)
        data["__path"] = path
        runs.append(data)
    return runs


def fmt(value, unit):
    if unit == "bytes":
        v = float(value)
        sign = "-" if v < 0 else ""
        v = abs(v)
        for suffix, scale in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if v >= scale:
                return f"{sign}{v / scale:.2f}{suffix}"
        return f"{sign}{v:.0f}B"
    if unit == "seconds":
        return f"{float(value):.2f}s"
    if unit == "ratio":
        return f"{float(value):.3f}"
    return f"{value:g}" if isinstance(value, float) else str(value)


def spread(values, unit):
    if len(values) < 2:
        return ""
    lo, hi = min(values), max(values)
    med = statistics.median(values)
    pct = (hi - lo) / med * 100 if med else 0.0
    return f"[{fmt(lo, unit)}..{fmt(hi, unit)}] {pct:.1f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--candidate", default="candidate")
    ap.add_argument("--candidate-desc", default="candidate")
    ap.add_argument("--base-desc", default="base")
    args = ap.parse_args()

    base = load_side(args.out, "base")
    cand = load_side(args.out, args.candidate)
    if not base or not cand:
        print(f"no results found under {args.out} (base: {len(base)}, {args.candidate}: {len(cand)})",
              file=sys.stderr)
        return 1

    cfg = cand[0]["config"]
    print("=" * 150)
    print(f"xds fleet scale footprint: {args.candidate_desc}  vs  {args.base_desc}")
    print(f"scale: {cfg['gateways']} clients, {cfg['backends']} backends, "
          f"{cfg['endpointsPerBackend']} endpoints/backend, {cfg['zones']} zones; "
          f"churn {cfg['churnRounds']} rounds x {cfg['churnBackends']} backends")
    print(f"reps: {len(cand)} candidate, {len(base)} base; "
          f"GOMAXPROCS={cand[0]['gomaxprocs']} GOGC={cand[0]['gogc']} "
          f"{cand[0]['goos']}/{cand[0]['goarch']}")
    print("=" * 150)

    header = (f"{'metric':43} {'base':>12} {'candidate':>12} {'delta':>12} {'delta %':>9}  "
              f"{'base spread':>24}  {'candidate spread':>24}")
    print(header)
    print("-" * len(header))

    for label, path, unit, lower_better in METRICS:
        try:
            b_vals = [dig(r, path) for r in base]
            c_vals = [dig(r, path) for r in cand]
        except (KeyError, TypeError):
            continue
        b_med = statistics.median(b_vals)
        c_med = statistics.median(c_vals)
        delta = c_med - b_med
        pct = (delta / b_med * 100) if b_med else float("nan")
        marker = ""
        if lower_better is not None and b_med and abs(pct) >= 5:
            improved = delta < 0 if lower_better else delta > 0
            marker = "  <-- better" if improved else "  <-- WORSE"
        print(f"{label:43} {fmt(b_med, unit):>12} {fmt(c_med, unit):>12} "
              f"{fmt(delta, unit):>12} {pct:>8.1f}%  {spread(b_vals, unit):>24}  "
              f"{spread(c_vals, unit):>24}{marker}")

    print()
    print("live heap by client count (median rep per side):")

    def median_run(runs):
        ordered = sorted(runs, key=lambda r: dig(r, ("perClientHeapBytes",)))
        return ordered[len(ordered) // 2]

    b_med_run, c_med_run = median_run(base), median_run(cand)
    for side, run in (("base", b_med_run), (args.candidate, c_med_run)):
        points = ", ".join(f"{s['clients']}:{fmt(s['heapAllocBytes'], 'bytes')}" for s in run["samples"])
        print(f"  {side:>10}: {points}")

    print()
    print("per-rep per-client slope:")
    for side, runs in (("base", base), (args.candidate, cand)):
        vals = ", ".join(fmt(dig(r, ("perClientHeapBytes",)), "bytes") for r in runs)
        print(f"  {side:>10}: {vals}")

    # Point at the median reps' heap profiles: the profile diff is what explains a
    # heap delta, and it is the part reviewers actually check.
    b_prof, c_prof = b_med_run.get("heapProfile", ""), c_med_run.get("heapProfile", "")
    if b_prof and c_prof and os.path.exists(b_prof) and os.path.exists(c_prof):
        print()
        print("attribute the heap delta (median reps, both with the full fleet connected):")
        print(f"  go tool pprof -top -nodecount=25 -diff_base={b_prof} {c_prof}")
        print(f"  go tool pprof -http=: -diff_base={b_prof} {c_prof}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
