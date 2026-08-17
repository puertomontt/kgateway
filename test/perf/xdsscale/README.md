# xDS fleet scale footprint

`TestXdsScaleFootprint` runs the whole kgateway control plane in-process against
envtest, creates N Gateways and M backends, connects one fake Envoy per Gateway over
ADS, and measures what the connected fleet costs in memory. It exists to A/B two
builds of the control plane — typically a branch against `main` — on the axis
per-client xDS work scales along, not to assert a threshold, so it is skipped unless
`XDSPERF` is set.

Every connected client gets its own CDS and EDS payload. A build that materializes a
separate `Cluster` and `ClusterLoadAssignment` proto for every (client, backend) pair
pays O(clients × backends) live heap; a build that shares those protos across clients
that resolve identically pays closer to O(backends) plus per-pair bookkeeping. This
harness measures the difference directly.

Everything is measured inside the test process. The envtest apiserver and etcd run as
separate processes, so their CPU is excluded; the fake Envoys run *in* the test
process, so their gRPC machinery is included in the numbers — equally on both sides.

A run assumes it has the machine to itself. Nothing enforces that: a concurrent
build, a second measurement, or another agent driving the same harness will contend
for CPU and quietly invalidate the numbers.

## The headline metric

`perClientHeapBytes` is the least-squares slope of post-GC live heap against the
number of connected clients, sampled at several client counts on the way up to the
full fleet (`XDSPERF_STEPS`). A slope rather than a total, because the total is
dominated by things this comparison is not about — informers, the client-independent
IR, the harness's own fixed cost — which all land in the intercept instead.

`slopeR2` is the fit quality and is reported so a bad measurement is visible rather
than silent. Below ~0.9 the samples are not on a line, which means the process was
not at steady state when they were taken; raise the client count, the backend count,
or the heap-stability requirement until the fit is clean, and do not quote the slope
until it is.

`perClientHeapFromDrain` is the same quantity estimated from the other direction:
the fleet disconnects and live heap is measured again, so `(steady − drained) /
clients`. The two estimates are computed from different data and should agree within
noise. `retained after drain` (drained − fixture) is the leak check: whatever the
fleet was holding has to be released when it disconnects.

| Metric | Meaning |
| --- | --- |
| `baselineHeap` | post-GC live heap after startup, before the fixture exists |
| `fixtureHeap` | after every object exists but no client has connected: the client-independent input |
| `samples[]` | post-GC live heap at each measured client count |
| `oneClientHeap` / `steadyHeap` | first and last of those samples |
| `drainedHeap` | after the whole fleet disconnects again |
| `perClientHeapBytes` | fitted slope: bytes of live heap per connected client |
| `perClientPerBackendBytes` | that slope divided by the backend count |
| `connect` | wall, CPU and cumulative allocation for bringing the fleet up |
| `churnEndpoints` | rewriting EndpointSlices with the fleet connected: the per-(client, backend) endpoint path |
| `churnReconnect` | reconnecting the whole fleet: every pair produced again from scratch |
| `xds.minClustersPerClient` / `minEndpointsPerClient` | the delivery guard, see below |
| `maxRSSBytes` | peak RSS of the test process (secondary — peak, not steady) |

`HeapAlloc` is the primary memory metric because it measures live allocations.
`HeapInuse` measures allocator spans, includes unused space within them, and can be
bimodal; it and peak RSS are secondary diagnostics.

### The delivery guard

Memory savings are only real if both builds served the fleet the same config. The
harness records the minimum cluster and endpoint resource count any client received
and fails the run if it is below the backend count, and the report prints both so a
"win" that comes from serving fewer resources cannot pass as a win. Always check that
row before quoting anything else.

## What the fixture looks like

- `XDSPERF_ZONES` Nodes with `topology.kubernetes.io/{region,zone}` labels, and one
  backing Pod per zone shared by every endpoint (unless `XDSPERF_ENDPOINT_PODS=false`,
  which drops endpoint locality entirely). The control plane reads an endpoint's Pod
  only for locality and labels, so a Pod per endpoint would create tens of thousands
  of objects to express the same topology.
- `XDSPERF_BACKENDS` Services, one EndpointSlice each with
  `XDSPERF_ENDPOINTS_PER_BACKEND` ready endpoints spread round-robin over the zones.
  The apiserver's service CIDR is widened to `/16` (`XDSPERF_SERVICE_CIDR`) because
  envtest's `10.0.0.0/24` default runs out of ClusterIPs at 254 Services and then
  fails every later create.
- `XDSPERF_GATEWAYS` Gateways with self-managed `GatewayParameters` (the deployer is
  out of the picture), one gateway Pod each — labeled and scheduled onto a zone — and
  `XDSPERF_ROUTES_PER_GATEWAY` HTTPRoutes.
- One ADS client per Gateway. Each subscribes to CDS wildcard, then to EDS by name
  for every cluster it learned plus its own local cluster, and ACKs every response.
  The ACKs matter: go-control-plane only opens the next watch for a type once the
  previous response is ACKed, so a client that stops ACKing stops receiving updates
  and makes every later phase look free.

Every client is a distinct `UniquelyConnectedClient` because the xDS role carries the
Gateway name. `XDSPERF_ZONES` controls how many of them can share a resolved result:
with one zone every client resolves endpoints identically (the best case for
sharing), with three they fall into three groups. Sweep it when the question is how
sensitive a sharing scheme is to fleet diversity.

## Running one measurement

```bash
XDSPERF=1 \
  XDSPERF_LABEL=mybranch \
  XDSPERF_GATEWAYS=20 \
  XDSPERF_BACKENDS=500 \
  XDSPERF_OUT=/tmp/xdsperf \
  go test -tags e2e -count=1 -timeout 90m -run TestXdsScaleFootprint ./test/perf/xdsscale/
```

Results land in `$XDSPERF_OUT` as `result-<label>.json` and `heap-<label>.pb.gz`, and
a single `XDSPERF_JSON {...}` line is printed to stdout for scripted collection. The
heap profile is taken with the full fleet connected, so `pprof -diff_base` between
two runs attributes the per-client difference directly.

Knobs: `XDSPERF_{GATEWAYS,BACKENDS,ENDPOINTS_PER_BACKEND,ZONES,STEPS,ROUTES_PER_GATEWAY,
CHURN_ROUNDS,CHURN_BACKENDS,ENDPOINT_PODS,QUIET,TIMEOUT,PARALLEL,STABLE_SAMPLES,
STABLE_TOLERANCE_PCT,STABLE_INTERVAL,MEMPROFILERATE,FIRST_CONNECT_DELAY,
APISERVER_START_TIMEOUT}`.

## Comparing two branches

```bash
# candidate = current worktree, base = main
hack/perf/xds-scale-compare.sh --base main --reps 3

# the noise floor: base vs base, run this first and trust nothing smaller than it
hack/perf/xds-scale-compare.sh --base main --control --reps 2
```

The script creates a detached worktree per side, copies one snapshot of the harness
into both so each side runs byte-identical measurement code, interleaves reps A/B
then B/A, pins `GOMAXPROCS`/`GOGC`, and verifies the harness hash in every tree before
and after the run. `hack/perf/xds_scale_report.py` prints the median per side with
the per-rep spread, and the `pprof -diff_base` command for the median reps.

### Methodology traps

These are the ways a comparison silently becomes invalid. Every one of them has
happened.

- **The two sides run different harness versions.** The harness is untracked on the
  base ref, so it has to be copied in. If it is copied from the live worktree, an
  edit landing between the two copies puts the sides on different measurement code —
  and the failure mode is a large, plausible, entirely fake delta. The script copies
  from a snapshot taken up front and hashes every tree before and after; do not work
  around that.
- **Editing a running script.** Bash re-reads a script from disk while running it.
  Run the sweep from a copy if you intend to edit it.
- **Shared worktrees.** Two comparisons using the same tree overwrite each other's
  harness. The script uses per-run private trees.
- **No noise floor.** Run `--control` first. Memory noise is usually well under 1%
  and CPU noise around 8%, but confirm it on the machine actually being used, and do
  not report a delta smaller than what base-vs-base produces.
- **Reading the total instead of the slope.** Steady heap includes everything that
  does not scale with the fleet, so a large per-client win shows up diluted there.
  The slope is the number; the total is context.
- **Quoting a slope with a bad R².** See above.
