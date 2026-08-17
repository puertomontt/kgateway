// Package xdsscale measures what a connected xDS fleet costs the control plane in
// memory, in-process, against envtest.
//
// It exists to A/B two builds (typically a branch against main) on the axis the
// per-client xDS work scales along: clients x backends. Every connected client gets
// its own CDS and EDS payload, so a build that materializes one Cluster and one
// ClusterLoadAssignment proto per (client, backend) pair pays O(clients x backends)
// live heap, while a build that shares those protos across clients that resolve
// identically pays closer to O(backends). The headline number here is the marginal
// live heap per additional connected client, and its per-backend quotient.
//
// The harness depends only on APIs that are stable across branches — envtest setup,
// Kubernetes objects, and the xDS wire protocol — so the exact same file can be
// dropped into two worktrees and produce comparable numbers. See README.md for the
// procedure, including the ways an A/B comparison can silently become invalid.
package xdsscale_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoydiscoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	istiokube "istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/test/envtestassets"
	"github.com/kgateway-dev/kgateway/v2/test/envtestutil"
)

const (
	// fixtureNamespace holds every object the harness creates.
	fixtureNamespace = "gwtest"
	// gatewayNameLabel is the label the deployer puts on gateway pods; the control
	// plane reads it to map a connected client back to its Gateway. Spelled out
	// rather than imported so this file depends only on cross-branch-stable APIs.
	gatewayNameLabel = "gateway.networking.k8s.io/gateway-name"
	// rolePrefix is the xDS node metadata "role" prefix that marks a client as a
	// Gateway API proxy. Full role is <prefix>~<namespace>~<gateway>.
	rolePrefix = "kgateway-kube-gateway-api"

	clusterTypeURL  = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	endpointTypeURL = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
)

// bootstrapYAML is applied before the control plane starts. selfManaged keeps the
// deployer out of the measurement: the harness creates the gateway pods itself, so
// no Deployments, Services or ConfigMaps are reconciled per Gateway.
const bootstrapYAML = `kind: GatewayClass
apiVersion: gateway.networking.k8s.io/v1
metadata:
  name: kgateway
spec:
  controllerName: kgateway.dev/kgateway
  parametersRef:
    group: gateway.kgateway.dev
    kind: GatewayParameters
    name: kgateway
    namespace: default
---
kind: GatewayParameters
apiVersion: gateway.kgateway.dev/v1alpha1
metadata:
  name: kgateway
spec:
  selfManaged: {}
---
kind: Namespace
apiVersion: v1
metadata:
  name: gwtest
---
# The install namespace exists so the bootstrap controller can create its OAuth2
# HMAC secret once instead of retrying on a backoff loop for the whole run.
kind: Namespace
apiVersion: v1
metadata:
  name: kgateway-system
`

type config struct {
	Label  string `json:"label"`
	Commit string `json:"commit"`
	// Gateways is both the Gateway count and the connected-client count: one xDS
	// client per Gateway, each a distinct UniquelyConnectedClient because the role
	// carries the Gateway name.
	Gateways int `json:"gateways"`
	// Backends is the number of Services, which is the number of clusters in every
	// client's CDS payload.
	Backends int `json:"backends"`
	// EndpointsPerBackend sizes each ClusterLoadAssignment.
	EndpointsPerBackend int `json:"endpointsPerBackend"`
	// Zones is the number of distinct node localities. Gateway pods and endpoint
	// pods are spread over them, so clients in different zones resolve endpoints
	// differently and cannot share one interned CLA. Zones=1 measures the
	// best case for sharing; raise it for the realistic multi-zone case.
	Zones            int `json:"zones"`
	RoutesPerGateway int `json:"routesPerGateway"`
	// EndpointPods controls whether endpoints reference a backing Pod (one per zone,
	// shared). Without them endpoints carry no locality, which makes every client's
	// CLA identical regardless of Zones.
	EndpointPods bool `json:"endpointPods"`
	// Steps is how many client counts live heap is sampled at on the way up to
	// Gateways. More steps cost a convergence wait each but make the fitted slope
	// harder to fool.
	Steps         int `json:"steps"`
	ChurnRounds   int `json:"churnRounds"`
	ChurnBackends int `json:"churnBackends"`

	quiet    time.Duration
	timeout  time.Duration
	outDir   string
	parallel int
	// Idle detection: how many consecutive post-GC live-heap readings must agree,
	// how closely, and how long to wait between them.
	stableSamples   int
	stableTolerance float64
	stableInterval  time.Duration
}

func loadConfig(t *testing.T) config {
	c := config{
		Label:               envOr("XDSPERF_LABEL", "unlabeled"),
		Gateways:            envInt(t, "XDSPERF_GATEWAYS", 20),
		Backends:            envInt(t, "XDSPERF_BACKENDS", 500),
		EndpointsPerBackend: envInt(t, "XDSPERF_ENDPOINTS_PER_BACKEND", 4),
		Zones:               envInt(t, "XDSPERF_ZONES", 3),
		RoutesPerGateway:    envInt(t, "XDSPERF_ROUTES_PER_GATEWAY", 2),
		EndpointPods:        envBool(t, "XDSPERF_ENDPOINT_PODS", true),
		Steps:               envInt(t, "XDSPERF_STEPS", 4),
		ChurnRounds:         envInt(t, "XDSPERF_CHURN_ROUNDS", 3),
		ChurnBackends:       envInt(t, "XDSPERF_CHURN_BACKENDS", 50),

		quiet:    envDuration(t, "XDSPERF_QUIET", 5*time.Second),
		timeout:  envDuration(t, "XDSPERF_TIMEOUT", 10*time.Minute),
		outDir:   envOr("XDSPERF_OUT", ""),
		parallel: envInt(t, "XDSPERF_PARALLEL", 16),

		stableSamples:   envInt(t, "XDSPERF_STABLE_SAMPLES", 4),
		stableTolerance: float64(envInt(t, "XDSPERF_STABLE_TOLERANCE_PCT", 3)) / 100,
		stableInterval:  envDuration(t, "XDSPERF_STABLE_INTERVAL", 5*time.Second),
	}
	if c.Gateways < 2 {
		t.Fatalf("XDSPERF_GATEWAYS must be at least 2: the headline metric is the marginal heap of clients 2..N")
	}
	if c.Zones < 1 {
		c.Zones = 1
	}
	if c.Steps < 2 {
		c.Steps = 2
	}
	if c.Steps > c.Gateways {
		c.Steps = c.Gateways
	}
	if c.ChurnBackends > c.Backends {
		c.ChurnBackends = c.Backends
	}
	if c.outDir == "" {
		c.outDir = t.TempDir()
	}
	if err := os.MkdirAll(c.outDir, 0o755); err != nil {
		t.Fatalf("failed to create out dir %s: %v", c.outDir, err)
	}
	c.Commit = gitCommit()
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(t *testing.T, key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

func envBool(t *testing.T, key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return b
}

func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

type memSnapshot struct {
	HeapAllocBytes  uint64 `json:"heapAllocBytes"`
	HeapInuseBytes  uint64 `json:"heapInuseBytes"`
	HeapObjects     uint64 `json:"heapObjects"`
	StackInuseBytes uint64 `json:"stackInuseBytes"`
	SysBytes        uint64 `json:"sysBytes"`
	NumGC           uint32 `json:"numGC"`
	Goroutines      int    `json:"goroutines"`
}

type phase struct {
	WallSeconds float64 `json:"wallSeconds"`
	CPUSeconds  float64 `json:"cpuSeconds"`
	// AllocBytes and Mallocs are cumulative allocation during the phase, not live
	// heap. They are the clearest signal of GC pressure and of per-client work that
	// allocates and is then thrown away.
	AllocBytes uint64 `json:"allocBytes"`
	Mallocs    uint64 `json:"mallocs"`
	// XdsResources counts xDS resources the fleet received during the phase, so a
	// cheaper phase can be distinguished from a phase that simply delivered less.
	XdsResources int64 `json:"xdsResources"`
}

type marker struct {
	wall    time.Time
	cpu     float64
	alloc   uint64
	mallocs uint64
	res     int64
}

// heapSample is live heap with a given number of clients connected and converged.
type heapSample struct {
	Clients        int    `json:"clients"`
	HeapAllocBytes uint64 `json:"heapAllocBytes"`
	HeapObjects    uint64 `json:"heapObjects"`
}

// xdsMetrics is the delivery guard. Two builds are only comparable if they served
// the fleet the same resources; a "saving" that comes from serving fewer clusters
// is a bug, not a win.
type xdsMetrics struct {
	Responses             int64 `json:"responses"`
	ClusterResources      int64 `json:"clusterResources"`
	EndpointResources     int64 `json:"endpointResources"`
	MinClustersPerClient  int64 `json:"minClustersPerClient"`
	MinEndpointsPerClient int64 `json:"minEndpointsPerClient"`
	// ResourcesDelivered is cumulative over the whole run, across reconnects.
	ResourcesDelivered int64 `json:"resourcesDelivered"`
}

type result struct {
	Config     config `json:"config"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	GOGC       string `json:"gogc"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`

	// Baseline is the started-but-empty control plane.
	Baseline memSnapshot `json:"baselineHeap"`
	// Fixture is after every Gateway, Service, EndpointSlice, Pod and HTTPRoute
	// exists but before any client connects: the client-independent IR, which the
	// per-client work is layered on top of.
	Fixture memSnapshot `json:"fixtureHeap"`
	// OneClient is with exactly one client connected and converged.
	OneClient memSnapshot `json:"oneClientHeap"`
	// Steady is with all Gateways connected and converged.
	Steady memSnapshot `json:"steadyHeap"`
	// Drained is after the whole fleet disconnects again. It is the independent
	// check on the slope: whatever the fleet was holding has to be released here,
	// and a Drained that does not come back down to roughly Fixture means something
	// per-client is being retained after the client is gone.
	Drained memSnapshot `json:"drainedHeap"`

	// Samples is live heap at each measured client count. The slope is fitted over
	// these rather than taken between two of them, so one unlucky GC cannot set the
	// headline number by itself.
	Samples []heapSample `json:"samples"`

	// PerClientHeapBytes is the headline metric: the least-squares slope of live
	// heap against connected clients. A slope rather than a total cancels out
	// everything that does not scale with the client count — informers, the
	// client-independent IR, the harness's own fixed cost — and leaves the quantity
	// this comparison is about.
	PerClientHeapBytes float64 `json:"perClientHeapBytes"`
	// PerClientPerBackendBytes is that slope divided by the backend count, which is
	// the number to extrapolate to a real cluster's fleet and service count.
	PerClientPerBackendBytes float64 `json:"perClientPerBackendBytes"`
	// SlopeR2 is the fit quality. Below ~0.9 the samples are not on a line and the
	// slope should not be quoted: raise the client count, the backend count, or the
	// heap-stability requirement until it is.
	SlopeR2 float64 `json:"slopeR2"`
	// PerClientHeapFromDrain is (steady - drained) / clients, an estimate of the
	// same quantity from the other direction. It includes each client's fixed cost
	// (gRPC stream, goroutines, snapshot bookkeeping) where the slope also does, so
	// the two should land within noise of each other.
	PerClientHeapFromDrain float64 `json:"perClientHeapFromDrain"`

	Create phase `json:"create"`
	// Connect covers bringing the whole fleet up, measured across all steps.
	Connect phase `json:"connect"`
	// ChurnEndpoints rewrites EndpointSlices, which re-resolves endpoints for every
	// (client, backend) pair that references them.
	ChurnEndpoints phase `json:"churnEndpoints"`
	// ChurnReconnect drops and re-establishes the whole fleet, which forces
	// per-client translation of every backend for every client from scratch.
	ChurnReconnect phase `json:"churnReconnect"`

	Xds             xdsMetrics `json:"xds"`
	MaxRSSBytes     uint64     `json:"maxRSSBytes"`
	TotalAllocBytes uint64     `json:"totalAllocBytes"`
	HeapProfile     string     `json:"heapProfile"`
	ResultFile      string     `json:"resultFile"`
}

func TestXdsScaleFootprint(t *testing.T) {
	if os.Getenv("XDSPERF") == "" {
		t.Skip("set XDSPERF=1 to run the xDS fleet scale footprint measurement")
	}
	cfg := loadConfig(t)
	t.Logf("xds scale footprint: label=%s commit=%s gateways=%d backends=%d endpoints/backend=%d zones=%d",
		cfg.Label, cfg.Commit, cfg.Gateways, cfg.Backends, cfg.EndpointsPerBackend, cfg.Zones)

	// The default 512KiB sampling rate is too coarse to attribute a few MiB of live
	// heap. Lower it for attribution runs only: sampling every allocation costs CPU,
	// so profiles taken this way pair with CPU numbers from a default-rate run.
	if rate := envInt(t, "XDSPERF_MEMPROFILERATE", 0); rate > 0 {
		runtime.MemProfileRate = rate
		t.Logf("MemProfileRate=%d: heap attribution is finer, CPU numbers are not comparable to default-rate runs", rate)
	}

	// Each new stream sleeps before go-control-plane opens its first watch, to give
	// per-client translation a head start. That is a latency knob, not a memory one,
	// and it serializes badly against a fleet connecting at once; the harness waits
	// for convergence explicitly instead. Set it before the control plane starts:
	// the value is read once, lazily, on the first stream.
	t.Setenv("KGW_XDS_FIRST_CONNECT_DELAY", envOr("XDSPERF_FIRST_CONNECT_DELAY", "0"))
	// Never measure with the shared-proto tripwire armed: it re-hashes every shared
	// resource on every snapshot assembly, which is exactly the cost the sharing
	// exists to avoid, and it only exists on one of the two sides.
	t.Setenv("ASSERT_SHARED_PROTO_IMMUTABILITY", "false")

	st, err := envtestutil.BuildSettings()
	if err != nil {
		t.Fatalf("failed to build settings: %v", err)
	}
	// Log I/O is real CPU and its volume differs between builds; keep it out of the
	// measurement unless explicitly asked for.
	if os.Getenv("KGW_LOG_LEVEL") == "" {
		st.LogLevel = "error"
	}

	assetsDir, err := envtestassets.GetEnvTestAssetsDir()
	if err != nil {
		t.Fatalf("failed to get envtest assets dir: %v", err)
	}
	root := filepath.Join("..", "..", "..")
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(root, "pkg", "kgateway", "crds"),
			filepath.Join(root, "install", "helm", "kgateway-crds", "templates"),
		},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assetsDir,
		// Back-to-back A/B reps start an apiserver while the previous run's is still
		// exiting, and these runs deliberately cap GOMAXPROCS; the 20s default start
		// timeout is not enough for that on a loaded machine.
		ControlPlaneStartTimeout: envDuration(t, "XDSPERF_APISERVER_START_TIMEOUT", 2*time.Minute),
		ControlPlaneStopTimeout:  time.Millisecond,
	}
	// envtest defaults the service CIDR to 10.0.0.0/24, which runs out of ClusterIPs
	// at 254 Services and then fails every later Service create with "range is full".
	// Backend counts here are deliberately larger than that.
	testEnv.ControlPlane.GetAPIServer().Configure().Set("service-cluster-ip-range", envOr("XDSPERF_SERVICE_CIDR", "10.0.0.0/16"))

	bootstrap := filepath.Join(t.TempDir(), "bootstrap.yaml")
	if err := os.WriteFile(bootstrap, []byte(bootstrapYAML), 0o600); err != nil {
		t.Fatalf("failed to write bootstrap yaml: %v", err)
	}

	envtestutil.RunController(
		t,
		st,
		testEnv,
		nil,
		[][]string{{"default", bootstrap}},
		nil,
		func(t *testing.T, ctx context.Context, _ *krt.DebugHandler, client istiokube.CLIClient, xdsPort int) {
			measure(t, ctx, client, cfg, xdsPort)
		},
		nil,
	)
}

func measure(t *testing.T, ctx context.Context, client istiokube.CLIClient, cfg config, xdsPort int) {
	res := result{
		Config:     cfg,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		GOGC:       envOr("GOGC", "default(100)"),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
	}

	fleet := newFleet(t, ctx, cfg, xdsPort)
	defer fleet.stop()

	// Let the control plane finish its initial sync before the baseline, so the
	// baseline reflects an idle-but-started process rather than a starting one.
	time.Sleep(cfg.quiet)
	runtime.GC()
	runtime.GC()
	res.Baseline = snapshotHeap()
	t.Logf("baseline live heap (no fixture, no clients): %s", humanBytes(res.Baseline.HeapAllocBytes))

	// Phase 1: create the fixture, then wait for the control plane to go idle. No
	// client is connected, so nothing per-client exists yet; this heap is the
	// client-independent input the per-client work is built from.
	createStart := fleet.mark()
	createFixture(t, ctx, client, cfg)
	waitHeapStable(t, cfg)
	res.Create = fleet.since(createStart)
	runtime.GC()
	runtime.GC()
	res.Fixture = snapshotHeap()
	t.Logf("fixture live heap (%d backends, no clients): %s (+%s over baseline)",
		cfg.Backends, humanBytes(res.Fixture.HeapAllocBytes),
		//nolint:gosec // G115: heap sizes are far below the int64 range; the signed form is what a delta needs
		humanBytes(int64(res.Fixture.HeapAllocBytes)-int64(res.Baseline.HeapAllocBytes)))

	// Phase 2: bring the fleet up in steps, sampling live heap at each step. The
	// slope over these samples is the headline metric; the fixed cost of the first
	// client (and of the harness's own gRPC machinery) lands in the intercept
	// instead of contaminating the per-client number.
	connectStart := fleet.mark()
	connected := 0
	for _, target := range connectSteps(cfg) {
		fleet.connect(t, connected, target)
		connected = target
		fleet.waitConverged(t, cfg, connected)
		waitHeapStable(t, cfg)
		runtime.GC()
		runtime.GC()
		snap := snapshotHeap()
		res.Samples = append(res.Samples, heapSample{
			Clients:        connected,
			HeapAllocBytes: snap.HeapAllocBytes,
			HeapObjects:    snap.HeapObjects,
		})
		t.Logf("live heap with %d client(s): %s (+%s over fixture)",
			connected, humanBytes(snap.HeapAllocBytes),
			//nolint:gosec // G115: heap sizes are far below the int64 range; the signed form is what a delta needs
			humanBytes(int64(snap.HeapAllocBytes)-int64(res.Fixture.HeapAllocBytes)))
		if connected == 1 {
			res.OneClient = snap
		}
		if connected == cfg.Gateways {
			res.Steady = snap
		}
	}
	res.Connect = fleet.since(connectStart)

	res.PerClientHeapBytes, res.SlopeR2 = fitSlope(res.Samples)
	res.PerClientPerBackendBytes = res.PerClientHeapBytes / float64(cfg.Backends)
	t.Logf("steady live heap (%d clients): %s; slope %s/client (R2 %.3f), %.0fB/client/backend",
		cfg.Gateways, humanBytes(res.Steady.HeapAllocBytes),
		humanBytes(int64(res.PerClientHeapBytes)), res.SlopeR2, res.PerClientPerBackendBytes)

	res.HeapProfile = filepath.Join(cfg.outDir, fmt.Sprintf("heap-%s.pb.gz", cfg.Label))
	writeHeapProfile(t, res.HeapProfile)

	// Phase 3: endpoint churn. Rewriting an EndpointSlice re-resolves that backend's
	// endpoints for every connected client, which is the allocation path the CLA
	// sharing targets.
	if cfg.ChurnRounds > 0 && cfg.ChurnBackends > 0 {
		churnStart := fleet.mark()
		for round := 1; round <= cfg.ChurnRounds; round++ {
			churnEndpoints(t, ctx, client, cfg, round)
			fleet.waitQuiet(t, cfg, fmt.Sprintf("endpoint churn round %d", round))
		}
		res.ChurnEndpoints = fleet.since(churnStart)
	}

	// Phase 4: drain. Everything the fleet was holding must be released when it
	// disconnects, so this heap should come back to roughly the fixture heap. It is
	// also a second, independent estimate of what a client costs.
	fleet.disconnectAll()
	waitHeapStable(t, cfg)
	runtime.GC()
	runtime.GC()
	res.Drained = snapshotHeap()
	res.PerClientHeapFromDrain = (float64(res.Steady.HeapAllocBytes) - float64(res.Drained.HeapAllocBytes)) / float64(cfg.Gateways)
	t.Logf("drained live heap (0 clients): %s (fixture was %s); %s/client by drop-back",
		humanBytes(res.Drained.HeapAllocBytes), humanBytes(res.Fixture.HeapAllocBytes),
		humanBytes(int64(res.PerClientHeapFromDrain)))

	// Phase 5: reconnect churn. Bringing the whole fleet back forces every (client,
	// backend) pair to be produced again from scratch, which is the closest thing to
	// a control plane restart with a warm cluster.
	reconnectStart := fleet.mark()
	fleet.connect(t, 0, cfg.Gateways)
	fleet.waitConverged(t, cfg, cfg.Gateways)
	res.ChurnReconnect = fleet.since(reconnectStart)

	res.Xds = fleet.metrics()
	// The comparison is only meaningful if both sides served the same resources.
	if want := int64(cfg.Backends); res.Xds.MinClustersPerClient < want {
		t.Fatalf("a client received only %d clusters, want >= %d: the sides are not serving comparable config",
			res.Xds.MinClustersPerClient, want)
	}
	if res.Xds.MinEndpointsPerClient < int64(cfg.Backends) {
		t.Fatalf("a client received only %d endpoint resources, want >= %d: the sides are not serving comparable config",
			res.Xds.MinEndpointsPerClient, cfg.Backends)
	}

	res.MaxRSSBytes = maxRSSBytes()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	res.TotalAllocBytes = final.TotalAlloc
	res.ResultFile = filepath.Join(cfg.outDir, fmt.Sprintf("result-%s.json", cfg.Label))
	writeResult(t, res)
	report(t, res)
}

// connectSteps returns the client counts to sample live heap at, always starting at
// 1 (so the fixed per-client cost lands in the intercept) and always ending at the
// full fleet.
func connectSteps(cfg config) []int {
	steps := make([]int, 0, cfg.Steps)
	for k := range cfg.Steps {
		target := 1 + (cfg.Gateways-1)*k/(cfg.Steps-1)
		if len(steps) > 0 && target <= steps[len(steps)-1] {
			continue
		}
		steps = append(steps, target)
	}
	if steps[len(steps)-1] != cfg.Gateways {
		steps = append(steps, cfg.Gateways)
	}
	return steps
}

// fitSlope returns the least-squares slope of live heap against client count, in
// bytes per client, and the fit's R². R² is reported rather than assumed: samples
// that do not lie on a line mean the process was not actually at steady state, and
// the slope from them is not a per-client cost.
func fitSlope(samples []heapSample) (slope, r2 float64) {
	n := float64(len(samples))
	if n < 2 {
		return 0, 0
	}
	var sumX, sumY float64
	for _, s := range samples {
		sumX += float64(s.Clients)
		sumY += float64(s.HeapAllocBytes)
	}
	meanX, meanY := sumX/n, sumY/n
	var num, den float64
	for _, s := range samples {
		dx := float64(s.Clients) - meanX
		num += dx * (float64(s.HeapAllocBytes) - meanY)
		den += dx * dx
	}
	if den == 0 {
		return 0, 0
	}
	slope = num / den
	intercept := meanY - slope*meanX
	var ssRes, ssTot float64
	for _, s := range samples {
		predicted := intercept + slope*float64(s.Clients)
		actual := float64(s.HeapAllocBytes)
		ssRes += (actual - predicted) * (actual - predicted)
		ssTot += (actual - meanY) * (actual - meanY)
	}
	if ssTot == 0 {
		return slope, 0
	}
	return slope, 1 - ssRes/ssTot
}

// createFixture creates the nodes, backends, endpoints, gateways, gateway pods and
// routes. Order matters only in that everything a client's snapshot depends on must
// exist before the client connects.
func createFixture(t *testing.T, ctx context.Context, client istiokube.CLIClient, cfg config) {
	kube := client.Kube()
	gwapi := client.GatewayAPI()

	// Nodes carry the topology labels the control plane derives pod locality from.
	for z := range cfg.Zones {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName(z),
				Labels: map[string]string{
					corev1.LabelHostname:       nodeName(z),
					corev1.LabelTopologyRegion: "r0",
					corev1.LabelTopologyZone:   fmt.Sprintf("r0z%d", z),
				},
			},
		}
		if _, err := kube.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create node %s: %v", node.Name, err)
		}
	}

	// One backing Pod per zone, shared by every backend's endpoints. The control
	// plane reads an endpoint's Pod only for its locality and labels, so a pod per
	// endpoint would create tens of thousands of objects to express the same
	// topology — and the pod count itself is not what this harness measures.
	if cfg.EndpointPods {
		for z := range cfg.Zones {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      endpointPodName(z),
					Namespace: fixtureNamespace,
					Labels:    map[string]string{"app": "backend"},
				},
				Spec: corev1.PodSpec{
					NodeName:   nodeName(z),
					Containers: []corev1.Container{{Name: "app", Image: "app"}},
				},
			}
			if _, err := kube.CoreV1().Pods(fixtureNamespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil &&
				!apierrors.IsAlreadyExists(err) {
				t.Fatalf("failed to create endpoint pod %s: %v", pod.Name, err)
			}
		}
	}

	// Backends: one Service plus one EndpointSlice each, with endpoints spread over
	// the zones so a client's locality decides how its CLA comes out.
	forEachConcurrent(cfg.parallel, cfg.Backends, func(i int) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: serviceName(i), Namespace: fixtureNamespace},
			Spec: corev1.ServiceSpec{
				Ports:    []corev1.ServicePort{{Name: "http", Port: 8080, TargetPort: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP}},
				Selector: map[string]string{"app": serviceName(i)},
			},
		}
		if _, err := kube.CoreV1().Services(fixtureNamespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create service %s: %v", svc.Name, err)
		}

		endpoints := make([]discoveryv1.Endpoint, 0, cfg.EndpointsPerBackend)
		for j := range cfg.EndpointsPerBackend {
			ep := discoveryv1.Endpoint{
				Addresses:  []string{endpointAddress(i, j)},
				Conditions: discoveryv1.EndpointConditions{Ready: new(true)},
			}
			if cfg.EndpointPods {
				ep.TargetRef = &corev1.ObjectReference{
					Kind:      "Pod",
					Name:      endpointPodName(j % cfg.Zones),
					Namespace: fixtureNamespace,
				}
			}
			endpoints = append(endpoints, ep)
		}
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sliceName(i),
				Namespace: fixtureNamespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName(i)},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: new("http"), Port: new(int32(8080)), Protocol: new(corev1.ProtocolTCP)}},
			Endpoints:   endpoints,
		}
		if _, err := kube.DiscoveryV1().EndpointSlices(fixtureNamespace).Create(ctx, slice, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create endpointslice %s: %v", slice.Name, err)
		}
	})

	// Gateways, plus the pod each connecting client claims to be. The pod supplies
	// the client's locality and the gateway-name label the control plane uses to
	// build the client's local cluster.
	forEachConcurrent(cfg.parallel, cfg.Gateways, func(i int) {
		gw := &gwv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{Name: gatewayName(i), Namespace: fixtureNamespace},
			Spec: gwv1.GatewaySpec{
				GatewayClassName: "kgateway",
				Listeners: []gwv1.Listener{{
					Name:     "http",
					Protocol: gwv1.HTTPProtocolType,
					Port:     8080,
					AllowedRoutes: &gwv1.AllowedRoutes{
						Namespaces: &gwv1.RouteNamespaces{From: new(gwv1.NamespacesFromAll)},
					},
				}},
			},
		}
		if _, err := gwapi.GatewayV1().Gateways(fixtureNamespace).Create(ctx, gw, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create gateway %s: %v", gw.Name, err)
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      proxyPodName(i),
				Namespace: fixtureNamespace,
				Labels:    map[string]string{gatewayNameLabel: gatewayName(i)},
			},
			Spec: corev1.PodSpec{
				NodeName:   nodeName(i % cfg.Zones),
				Containers: []corev1.Container{{Name: "envoy", Image: "envoy"}},
			},
		}
		if _, err := kube.CoreV1().Pods(fixtureNamespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create proxy pod %s: %v", pod.Name, err)
		}
	})

	routes := cfg.Gateways * cfg.RoutesPerGateway
	forEachConcurrent(cfg.parallel, routes, func(i int) {
		route := &gwv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Name: routeName(i), Namespace: fixtureNamespace},
			Spec: gwv1.HTTPRouteSpec{
				CommonRouteSpec: gwv1.CommonRouteSpec{
					ParentRefs: []gwv1.ParentReference{{
						Name: gwv1.ObjectName(gatewayName(i % cfg.Gateways)),
					}},
				},
				Hostnames: []gwv1.Hostname{gwv1.Hostname(fmt.Sprintf("r%d.example.com", i))},
				Rules: []gwv1.HTTPRouteRule{{
					Matches: []gwv1.HTTPRouteMatch{{
						Path: &gwv1.HTTPPathMatch{
							Type:  new(gwv1.PathMatchPathPrefix),
							Value: new(fmt.Sprintf("/r%d", i)),
						},
					}},
					BackendRefs: []gwv1.HTTPBackendRef{{
						BackendRef: gwv1.BackendRef{
							BackendObjectReference: gwv1.BackendObjectReference{
								Name: gwv1.ObjectName(serviceName(i % cfg.Backends)),
								Port: new(gwv1.PortNumber(8080)),
							},
						},
					}},
				}},
			},
		}
		if _, err := gwapi.GatewayV1().HTTPRoutes(fixtureNamespace).Create(ctx, route, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("failed to create route %s: %v", route.Name, err)
		}
	})
	pods := cfg.Gateways
	if cfg.EndpointPods {
		pods += cfg.Zones
	}
	t.Logf("created %d services (%d endpoints each), %d gateways, %d routes, %d pods across %d zones",
		cfg.Backends, cfg.EndpointsPerBackend, cfg.Gateways, routes, pods, cfg.Zones)
}

// churnEndpoints rewrites the addresses of the first ChurnBackends EndpointSlices,
// which is an endpoint-only change: the clusters are untouched, so what it measures
// is the per-(client, backend) endpoint path.
func churnEndpoints(t *testing.T, ctx context.Context, client istiokube.CLIClient, cfg config, round int) {
	kube := client.Kube()
	forEachConcurrent(cfg.parallel, cfg.ChurnBackends, func(i int) {
		slice, err := kube.DiscoveryV1().EndpointSlices(fixtureNamespace).Get(ctx, sliceName(i), metav1.GetOptions{})
		if err != nil {
			t.Errorf("failed to get endpointslice %s: %v", sliceName(i), err)
			return
		}
		for j := range slice.Endpoints {
			// Rotate the last octet so every round is a real change.
			slice.Endpoints[j].Addresses = []string{churnAddress(i, j, round)}
		}
		if _, err := kube.DiscoveryV1().EndpointSlices(fixtureNamespace).Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
			t.Errorf("failed to update endpointslice %s: %v", slice.Name, err)
		}
	})
}

// ---------------------------------------------------------------------------
// xDS fleet
// ---------------------------------------------------------------------------

// xdsClient is one connected Envoy: an ADS stream that subscribes to CDS and, once
// it knows the cluster names, EDS — the same order and shape a real Envoy uses. It
// ACKs every response, because go-control-plane opens the next watch for a type only
// after the previous response is ACKed; a client that never ACKs would silently stop
// receiving updates and make every later phase look free.
type xdsClient struct {
	idx     int
	gateway string
	conn    *grpc.ClientConn
	stream  envoydiscoveryv3.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	cancel  context.CancelFunc
	node    *envoycorev3.Node

	sendMu sync.Mutex
	// subscribed EDS resource names, retained so every EDS ACK repeats them.
	edsNames []string

	clusters     atomic.Int64 // resources in the most recent CDS response
	endpoints    atomic.Int64 // resources in the most recent EDS response
	responses    atomic.Int64
	lastResponse atomic.Int64 // unix nanos
	// delivered is the fleet-wide cumulative resource counter, shared by every
	// client so it survives the reconnect phase replacing the client objects.
	delivered *atomic.Int64
	done      chan struct{}
	closed    atomic.Bool
}

type fleet struct {
	t         *testing.T
	ctx       context.Context
	cfg       config
	port      int
	clients   []*xdsClient
	delivered atomic.Int64
}

func newFleet(t *testing.T, ctx context.Context, cfg config, port int) *fleet {
	return &fleet{t: t, ctx: ctx, cfg: cfg, port: port, clients: make([]*xdsClient, cfg.Gateways)}
}

// connect brings up clients [from, to) concurrently and returns once every stream is
// open and its first request is sent.
func (f *fleet) connect(t *testing.T, from, to int) {
	var wg sync.WaitGroup
	for i := from; i < to; i++ {
		wg.Go(func() {
			c := f.dial(t, i)
			f.clients[i] = c
			go c.recvLoop(t)
		})
	}
	wg.Wait()
}

func (f *fleet) dial(t *testing.T, i int) *xdsClient {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", f.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// A fleet-sized snapshot is well past the 4MiB default.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024*1024)),
	)
	if err != nil {
		t.Fatalf("client %d failed to connect to xds server: %v", i, err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	stream, err := envoydiscoveryv3.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(ctx)
	if err != nil {
		cancel()
		t.Fatalf("client %d failed to open ads stream: %v", i, err)
	}
	c := &xdsClient{
		idx:       i,
		gateway:   gatewayName(i),
		conn:      conn,
		stream:    stream,
		cancel:    cancel,
		delivered: &f.delivered,
		done:      make(chan struct{}),
		node: &envoycorev3.Node{
			// The control plane resolves the pod from "<name>.<namespace>" to get the
			// client's locality and labels.
			Id: proxyPodName(i) + "." + fixtureNamespace,
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"role": structpb.NewStringValue(fmt.Sprintf("%s~%s~%s", rolePrefix, fixtureNamespace, gatewayName(i))),
				},
			},
		},
	}
	// CDS first, wildcard, exactly as Envoy does.
	c.send(t, clusterTypeURL, nil, "", "")
	return c
}

func (c *xdsClient) send(t *testing.T, typeURL string, names []string, version, nonce string) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.closed.Load() {
		return
	}
	err := c.stream.Send(&envoydiscoveryv3.DiscoveryRequest{
		Node:          c.node,
		TypeUrl:       typeURL,
		ResourceNames: names,
		VersionInfo:   version,
		ResponseNonce: nonce,
	})
	if err != nil {
		// A send failure on a client we are tearing down is expected; anything else
		// invalidates the run, so surface it without racing t.Fatalf off-goroutine.
		if !c.closed.Load() {
			t.Errorf("client %d failed to send %s request: %v", c.idx, typeURL, err)
		}
	}
}

func (c *xdsClient) recvLoop(t *testing.T) {
	defer close(c.done)
	for {
		resp, err := c.stream.Recv()
		if err != nil {
			return
		}
		n := int64(len(resp.GetResources()))
		c.responses.Add(1)
		c.delivered.Add(n)
		c.lastResponse.Store(time.Now().UnixNano())

		switch resp.GetTypeUrl() {
		case clusterTypeURL:
			c.clusters.Store(n)
			// Subscribe to endpoints for every cluster just learned, plus this
			// client's own local cluster. Envoy names the local cluster because its
			// static bootstrap declares it; the control plane withholds that resource
			// until it sees the name, so a harness that never asks for it would
			// measure a slightly different snapshot than production.
			names := make([]string, 0, len(resp.GetResources())+1)
			names = append(names, localClusterName(c.gateway))
			for _, raw := range resp.GetResources() {
				var cluster envoyclusterv3.Cluster
				if err := raw.UnmarshalTo(&cluster); err != nil {
					t.Errorf("client %d failed to unmarshal cluster: %v", c.idx, err)
					continue
				}
				if name := cluster.GetEdsClusterConfig().GetServiceName(); name != "" {
					names = append(names, name)
					continue
				}
				names = append(names, cluster.GetName())
			}
			c.sendMu.Lock()
			c.edsNames = names
			c.sendMu.Unlock()
			// ACK the clusters, then (re)subscribe endpoints.
			c.send(t, clusterTypeURL, nil, resp.GetVersionInfo(), resp.GetNonce())
			c.send(t, endpointTypeURL, names, "", "")
		case endpointTypeURL:
			c.endpoints.Store(n)
			c.sendMu.Lock()
			names := c.edsNames
			c.sendMu.Unlock()
			c.send(t, endpointTypeURL, names, resp.GetVersionInfo(), resp.GetNonce())
		default:
			c.send(t, resp.GetTypeUrl(), nil, resp.GetVersionInfo(), resp.GetNonce())
		}
	}
}

func (c *xdsClient) close() {
	c.closed.Store(true)
	c.cancel()
	c.conn.Close()
	<-c.done
}

func (f *fleet) disconnectAll() {
	for i, c := range f.clients {
		if c == nil {
			continue
		}
		c.close()
		f.clients[i] = nil
	}
	// The control plane drops a client's per-client state on stream close; give it
	// the chance to before the reconnect phase starts.
	time.Sleep(f.cfg.quiet)
}

func (f *fleet) stop() {
	for _, c := range f.clients {
		if c != nil {
			c.close()
		}
	}
}

// waitConverged waits until the first n clients have each received a full CDS and
// EDS payload and the fleet has then been quiet for cfg.quiet. Resource counts, not
// wall time, define convergence: a fleet that is still being served is not converged
// no matter how long it has been running.
func (f *fleet) waitConverged(t *testing.T, cfg config, n int) {
	want := int64(cfg.Backends)
	f.waitFor(t, cfg, func() (bool, string) {
		for i := range n {
			c := f.clients[i]
			if c == nil {
				return false, fmt.Sprintf("client %d not connected", i)
			}
			if got := c.clusters.Load(); got < want {
				return false, fmt.Sprintf("client %d has %d/%d clusters", i, got, want)
			}
			if got := c.endpoints.Load(); got < want {
				return false, fmt.Sprintf("client %d has %d/%d endpoint resources", i, got, want)
			}
		}
		return true, ""
	}, fmt.Sprintf("%d clients to receive %d clusters and endpoints", n, want))
}

// waitQuiet waits for the fleet to stop receiving responses.
func (f *fleet) waitQuiet(t *testing.T, cfg config, what string) {
	f.waitFor(t, cfg, func() (bool, string) { return true, "" }, what)
}

func (f *fleet) waitFor(t *testing.T, cfg config, pred func() (bool, string), what string) {
	deadline := time.Now().Add(cfg.timeout)
	for {
		ok, why := pred()
		if ok && time.Since(f.lastResponseAt()) >= cfg.quiet {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s (%s)", cfg.timeout, what, why)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (f *fleet) lastResponseAt() time.Time {
	var latest int64
	for _, c := range f.clients {
		if c == nil {
			continue
		}
		if v := c.lastResponse.Load(); v > latest {
			latest = v
		}
	}
	if latest == 0 {
		// Nothing received yet: treat as long quiet so a pred-only wait can pass.
		return time.Time{}
	}
	return time.Unix(0, latest)
}

// resourcesDelivered is cumulative over the run, not a sum over the current
// clients: the reconnect phase replaces every client, and a phase counter that reset
// with them would report negative deliveries for the phase that did the most work.
func (f *fleet) resourcesDelivered() int64 { return f.delivered.Load() }

func (f *fleet) metrics() xdsMetrics {
	m := xdsMetrics{MinClustersPerClient: -1, MinEndpointsPerClient: -1, ResourcesDelivered: f.delivered.Load()}
	for _, c := range f.clients {
		if c == nil {
			continue
		}
		m.Responses += c.responses.Load()
		m.ClusterResources += c.clusters.Load()
		m.EndpointResources += c.endpoints.Load()
		if v := c.clusters.Load(); m.MinClustersPerClient < 0 || v < m.MinClustersPerClient {
			m.MinClustersPerClient = v
		}
		if v := c.endpoints.Load(); m.MinEndpointsPerClient < 0 || v < m.MinEndpointsPerClient {
			m.MinEndpointsPerClient = v
		}
	}
	return m
}

func (f *fleet) mark() marker {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return marker{
		wall:    time.Now(),
		cpu:     cpuSeconds(),
		alloc:   ms.TotalAlloc,
		mallocs: ms.Mallocs,
		res:     f.resourcesDelivered(),
	}
}

func (f *fleet) since(m marker) phase {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return phase{
		WallSeconds:  time.Since(m.wall).Seconds(),
		CPUSeconds:   cpuSeconds() - m.cpu,
		AllocBytes:   ms.TotalAlloc - m.alloc,
		Mallocs:      ms.Mallocs - m.mallocs,
		XdsResources: f.resourcesDelivered() - m.res,
	}
}

// ---------------------------------------------------------------------------
// measurement primitives
// ---------------------------------------------------------------------------

// waitHeapStable waits for the process to actually go idle. A quiet xDS stream is
// not sufficient: the last snapshot push happens while the translations it triggered
// are still finishing, and that work produces no further responses. With a short
// window the measured heap is dominated by whatever is in flight, so the same binary
// reports wildly different numbers depending on how long the harness waits.
func waitHeapStable(t *testing.T, cfg config) {
	samples := make([]uint64, 0, cfg.stableSamples)
	deadline := time.Now().Add(cfg.timeout)
	for {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		samples = append(samples, ms.HeapAlloc)
		if len(samples) > cfg.stableSamples {
			samples = samples[1:]
		}
		if len(samples) == cfg.stableSamples {
			lo, hi := samples[0], samples[0]
			for _, s := range samples {
				lo = min(lo, s)
				hi = max(hi, s)
			}
			if lo > 0 && float64(hi-lo)/float64(lo) <= cfg.stableTolerance {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for live heap to stabilize (last samples: %v)", cfg.timeout, samples)
		}
		time.Sleep(cfg.stableInterval)
	}
}

func snapshotHeap() memSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return memSnapshot{
		HeapAllocBytes:  ms.HeapAlloc,
		HeapInuseBytes:  ms.HeapInuse,
		HeapObjects:     ms.HeapObjects,
		StackInuseBytes: ms.StackInuse,
		SysBytes:        ms.Sys,
		NumGC:           ms.NumGC,
		Goroutines:      runtime.NumGoroutine(),
	}
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 {
		return float64(t.Sec) + float64(t.Usec)/1e6
	}
	return tv(ru.Utime) + tv(ru.Stime)
}

// maxRSSBytes returns peak resident set size. ru_maxrss is bytes on darwin and
// kilobytes on linux.
func maxRSSBytes() uint64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return uint64(ru.Maxrss) //nolint:gosec // G115: ru_maxrss is a byte count and never negative
	}
	return uint64(ru.Maxrss) * 1024 //nolint:gosec // G115: ru_maxrss is a KiB count and never negative
}

func writeHeapProfile(t *testing.T, path string) {
	// FreeOSMemory forces a GC and returns free pages, so the profile reflects live
	// data rather than whatever the last GC cycle happened to leave behind.
	debug.FreeOSMemory()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create heap profile %s: %v", path, err)
	}
	defer f.Close()
	if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
		t.Fatalf("failed to write heap profile: %v", err)
	}
	t.Logf("heap profile: %s", path)
}

func writeResult(t *testing.T, res result) {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}
	if err := os.WriteFile(res.ResultFile, b, 0o600); err != nil {
		t.Fatalf("failed to write result: %v", err)
	}
	compact, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("failed to marshal compact result: %v", err)
	}
	// A single grep-able line so a driver script can collect runs without parsing
	// the whole log.
	fmt.Printf("XDSPERF_JSON %s\n", compact)
}

func report(t *testing.T, res result) {
	cfg := res.Config
	t.Logf("=== xds scale footprint (%s @ %s) ===", cfg.Label, cfg.Commit)
	t.Logf("  clients=%d backends=%d endpoints/backend=%d zones=%d gomaxprocs=%d",
		cfg.Gateways, cfg.Backends, cfg.EndpointsPerBackend, cfg.Zones, res.GOMAXPROCS)
	t.Logf("  live heap:     baseline %s -> fixture %s -> 1 client %s -> %d clients %s",
		humanBytes(res.Baseline.HeapAllocBytes), humanBytes(res.Fixture.HeapAllocBytes),
		humanBytes(res.OneClient.HeapAllocBytes), cfg.Gateways, humanBytes(res.Steady.HeapAllocBytes))
	for _, s := range res.Samples {
		t.Logf("    %4d clients: %s live heap, %d objects", s.Clients, humanBytes(s.HeapAllocBytes), s.HeapObjects)
	}
	t.Logf("  per client:    %s fitted slope, R2 %.3f (%.0f B per client per backend)",
		humanBytes(int64(res.PerClientHeapBytes)), res.SlopeR2, res.PerClientPerBackendBytes)
	t.Logf("  drain check:   %s/client from drop-back; drained to %s vs fixture %s",
		humanBytes(int64(res.PerClientHeapFromDrain)), humanBytes(res.Drained.HeapAllocBytes),
		humanBytes(res.Fixture.HeapAllocBytes))
	t.Logf("  fleet cost:    %s for %d clients beyond the first",
		//nolint:gosec // G115: heap sizes are far below the int64 range; the signed form is what a delta needs
		humanBytes(int64(res.Steady.HeapAllocBytes)-int64(res.OneClient.HeapAllocBytes)), cfg.Gateways-1)
	t.Logf("  heap inuse:    %s (secondary allocator-span metric)", humanBytes(res.Steady.HeapInuseBytes))
	t.Logf("  heap objects:  %d", res.Steady.HeapObjects)
	t.Logf("  peak rss:      %s", humanBytes(res.MaxRSSBytes))
	t.Logf("  total alloc:   %s", humanBytes(res.TotalAllocBytes))
	t.Logf("  create:        %.2fs cpu %s alloc", res.Create.CPUSeconds, humanBytes(res.Create.AllocBytes))
	t.Logf("  connect fleet: %.2fs cpu %s alloc (%d resources over %d steps)",
		res.Connect.CPUSeconds, humanBytes(res.Connect.AllocBytes), res.Connect.XdsResources, len(res.Samples))
	t.Logf("  endpoint churn:%.2fs cpu %s alloc (%d rounds x %d backends, %d resources)",
		res.ChurnEndpoints.CPUSeconds, humanBytes(res.ChurnEndpoints.AllocBytes),
		cfg.ChurnRounds, cfg.ChurnBackends, res.ChurnEndpoints.XdsResources)
	t.Logf("  reconnect:     %.2fs cpu %s alloc (%d resources)",
		res.ChurnReconnect.CPUSeconds, humanBytes(res.ChurnReconnect.AllocBytes), res.ChurnReconnect.XdsResources)
	t.Logf("  delivered:     %d cluster + %d endpoint resources in the final snapshot (min per client %d/%d)",
		res.Xds.ClusterResources, res.Xds.EndpointResources, res.Xds.MinClustersPerClient, res.Xds.MinEndpointsPerClient)
	t.Logf("  result: %s", res.ResultFile)
}

func humanBytes[T int64 | uint64](n T) string {
	f := float64(n)
	neg := ""
	if f < 0 {
		neg, f = "-", -f
	}
	switch {
	case f >= 1<<30:
		return fmt.Sprintf("%s%.2fGiB", neg, f/(1<<30))
	case f >= 1<<20:
		return fmt.Sprintf("%s%.1fMiB", neg, f/(1<<20))
	case f >= 1<<10:
		return fmt.Sprintf("%s%.1fKiB", neg, f/(1<<10))
	default:
		return fmt.Sprintf("%s%.0fB", neg, f)
	}
}

func serviceName(i int) string     { return fmt.Sprintf("svc-%04d", i) }
func sliceName(i int) string       { return fmt.Sprintf("svc-%04d-eps", i) }
func gatewayName(i int) string     { return fmt.Sprintf("gw-%03d", i) }
func proxyPodName(i int) string    { return fmt.Sprintf("gwproxy-%03d", i) }
func routeName(i int) string       { return fmt.Sprintf("route-%05d", i) }
func nodeName(z int) string        { return fmt.Sprintf("node-%d", z) }
func endpointPodName(z int) string { return fmt.Sprintf("ep-zone-%d", z) }

// localClusterName mirrors the control plane's <gateway>.<namespace> local cluster
// naming. Spelled out rather than imported to keep this file's dependencies to APIs
// that do not move between branches.
func localClusterName(gateway string) string { return gateway + "." + fixtureNamespace }

// endpointAddress lays out addresses so each backend's endpoints are distinct and
// stable across runs. 10.<backend/250>.<backend%250>.<endpoint>.
func endpointAddress(backend, endpoint int) string {
	return fmt.Sprintf("10.%d.%d.%d", backend/250, backend%250, endpoint+1)
}

// churnAddress moves an endpoint to a new address for round r, staying inside the
// same /24 so the change is an endpoint update rather than a topology change.
func churnAddress(backend, endpoint, round int) string {
	return fmt.Sprintf("10.%d.%d.%d", backend/250, backend%250, 100+round*10+endpoint)
}

func forEachConcurrent(parallel, n int, fn func(i int)) {
	if parallel < 1 {
		parallel = 1
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(i)
		})
	}
	wg.Wait()
}
