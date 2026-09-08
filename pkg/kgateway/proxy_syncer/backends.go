package proxy_syncer

import (
	"cmp"
	"context"
	"hash/fnv"
	"maps"
	"slices"
	"sync"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"istio.io/istio/pkg/kube/krt"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/proxy_syncer/sharedproto"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/translator/irtranslator"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	krtutil "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/krtutil"
)

// baseEnvoyCluster is the prepared form of one backend: its client-invariant
// translation, together with the backend IR it was translated from. The Cluster
// proto is shared read-only by every client that targets this backend; per-client
// mutations clone it. One row per backend, keyed by cluster name.
//
// The row's equality covers two kinds of input on purpose. The emitted fields
// (ClusterVersion, Error, source identity) say whether the shared cluster
// changed. Backend says whether anything a per-client overlay may read changed,
// which includes Service metadata that never reaches the cluster proto: a label
// flipping whether the waypoint overlay applies, for example. Either must re-run
// the overlays, so both are compared here. Whether the overlay *output* changed
// is decided one collection later, by backendClusters.Equals, so an input-only
// change stops there.
type baseEnvoyCluster struct {
	// Name is both the Envoy cluster name and the KRT key; translation always
	// names the cluster (blackhole included) after BackendObjectIR.ClusterName(),
	// which is the name routes reference.
	Name string
	// Cluster is wrapped so consumers cannot mutate the proto shared across
	// every client snapshot; see package sharedproto. Content equality is
	// carried by ClusterVersion (a content hash over the proto plus, for
	// inline-CLA bases, the endpoint and policy inputs) together with Error.
	// +noKrtEquals
	Cluster        sharedproto.Shared[*envoyclusterv3.Cluster]
	ClusterVersion uint64
	// Error is the translation error for this backend, if any. Compared by message in
	// Equals because all errored clusters share one blackhole proto and baseClusterVersion
	// collapses every error to 0, so ClusterVersion can't tell error states apart.
	Error error
	// BackendSource identifies the Backend this cluster was translated from, for status attribution.
	BackendSource ir.ObjectSource
	// BackendGeneration is the observed generation of the source Backend.
	BackendGeneration int64
	// Backend is the IR this base was translated from, carried forward so the
	// overlay transform evaluates every client against exactly the inputs the
	// base saw rather than re-fetching a possibly newer IR.
	// +noKrtEquals compared through backendEquals
	Backend *ir.BackendObjectIR
	// Base is the non-proto portion of the base-translation result retained for
	// per-client processing. Base.Cluster is always nil: the only retained copy
	// of the shared proto lives behind Cluster, so future code cannot mutate it
	// through a raw *BaseCluster alias.
	// +noKrtEquals
	Base *irtranslator.BaseCluster
}

func (b baseEnvoyCluster) ResourceName() string { return b.Name }

func (b baseEnvoyCluster) Equals(in baseEnvoyCluster) bool {
	return b.Name == in.Name &&
		b.ClusterVersion == in.ClusterVersion &&
		b.BackendSource == in.BackendSource &&
		b.BackendGeneration == in.BackendGeneration &&
		errString(b.Error) == errString(in.Error) &&
		backendEquals(b.Backend, in.Backend)
}

// backendEquals compares two backend IRs by BackendObjectIR.Equals, which covers
// object metadata (labels and annotations) as well as the spec generation, so a
// metadata-only Service update re-runs the overlays that read it.
func backendEquals(a, b *ir.BackendObjectIR) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equals(*b)
}

// uccClusterDelta is a per-client cluster materialized only when at least one
// PerClientClusterOverlay returns non-nil for (ucc, backend), when the cluster
// needs an inline CLA (which is always per-client via PrioritizeEndpoints), or
// when strict-mode validation fails on the per-client cluster (the delta then
// carries the blackhole + error so the snapshot tracks it as errored for this
// UCC only — other UCCs may still see a valid cluster).
//
// The containing backendClusters row omits entries for the dominant case where
// no overlay applies. This keeps actual delta storage O(N*K), where K is the
// count of backends that genuinely vary per UCC, while the row's client snapshot
// disambiguates sparse absence.
type uccClusterDelta struct {
	Client ir.UniquelyConnectedClient
	Name   string
	// Cluster is wrapped so consumers cannot mutate the proto interned across
	// UCCs; see package sharedproto. Content equality is carried by
	// ClusterVersion (the proto content hash; errored rows hash the error
	// message instead) together with Error.
	// +noKrtEquals
	Cluster        sharedproto.Shared[*envoyclusterv3.Cluster]
	ClusterVersion uint64
	// Error participates in Equals by message.
	Error error
}

func (d uccClusterDelta) Equals(in uccClusterDelta) bool {
	return d.Client.Equals(in.Client) &&
		d.Name == in.Name &&
		d.ClusterVersion == in.ClusterVersion &&
		errString(d.Error) == errString(in.Error)
}

// uccClusterResourceName builds the per-client identity key for a uccWithCluster
// row. Deltas are not KRT rows — they live in a map keyed by UCC name inside
// backendClusters and need no key of their own — and the uccWithCluster rows
// that are (the status collection) are rebuilt per event, so there is nothing to
// cache the key on the way UccWithEndpoints does. Plain concatenation is still
// ~2.5x cheaper than fmt.Sprintf on these key shapes and allocates once instead
// of three times.
func uccClusterResourceName(client ir.UniquelyConnectedClient, name string) string {
	return client.ResourceName() + "/" + name
}

// clientInputSnapshot is one immutable view of the UCC collection, shared by
// pointer across every backend row evaluated against it. A row proves that a
// specific client was evaluated by pointing at a snapshot that contains that
// client's exact current identity; rows evaluated against the same client set
// point at the same snapshot, which is what backendClusters.Equals compares.
type clientInputSnapshot struct {
	// Clients is sorted by ResourceName so evaluation order — and therefore
	// which of several byte-identical per-client clusters gets interned first —
	// is stable across recomputes.
	Clients []ir.UniquelyConnectedClient
	// byName is derived from Clients and is immutable after construction.
	byName map[string]ir.UniquelyConnectedClient
}

func newClientInputSnapshot(clients []ir.UniquelyConnectedClient) *clientInputSnapshot {
	ordered := slices.Clone(clients)
	for i := range ordered {
		// UCCs are values except for Labels. Clone that map so a plugin cannot
		// mutate the resolution proof retained by every backend row. Clients and
		// byName below share these clones; both are immutable after construction.
		ordered[i].Labels = maps.Clone(ordered[i].Labels)
	}
	slices.SortFunc(ordered, func(a, b ir.UniquelyConnectedClient) int {
		return cmp.Compare(a.ResourceName(), b.ResourceName())
	})
	byName := make(map[string]ir.UniquelyConnectedClient, len(ordered))
	for _, client := range ordered {
		byName[client.ResourceName()] = client
	}
	return &clientInputSnapshot{
		Clients: ordered,
		byName:  byName,
	}
}

// matches reports whether clients is exactly the set this snapshot holds, by
// full UCC equality rather than by key.
func (s *clientInputSnapshot) matches(clients []ir.UniquelyConnectedClient) bool {
	if len(s.Clients) != len(clients) {
		return false
	}
	for _, client := range clients {
		evaluated, ok := s.byName[client.ResourceName()]
		if !ok || !evaluated.Equals(client) {
			return false
		}
	}
	return true
}

// ContainsCurrent reports whether this snapshot evaluated the exact current
// version of client. ResourceName alone is insufficient because fields that do
// not participate in the key, notably KnowsLocalCluster, affect overlays. A nil
// snapshot has evaluated nobody.
func (s *clientInputSnapshot) ContainsCurrent(client ir.UniquelyConnectedClient) bool {
	if s == nil {
		return false
	}
	evaluated, ok := s.byName[client.ResourceName()]
	return ok && evaluated.Equals(client)
}

// clientInputSnapshotInterner hands every backend transform in a batch the same
// snapshot pointer for the same client set, without inserting another KRT
// collection between UCC events and overlay recomputation. It retains only the
// most recently requested snapshot; older snapshots stay alive solely while
// backend rows still reference them during convergence.
type clientInputSnapshotInterner struct {
	mu     sync.Mutex
	latest *clientInputSnapshot
}

func (i *clientInputSnapshotInterner) intern(clients []ir.UniquelyConnectedClient) *clientInputSnapshot {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.latest != nil && i.latest.matches(clients) {
		return i.latest
	}
	i.latest = newClientInputSnapshot(clients)
	return i.latest
}

// backendClusters is the completed result for one backend: the shared base
// cluster, the sparse per-client overrides evaluated against it, and the exact
// client snapshot those overrides were evaluated with. Because the base and its
// overrides travel in one row, a reader can never combine a new base with
// overrides cloned from an older one. One row per backend, keyed by cluster
// name.
//
// The overrides are a cache, not the source of truth. A reader asking for a
// client the row has not evaluated computes that client's override itself, from
// the same base and inputs the row carries (see FetchClustersForClient); the row
// exists so that the dominant events — backend changes — pay for overlays once
// per backend rather than once per client.
//
// Equality is over emitted state only: an input change that leaves the base and
// every override byte-identical does not publish a new row.
type backendClusters struct {
	Name string
	// Cluster is the shared base, served to every client without an override.
	// Content equality is carried by ClusterVersion together with Error.
	// +noKrtEquals
	Cluster        sharedproto.Shared[*envoyclusterv3.Cluster]
	ClusterVersion uint64
	// Error is the base translation error, compared by message; see baseEnvoyCluster.
	Error error
	// BackendSource identifies the Backend this cluster was translated from, for status attribution.
	BackendSource ir.ObjectSource
	// BackendGeneration is the observed generation of the source Backend.
	BackendGeneration int64
	// Clients is the immutable UCC snapshot Deltas was evaluated against. A
	// client absent from Deltas but present in Clients was evaluated and needs
	// no override; a client absent from Clients has not been evaluated by this
	// row. Compared by identity: the interner returns one pointer per distinct
	// client set, so a moved client set is always a different pointer, and a
	// stale snapshot can never be retained as equal — which would withhold a
	// client's CDS with no event left to recover it.
	// +noKrtEquals compared by pointer identity
	Clients *clientInputSnapshot
	// Deltas contains only clients whose cluster genuinely differs from the base.
	// +noKrtEquals compared entry-wise
	Deltas map[string]uccClusterDelta
	// Backend and Base are the inputs Deltas was computed from, retained so a
	// reader can evaluate a client this row has not. They are inputs, not
	// emitted state, and stay out of Equals: a row whose inputs moved without
	// changing any output is kept, and its inputs are then at most one
	// propagation behind the base row — the same propagation that re-evaluates
	// the row against the client that asked.
	// +noKrtEquals input, not emitted state
	Backend *ir.BackendObjectIR
	// +noKrtEquals input, not emitted state
	Base *irtranslator.BaseCluster
}

func (b backendClusters) ResourceName() string { return b.Name }

func (b backendClusters) Equals(in backendClusters) bool {
	if b.Name != in.Name ||
		b.ClusterVersion != in.ClusterVersion ||
		b.BackendSource != in.BackendSource ||
		b.BackendGeneration != in.BackendGeneration ||
		errString(b.Error) != errString(in.Error) ||
		b.Clients != in.Clients ||
		len(b.Deltas) != len(in.Deltas) {
		return false
	}
	for client, delta := range b.Deltas {
		other, ok := in.Deltas[client]
		if !ok || !delta.Equals(other) {
			return false
		}
	}
	return true
}

// clientClusterView is one client's reading of one backendClusters row: the
// cluster that client should be served when the row has evaluated the client,
// or the row itself when it has not, so the reader can evaluate the client
// against the same base and inputs. FetchClustersForClient projects rows through
// it with krt.PartialFetch, so a change that concerns other clients only —
// another client's override appearing, say — does not retrigger this client's
// CDS assembly.
type clientClusterView struct {
	// Resolved is true when the row has evaluated this exact client and Cluster
	// is what it is served. When false, Row carries the inputs for evaluating it.
	Resolved bool
	Cluster  uccWithCluster
	// +noKrtEquals an unresolved view is never equal to anything; see Equals
	Row backendClusters
}

// Equals suppresses recomputation only between two resolved views of the same
// cluster. An unresolved view is never equal to anything: while the reader is
// evaluating a client itself, every change to the row must reach it.
func (v clientClusterView) Equals(in clientClusterView) bool {
	return v.Resolved && in.Resolved && v.Cluster.Equals(in.Cluster)
}

// forClient resolves one client against this row. A materialized delta wins on
// cluster, name and version; its error wins over a base error because a
// per-client failure (strict-mode validation of the post-overlay cluster) is
// the more specific signal — and base errors short-circuit the overlay loop, so
// a row with both is impossible in production. Backend identity always comes
// from the base, which is where the source Backend is tracked.
func (b backendClusters) forClient(client ir.UniquelyConnectedClient) clientClusterView {
	if !b.Clients.ContainsCurrent(client) {
		return clientClusterView{Row: b}
	}
	d, ok := b.Deltas[client.ResourceName()]
	return clientClusterView{Resolved: true, Cluster: b.serve(client, d, ok)}
}

// serve is the cluster client receives from this row given its override, if any.
func (b backendClusters) serve(client ir.UniquelyConnectedClient, d uccClusterDelta, hasDelta bool) uccWithCluster {
	out := uccWithCluster{
		Client:            client,
		Cluster:           b.Cluster,
		ClusterVersion:    b.ClusterVersion,
		Name:              b.Name,
		Error:             b.Error,
		BackendSource:     b.BackendSource,
		BackendGeneration: b.BackendGeneration,
	}
	if hasDelta {
		out.Cluster = d.Cluster
		out.ClusterVersion = d.ClusterVersion
		out.Name = d.Name
		if d.Error != nil {
			out.Error = d.Error
		}
	}
	return out
}

// applyOverlay evaluates one (client, backend) pair against a sealed base and
// returns the materialized override, or false when the client is served the
// shared base. It is the single place a per-client cluster is built, shared by
// the backend row transform (which passes an interner so identical overrides
// across clients share one proto) and by a reader evaluating a client the row
// has not (which passes none: that result lives only until the row catches up).
func applyOverlay(
	kctx krt.HandlerContext,
	ctx context.Context,
	translator *irtranslator.BackendTranslator,
	ucc ir.UniquelyConnectedClient,
	backend *ir.BackendObjectIR,
	base *irtranslator.BaseCluster,
	cluster sharedproto.Shared[*envoyclusterv3.Cluster],
	intern *sharedproto.Interner[*envoyclusterv3.Cluster],
) (uccClusterDelta, bool) {
	if base == nil || base.Error != nil {
		// Errored base: every UCC sees the same blackhole, no per-client
		// variation possible.
		return uccClusterDelta{}, false
	}
	// Lend ApplyPerClient the shared base proto rather than a copy of it. It
	// only reads this proto, and clones internally before letting an overlay
	// touch one, so it already performs the single clone a materialized delta
	// needs — and none at all on the dominant path where it returns nil
	// untouched. Cloning here to produce the *Cluster it takes would add a
	// per-backend deep copy on every recompute whose only purpose is to unseal
	// the base, which measures on the same order as the whole translation it
	// feeds. EndpointInputs, one field over, is already shared this way.
	perClientBase := *base
	perClientBase.Cluster = cluster.BorrowForRead()
	perClient, err := translator.ApplyPerClient(kctx, ctx, ucc, backend, &perClientBase)
	if err != nil {
		// Carry the error so the snapshot tracks this cluster as errored for
		// THIS UCC only. Falling back to the (valid) base would defeat
		// strict-mode validation; the user opted in to having broken configs
		// surface as errors rather than NACKs at the Envoy data plane.
		logger.Error("failed to apply per-client overlay",
			"backend", cluster.BorrowForRead().GetName(), "ucc", ucc.ResourceName(), "error", err)
		name := cluster.BorrowForRead().GetName()
		if perClient != nil {
			name = perClient.GetName()
		}
		return uccClusterDelta{
			Client: ucc,
			Name:   name,
			// Hash 0: errored rows are never published, so they opt out of
			// tripwire verification.
			Cluster:        sharedproto.WrapPrehashed(perClient, 0),
			ClusterVersion: utils.HashString(err.Error()),
			Error:          err,
		}, true
	}
	if perClient == nil {
		// No per-client variation: the client is served the shared base.
		return uccClusterDelta{}, false
	}
	clusterVersion := utils.HashProto(perClient)
	shared := sharedproto.WrapPrehashed(perClient, clusterVersion)
	if intern != nil {
		shared = intern.InternPrehashed(perClient, clusterVersion)
	}
	return uccClusterDelta{
		Client:         ucc,
		Name:           perClient.GetName(),
		Cluster:        shared,
		ClusterVersion: clusterVersion,
	}, true
}

// uccWithCluster is the resolved view returned by FetchClustersForClient: the
// cluster this client is served (base or delta) along with any translation
// error and the source Backend identity used for status attribution. It is also
// the row type of the status-only collection built by StatusClusters, where the
// Cluster and ClusterVersion fields are left zero because status does not read
// them.
type uccWithCluster struct {
	Client ir.UniquelyConnectedClient
	// Cluster is wrapped so snapshot assembly cannot mutate the proto shared
	// with other clients; the only exits are ResourceWithTTL (into the
	// envoycache snapshot, tripwire-verified) and Clone. Content equality is
	// carried by ClusterVersion, a content hash over the same proto.
	// +noKrtEquals
	Cluster        sharedproto.Shared[*envoyclusterv3.Cluster]
	ClusterVersion uint64
	Name           string
	Error          error
	// BackendSource identifies the Backend this cluster was translated from, for status attribution.
	BackendSource ir.ObjectSource
	// BackendGeneration is the observed generation of the source Backend.
	BackendGeneration int64
}

func (c uccWithCluster) ResourceName() string {
	return uccClusterResourceName(c.Client, c.Name)
}

func (c uccWithCluster) Equals(in uccWithCluster) bool {
	return c.Client.Equals(in.Client) &&
		c.ClusterVersion == in.ClusterVersion &&
		c.Name == in.Name &&
		c.BackendSource == in.BackendSource &&
		c.BackendGeneration == in.BackendGeneration &&
		errString(c.Error) == errString(in.Error)
}

// errString renders an error for comparison inside an Equals method, where a nil
// error and an empty message must compare equal.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// baseClusterVersion returns the equality hash for a base translation result.
// It folds the inline endpoints hash into the cluster proto hash when the cluster
// type supports an inline CLA: the per-client CLA is built from
// BaseCluster.EndpointInputs and is NOT part of the base proto, so without this
// the base would not re-publish when endpoints change — leaving clients pinned
// to stale LoadAssignments for non-EDS backends (e.g. ServiceEntry-style).
//
// The attached-policy hash is folded in for the same reason: EndpointInputs
// carries the backend's AttachedPolicies, which PerClientProcessEndpoints hooks
// consume when building the per-client CLA. KRT keeps the OLD stored object when
// Equals returns true, so any Base state consumed downstream but missing from
// this version would be served stale forever. This mirrors the EDS path, which
// folds backendEndpointVersionHash into LbEpsEqualityHash in
// newFinalBackendEndpoints.
//
// For EDS clusters EndpointInputs may also be non-nil, but those endpoints feed
// the separate EDS pipeline and are not used by ApplyPerClient; gating on
// SupportsInlineCLA keeps the version stable for the EDS case so equivalent
// translations do not churn the snapshot.
func baseClusterVersion(backend *ir.BackendObjectIR, b *irtranslator.BaseCluster) uint64 {
	if b.Error != nil {
		return 0
	}
	hasher := fnv.New64a()
	utils.HashProtoWithHasher(hasher, b.Cluster)
	if b.SupportsInlineCLA && b.EndpointInputs != nil {
		utils.HashUint64(hasher, b.EndpointInputs.EndpointsForBackend.LbEpsEqualityHash)
		utils.HashUint64(hasher, backendEndpointVersionHash(backend))
	}
	return hasher.Sum64()
}

// PerClientEnvoyClusters is the cluster half of per-client xDS, stored as one
// row per backend rather than one per (client, backend) pair:
//
//   - base holds each backend's client-invariant translation together with the
//     backend IR it came from. It exists so that client churn never re-runs
//     base translation.
//   - clusters holds each backend's completed result: the shared base, the
//     sparse per-client overrides evaluated against it, and the client snapshot
//     they were evaluated with. Readers consume only this collection.
//
// Whether a row has evaluated the requesting client is read off the row itself,
// which retains the exact UCC snapshot it was evaluated with; a client it has not
// evaluated is evaluated by the reader, from the same inputs. The UCC collection
// is deliberately not held here: the transform that reads these is driven by
// that collection, so KRT already hands it the current client, and fetching the
// parent again would register a second handler on it (see FetchClustersForClient).
//
// Consumers never read the fields directly: FetchClustersForClient resolves each
// row for one client, and StatusClusters projects the errors out for status.
// Construct with [NewPerClientEnvoyClusters].
type PerClientEnvoyClusters struct {
	base     krt.Collection[baseEnvoyCluster]
	clusters krt.Collection[backendClusters]
	// translator and ctx let FetchClustersForClient evaluate a client that a
	// row has not; they are the same values the row transform uses.
	translator *irtranslator.BackendTranslator
	ctx        context.Context
	// status is built once by the constructor. Deriving it on demand instead
	// would let a second caller stand up a duplicate collection over the same
	// inputs, which KRT has no way to flag.
	status krt.Collection[uccWithCluster]
}

// HasSynced reports whether both collections have synced. Publishing is not
// gated on this (the readiness gates were reverted in favor of the
// first-connect grace period, #14380); it exists for callers — currently tests —
// that need to wait for cluster translation to reach steady state.
func (iu *PerClientEnvoyClusters) HasSynced() bool {
	if iu.base != nil && !iu.base.HasSynced() {
		return false
	}
	if iu.clusters != nil && !iu.clusters.HasSynced() {
		return false
	}
	return true
}

// FetchClustersForClient returns one cluster per backend for a UCC: the client's
// own override where it has one, the shared base otherwise. Every row carries
// the base its overrides were cloned from, so there is no cross-row coherence to
// check, and nothing is ever withheld: a row that has not yet evaluated this
// exact client — it just connected, or its identity changed in place — is
// evaluated here, against the very base and inputs the row carries, so the
// client receives a complete CDS on the first pass. When the row catches up its
// override is byte-identical, so the cluster version and Envoy see nothing.
//
// A row evaluates a client the reader has already served twice over: once here,
// once in the row. That is the price of never holding a client back, paid only
// on connect and identity change, which are rare next to backend changes; a
// backend change re-runs exactly one row and is served from it directly.
//
// The *Cluster protos in the returned slice are shared with other UCCs (base) or
// interned across the UCCs whose overrides came out identical; callers MUST NOT
// mutate them. An empty result means there are no backend rows at all.
func (iu *PerClientEnvoyClusters) FetchClustersForClient(kctx krt.HandlerContext, ucc ir.UniquelyConnectedClient) []uccWithCluster {
	if iu.clusters == nil {
		return nil
	}
	// ucc is not re-fetched from the UCC collection. The calling transform is
	// keyed by that collection, and KRT passes it the parent's current stored
	// object on both the primary and the secondary-dependency path, so a keyed
	// FetchOne here could only ever disagree in the window between that lookup
	// and this call, when the queued primary event reruns the transform anyway.
	// It would also register a second handler on the UCC collection and
	// recompute every changed client twice.
	views := krt.PartialFetch(kctx, iu.clusters,
		func(b backendClusters) clientClusterView { return b.forClient(ucc) },
		clientClusterView.Equals,
	)
	out := make([]uccWithCluster, 0, len(views))
	for _, view := range views {
		if view.Resolved {
			out = append(out, view.Cluster)
			continue
		}
		row := view.Row
		d, ok := applyOverlay(kctx, iu.ctx, iu.translator, ucc, row.Backend, row.Base, row.Cluster, nil)
		out = append(out, row.serve(ucc, d, ok))
	}
	return out
}

// StatusClusters returns the status projection built by
// [NewPerClientEnvoyClusters]; see newStatusClusters for what it contains. The
// zero PerClientEnvoyClusters has none.
func (iu *PerClientEnvoyClusters) StatusClusters() krt.Collection[uccWithCluster] {
	return iu.status
}

// newStatusClusters builds the cluster view needed for fleet-wide Backend status
// attribution: one row per backend (carrying the source Backend identity and any
// UCC-invariant translation error) plus one row per errored per-client override
// (carrying the per-client translation error attributed to the same Backend).
// Only Name, Error, BackendSource, BackendGeneration — and Client on override
// rows — are populated; those are the fields GenerateBackendStatusReport
// consumes. Non-errored overrides contribute nothing to status and are skipped.
//
// This is a collection rather than a Fetch helper because backendStatusContributions
// indexes it by Backend: one client's cluster error then recomputes only its owning
// Backend's status, not every Backend's.
//
// Unlike FetchClustersForClient, no client-resolution check is applied. Status
// has no per-client coherence requirement, and the same UCC event that moves the
// client set recomputes the row, so at worst a departed client's error lingers
// for one propagation.
func newStatusClusters(
	krtopts krtutil.KrtOptions,
	clusters krt.Collection[backendClusters],
) krt.Collection[uccWithCluster] {
	if clusters == nil {
		return krt.NewStaticCollection[uccWithCluster](nil, nil, krtopts.ToOptions("BackendStatusClusters")...)
	}
	return krt.NewManyCollection(clusters, func(_ krt.HandlerContext, b backendClusters) []uccWithCluster {
		// The base row carries the zero UCC, whose ResourceName is empty; a connected
		// client's never is, so base and override rows cannot collide on the KRT key.
		out := []uccWithCluster{{
			Name:              b.Name,
			Error:             b.Error,
			BackendSource:     b.BackendSource,
			BackendGeneration: b.BackendGeneration,
		}}
		for _, d := range b.Deltas {
			if d.Error == nil {
				continue
			}
			out = append(out, uccWithCluster{
				Client:            d.Client,
				Name:              d.Name,
				Error:             d.Error,
				BackendSource:     b.BackendSource,
				BackendGeneration: b.BackendGeneration,
			})
		}
		return out
	}, krtopts.ToOptions("BackendStatusClusters")...)
}

// NewPerClientEnvoyClusters builds the collections that back
// [PerClientEnvoyClusters], translating every backend in finalBackends into an
// Envoy cluster for every client in uccs.
//
// The work is split so that the expensive part does not scale with the client
// count: each backend is translated once into a shared base, and each connected
// client is then offered a cheap overlay on top of it. Only the (client, backend)
// pairs whose cluster genuinely differs — a matching destination rule, a waypoint
// redirect, an inline CLA, a per-client validation failure — materialize a delta.
// For a fleet where few backends vary per client, storage and translation cost
// stay close to O(backends) instead of O(backends * clients).
//
// The chain is linear: base rows feed the overlay transform, whose rows carry
// the base they were evaluated against. A backend change therefore re-runs the
// overlays for that backend alone, against the base it just produced, and a
// reader never has to reconcile a base with overrides from another generation.
func NewPerClientEnvoyClusters(
	ctx context.Context,
	krtopts krtutil.KrtOptions,
	translator *irtranslator.BackendTranslator,
	finalBackends krt.Collection[*ir.BackendObjectIR],
	uccs krt.Collection[ir.UniquelyConnectedClient],
) PerClientEnvoyClusters {
	// Share immutable UCC snapshots across backend transforms without adding an
	// extra KRT propagation hop between a UCC event and overlay recomputation.
	clientInputs := &clientInputSnapshotInterner{}

	// Base clusters: one entry per backend, computed once and shared across all
	// UCCs. Anything that does not depend on the UCC lives here:
	// initializeCluster, InitEnvoyBackend, DNS lookup family, non-per-client
	// ProcessBackend hooks, gateway client certificate injection, and strict-mode
	// validation.
	base := krt.NewCollection(finalBackends, func(kctx krt.HandlerContext, backendObj *ir.BackendObjectIR) *baseEnvoyCluster {
		baseRes := translator.TranslateBackendBase(ctx, backendObj)
		if baseRes == nil {
			return nil
		}
		name := baseRes.Cluster.GetName()
		if name != backendObj.ClusterName() {
			// Routes reference this backend by its memoized ClusterName, and
			// the per-backend rows are keyed by the name the proto carries. A
			// renamed cluster would be served under a name nothing references.
			// Nothing in tree renames the cluster (initializeCluster and
			// buildBlackholeCluster both take the name from ClusterName, and no
			// plugin reassigns it); if that changes, drop this one backend
			// loudly rather than publish an unreachable cluster silently.
			logger.Error("backend translation renamed the cluster; dropping the backend",
				"backend", backendObj.ResourceName(),
				"expected", backendObj.ClusterName(), "got", name)
			return nil
		}
		clusterVersion := baseClusterVersion(backendObj, baseRes)
		sharedCluster := sharedproto.Wrap(baseRes.Cluster)
		// Seal the only retained raw alias. Per-client processing reconstructs a
		// temporary BaseCluster whose Cluster is borrowed from sharedCluster.
		baseRes.Cluster = nil
		var backendGeneration int64
		if backendObj.Obj != nil {
			backendGeneration = backendObj.Obj.GetGeneration()
		}
		return &baseEnvoyCluster{
			Name:              name,
			Cluster:           sharedCluster,
			ClusterVersion:    clusterVersion,
			Error:             baseRes.Error,
			BackendSource:     backendObj.GetObjectSource(),
			BackendGeneration: backendGeneration,
			Backend:           backendObj,
			Base:              baseRes,
		}
	}, krtopts.ToOptions("BaseEnvoyClusters")...)

	// Completed rows: the base plus the sparse per-client overrides evaluated
	// against it. Driven off base, so a backend change re-runs exactly this
	// backend's overlays against the base it just produced — including a
	// metadata-only change, which baseEnvoyCluster.Equals surfaces through the
	// carried IR — and a client change re-runs every backend's overlays against
	// the already-translated base without re-translating it. Most (client,
	// backend) pairs emit nothing, which is what keeps the rows sparse.
	clusters := krt.NewCollection(base, func(kctx krt.HandlerContext, b baseEnvoyCluster) *backendClusters {
		clientSnapshot := clientInputs.intern(krt.Fetch(kctx, uccs))
		out := &backendClusters{
			Name:              b.Name,
			Cluster:           b.Cluster,
			ClusterVersion:    b.ClusterVersion,
			Error:             b.Error,
			BackendSource:     b.BackendSource,
			BackendGeneration: b.BackendGeneration,
			Clients:           clientSnapshot,
			Backend:           b.Backend,
			Base:              b.Base,
		}
		if b.Error != nil || b.Base == nil {
			// Errored base: every UCC sees the same blackhole, no per-client
			// variation possible. The row still records which clients it
			// evaluated.
			return out
		}
		// Intern identical per-client clusters across UCCs. Inline-CLA backends
		// materialize a delta for every UCC, but UCCs that share the relevant
		// inputs often produce byte-identical clusters.
		var clusterInterner sharedproto.Interner[*envoyclusterv3.Cluster]
		for _, ucc := range clientSnapshot.Clients {
			d, ok := applyOverlay(kctx, ctx, translator, ucc, b.Backend, b.Base, b.Cluster, &clusterInterner)
			if !ok {
				continue
			}
			if out.Deltas == nil {
				out.Deltas = make(map[string]uccClusterDelta)
			}
			out.Deltas[ucc.ResourceName()] = d
		}
		return out
	}, krtopts.ToOptions("BackendEnvoyClusters")...)

	return PerClientEnvoyClusters{
		base:       base,
		clusters:   clusters,
		translator: translator,
		ctx:        ctx,
		status:     newStatusClusters(krtopts, clusters),
	}
}
