package backend

import (
	"testing"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/api/v1alpha1/kgateway"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/krtcollections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// backendSource is the ObjectSource the in-tree Backend kind supplies.
func backendSource(be *kgateway.Backend) ir.ObjectSource {
	gk := wellknown.BackendGVK.GroupKind()
	return ir.ObjectSource{
		Group:     gk.Group,
		Kind:      gk.Kind,
		Namespace: be.GetNamespace(),
		Name:      be.GetName(),
	}
}

// translateBackendForTest keeps the pre-seam call shape the translation tests in this
// package were written against: a function from a *kgateway.Backend to its IR.
func translateBackendForTest(
	col krt.Collection[*kgateway.Backend],
	secrets *krtcollections.SecretIndex,
	enableAwsEc2Discovery bool,
) func(krt.HandlerContext, *kgateway.Backend) *BackendIr {
	c := &Constructor{backends: col, secrets: secrets, enableAwsEc2Discovery: enableAwsEc2Discovery}
	return func(krtctx krt.HandlerContext, be *kgateway.Backend) *BackendIr {
		return c.ConstructIR(krtctx, backendSource(be), &be.Spec)
	}
}

func buildPriorityGroupsIrForTest(
	krtctx krt.HandlerContext,
	col krt.Collection[*kgateway.Backend],
	be *kgateway.Backend,
) (*PriorityGroupsIr, []error) {
	return buildPriorityGroupsIr(krtctx, col, backendSource(be), be.Spec.PriorityGroups)
}

// downstreamBackend stands in for a CRD in another repo whose spec embeds
// kgateway.BackendSpec and adds a field of its own.
type downstreamBackend struct {
	metav1.ObjectMeta
	Spec downstreamBackendSpec
}

type downstreamBackendSpec struct {
	kgateway.BackendSpec
	Custom string
}

func (b *downstreamBackend) BackendSpec() *kgateway.BackendSpec { return &b.Spec.BackendSpec }

var _ SpecSource = &downstreamBackend{}

// downstreamIr stands in for that CRD's IR, which embeds the in-tree one.
type downstreamIr struct {
	embedded *BackendIr
	custom   string
}

func (i *downstreamIr) BackendIr() *BackendIr { return i.embedded }

// Equals is what ir.BackendObjectIR requires of any ObjIr. A downstream IR compares
// its own fields and delegates the embedded half.
func (i *downstreamIr) Equals(other any) bool {
	o, ok := other.(*downstreamIr)
	if !ok {
		return false
	}
	return i.custom == o.custom && i.embedded.Equals(o.embedded)
}

var _ IrSource = &downstreamIr{}

// TestSeamTranslatesEmbeddedSpecUnderTheCallersIdentity is the whole point of the
// seam: a kind outside this package reuses the in-tree translation of the embedded
// spec, and every identity the in-tree code derives is the caller's own rather than
// gateway.kgateway.dev/Backend. Getting this wrong is what produced
// solo-io/gloo-gateway#2658 for the inlined TrafficPolicy fields.
func TestSeamTranslatesEmbeddedSpecUnderTheCallersIdentity(t *testing.T) {
	obj := &downstreamBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "by-tenant", Namespace: "gwtest"},
		Spec: downstreamBackendSpec{
			BackendSpec: kgateway.BackendSpec{
				Static: &kgateway.StaticBackend{
					Hosts: []kgateway.Host{{Host: "1.2.3.4", Port: gwv1.PortNumber(8080)}},
				},
			},
			Custom: "downstream-only",
		},
	}
	src := ir.ObjectSource{
		Group:     "enterprisekgateway.solo.io",
		Kind:      "EnterpriseBackend",
		Namespace: obj.Namespace,
		Name:      obj.Name,
	}

	c := &Constructor{}
	embedded := c.ConstructIR(krt.TestingDummyContext{}, src, SpecOf(obj))
	require.Empty(t, embedded.Errors(), "a static backend translates with no dependencies")

	objIr := &downstreamIr{embedded: embedded, custom: obj.Spec.Custom}

	// The seam resolves both halves without knowing either type.
	assert.Same(t, &obj.Spec.BackendSpec, SpecOf(obj))
	assert.Same(t, embedded, IrOf(objIr))

	// And the in-tree cluster configuration lands on the downstream kind's cluster.
	in := ir.NewBackendObjectIR(src, 8080, "", "enterprisebackend")
	in.Obj = obj
	in.ObjIr = objIr
	out := &envoyclusterv3.Cluster{}
	require.Nil(t, InitEnvoyBackend(t.Context(), in, out))
	assert.NotNil(t, out.GetLoadAssignment(), "static hosts become an inline load assignment")

	assert.Equal(t, "1.2.3.4", CanonicalHostnameFor(SpecOf(obj)))
	assert.Equal(t, ir.DefaultAppProtocol, AppProtocolFor(SpecOf(obj)))
}

// TestSeamIgnoresUnknownShapes keeps the resolvers from panicking on an object or IR
// that does not carry an embedded spec — a plugin registering the in-tree hooks for
// a kind that forgot the two methods should degrade, not crash.
func TestSeamIgnoresUnknownShapes(t *testing.T) {
	assert.Nil(t, SpecOf(nil))
	assert.Nil(t, SpecOf(&metav1.ObjectMeta{}))
	assert.Nil(t, IrOf(nil))
	assert.Nil(t, IrOf(struct{}{}))

	in := ir.NewBackendObjectIR(ir.ObjectSource{Kind: "Mystery"}, 0, "", "mystery")
	in.Obj = &metav1.ObjectMeta{}
	assert.Nil(t, InitEnvoyBackend(t.Context(), in, &envoyclusterv3.Cluster{}))
}

// TestPriorityGroupsWithoutACollectionSaysSo covers the arm a caller that does not
// expose priorityGroups reaches if its spec sets the field anyway.
func TestPriorityGroupsWithoutACollectionSaysSo(t *testing.T) {
	c := &Constructor{}
	beIr := c.ConstructIR(krt.TestingDummyContext{}, ir.ObjectSource{Namespace: "gwtest", Name: "pg"},
		&kgateway.BackendSpec{PriorityGroups: []kgateway.PriorityGroup{{}}})

	require.Len(t, beIr.Errors(), 1)
	assert.ErrorContains(t, beIr.Errors()[0], "priority groups are not supported by this backend kind")
	assert.True(t, c.HasSynced(), "a Constructor with no collection has nothing to wait for")
}
