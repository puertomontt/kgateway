package irtranslator

import (
	"errors"
	"testing"
	"time"

	envoyroutev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kgateway-dev/kgateway/v2/api/conditions"
	apisettings "github.com/kgateway-dev/kgateway/v2/api/settings"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	reportssdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/reporter"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

// namedPolicyIR is a policy IR distinguished by name.
type namedPolicyIR struct{ name string }

func (p *namedPolicyIR) CreationTime() time.Time { return time.Time{} }

func (p *namedPolicyIR) Equals(other any) bool {
	o, ok := other.(*namedPolicyIR)
	return ok && o.name == p.name
}

// recordingBackendObjectPolicyPass records the policies passed to ApplyForBackendObjectPolicies.
type recordingBackendObjectPolicyPass struct {
	ir.UnimplementedProxyTranslationPass
	applied [][]ir.PolicyAtt
	err     error
}

var _ ir.BackendObjectPolicyApplier = &recordingBackendObjectPolicyPass{}

func (p *recordingBackendObjectPolicyPass) ApplyForBackendObjectPolicies(pols []ir.PolicyAtt, pCtx *ir.RouteBackendContext) error {
	p.applied = append(p.applied, pols)
	pCtx.TypedFilterConfig.AddTypedConfig("test.filter", wrapperspb.String("set"))
	return p.err
}

func TestRunBackendObjectPolicies(t *testing.T) {
	gk := schema.GroupKind{Group: "test.kgateway.dev", Kind: "TestPolicy"}
	first, second := &namedPolicyIR{name: "first"}, &namedPolicyIR{name: "second"}
	mergeCalled := false
	merge := func([]ir.PolicyAtt) ir.PolicyAtt { mergeCalled = true; return ir.PolicyAtt{} }

	newTranslator := func(pass ir.ProxyTranslationPass) *httpRouteConfigurationTranslator {
		h := testHTTPRouteTranslator(nil, "")
		h.pluginPass = TranslationPassPlugins{gk: &TranslationPass{ProxyTranslationPass: pass, MergePolicies: merge}}
		return h
	}
	backendWith := func(pols ...ir.PolicyAtt) *ir.BackendObjectIR {
		return &ir.BackendObjectIR{AttachedPolicies: ir.AttachedPolicies{Policies: map[schema.GroupKind][]ir.PolicyAtt{gk: pols}}}
	}

	t.Run("passes the unmerged policies to the applier", func(t *testing.T) {
		mergeCalled = false
		pass := &recordingBackendObjectPolicyPass{}
		pCtx := &ir.RouteBackendContext{}
		pols := []ir.PolicyAtt{{PolicyIr: first}, {PolicyIr: second}}

		require.NoError(t, newTranslator(pass).runBackendObjectPolicies(backendWith(pols...), pCtx))
		require.Len(t, pass.applied, 1, "applier should be called once per policy kind")
		assert.Equal(t, pols, pass.applied[0], "applier should get the unmerged policies")
		assert.False(t, mergeCalled, "the translator should leave merging to the applier")
		assert.NotNil(t, pCtx.TypedFilterConfig.GetTypedConfig("test.filter"), "plugin output should land in the shared typed filter config")
	})

	t.Run("skips plugins that do not implement the applier", func(t *testing.T) {
		mergeCalled = false
		pCtx := &ir.RouteBackendContext{}

		require.NoError(t, newTranslator(ir.UnimplementedProxyTranslationPass{}).runBackendObjectPolicies(backendWith(ir.PolicyAtt{PolicyIr: first}), pCtx))
		assert.False(t, mergeCalled, "policies of plugins without the applier should not be merged")
		assert.Empty(t, pCtx.TypedFilterConfig, "no per-route config should be added")
	})

	t.Run("returns applier errors", func(t *testing.T) {
		pass := &recordingBackendObjectPolicyPass{err: errors.New("boom")}

		err := newTranslator(pass).runBackendObjectPolicies(backendWith(ir.PolicyAtt{PolicyIr: first}), &ir.RouteBackendContext{})
		assert.EqualError(t, err, "boom")
	})

	t.Run("ignores policy kinds without a translation pass", func(t *testing.T) {
		h := testHTTPRouteTranslator(nil, "")

		require.NoError(t, h.runBackendObjectPolicies(backendWith(ir.PolicyAtt{PolicyIr: first}), &ir.RouteBackendContext{}))
	})
}

// errBackendPass is a backend kind's translation pass whose ApplyForBackend fails.
type errBackendPass struct {
	ir.UnimplementedProxyTranslationPass
	err error
}

func (p *errBackendPass) ApplyForBackend(*ir.RouteBackendContext, ir.HttpBackend, *envoyroutev3.Route) error {
	return p.err
}

func TestEnvoyRoutesReplacesRouteOnBackendErrors(t *testing.T) {
	backendGK := schema.GroupKind{Group: "test.kgateway.dev", Kind: "TestBackend"}
	policyGK := schema.GroupKind{Group: "test.kgateway.dev", Kind: "TestPolicy"}

	routeTo := func(backend *ir.BackendObjectIR) ir.HttpRouteRuleMatchIR {
		in := testRouteIR(0, "/", "test-cluster")
		in.Backends[0].Backend.BackendObject = backend
		return in
	}

	tests := []struct {
		name    string
		passes  TranslationPassPlugins
		backend *ir.BackendObjectIR
		wantErr string
	}{
		{
			name:    "backend plugin error",
			passes:  TranslationPassPlugins{backendGK: &TranslationPass{ProxyTranslationPass: &errBackendPass{err: errors.New("backend boom")}}},
			backend: new(ir.NewBackendObjectIR(ir.ObjectSource{Group: backendGK.Group, Kind: backendGK.Kind, Name: "be"}, 0, "", "")),
			wantErr: "backend boom",
		},
		{
			name:   "backend object policy error",
			passes: TranslationPassPlugins{policyGK: &TranslationPass{ProxyTranslationPass: &recordingBackendObjectPolicyPass{err: errors.New("policy boom")}}},
			backend: &ir.BackendObjectIR{AttachedPolicies: ir.AttachedPolicies{Policies: map[schema.GroupKind][]ir.PolicyAtt{
				policyGK: {{PolicyIr: &namedPolicyIR{name: "first"}}},
			}}},
			wantErr: "policy boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testHTTPRouteTranslator(nil, apisettings.ValidationStandard)
			h.pluginPass = tt.passes
			report := &reports.ParentRefReport{}

			out := h.envoyRoutes(report, routeTo(tt.backend), "test-route")

			require.NotNil(t, out, "the route should be replaced, not dropped")
			assert.Equal(t, uint32(500), out.GetDirectResponse().GetStatus(), "the route should fail closed with a 500")
			cond := findCondition(report.Conditions, conditions.KgatewayConditionProgrammed)
			require.NotNil(t, cond, "the route should report the error")
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Equal(t, string(reportssdk.RouteRuleReplacedReason), cond.Reason)
			assert.Contains(t, cond.Message, tt.wantErr)
		})
	}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}
