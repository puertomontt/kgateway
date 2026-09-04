package backend

import (
	"context"
	"errors"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoyroutev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kgateway-dev/kgateway/v2/api/v1alpha1/kgateway"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/logging"
	sdk "github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/collections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/filters"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/reporter"
)

var logger = logging.New("plugin/backend")

const (
	ExtensionName = "backend"
)

var errAwsEc2DiscoveryDisabled = errors.New("aws ec2 discovery is disabled by controller settings")

// BackendIr is the intermediate representation of a kgateway.BackendSpec.
//
// It is exported so that a plugin owning a kind that embeds kgateway.BackendSpec can
// hold one and hand it back to ProcessForEnvoy; its fields stay unexported because
// everything a caller needs to do with it is done by this package. See constructor.go.
type BackendIr struct {
	awsIr            *AwsIr
	staticIr         *StaticIr
	dfpIr            *DfpIr
	gcpIr            *GcpIr
	priorityGroupsIr *PriorityGroupsIr
	errors           []error
}

// Errors returns the translation errors collected while constructing the IR. A
// caller reporting status for its own kind reports these as its own.
func (u *BackendIr) Errors() []error {
	if u == nil {
		return nil
	}
	return u.errors
}

func (u *BackendIr) Equals(other any) bool {
	otherBackend, ok := other.(*BackendIr)
	if !ok {
		return false
	}
	// AWS
	if !u.awsIr.Equals(otherBackend.awsIr) {
		return false
	}
	// Static
	if !u.staticIr.Equals(otherBackend.staticIr) {
		return false
	}
	// DFP
	if !u.dfpIr.Equals(otherBackend.dfpIr) {
		return false
	}
	// GCP
	if !u.gcpIr.Equals(otherBackend.gcpIr) {
		return false
	}
	// Priority groups
	if !u.priorityGroupsIr.Equals(otherBackend.priorityGroupsIr) {
		return false
	}
	if len(u.errors) != len(otherBackend.errors) {
		return false
	}
	for i := range u.errors {
		if !backendIRErrorEqual(u.errors[i], otherBackend.errors[i]) {
			return false
		}
	}
	return true
}

func backendIRErrorEqual(a, b error) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Error() == b.Error()
	}
}

func NewPlugin(ctx context.Context, commoncol *collections.CommonCollections) sdk.Plugin {
	col := NewBackendCollection(commoncol, "Backends")
	constructor := NewConstructor(commoncol, col)

	gk := wellknown.BackendGVK.GroupKind()
	bcol := krt.NewCollection(col, func(krtctx krt.HandlerContext, i *kgateway.Backend) *ir.BackendObjectIR {
		objSrc := ir.ObjectSource{
			Kind:      gk.Kind,
			Group:     gk.Group,
			Namespace: i.GetNamespace(),
			Name:      i.GetName(),
		}
		backendIR := constructor.ConstructIR(krtctx, objSrc, &i.Spec)
		if len(backendIR.errors) > 0 {
			logger.Error("failed to translate backend", "backend", i.GetName(), "error", errors.Join(backendIR.errors...))
		}
		backend := ir.NewBackendObjectIR(objSrc, 0, "", ExtensionName)
		backend.CanonicalHostname = CanonicalHostnameFor(&i.Spec)
		backend.AppProtocol = AppProtocolFor(&i.Spec)
		backend.Obj = i
		backend.ObjIr = backendIR
		backend.Errors = backendIR.errors

		// Parse common annotations
		ir.ParseObjectAnnotations(&backend, i)
		return &backend
	})
	ec2Endpoints := NewEc2EndpointsCollection(ctx, commoncol, bcol)
	return sdk.Plugin{
		ContributesBackends: map[schema.GroupKind]sdk.BackendPlugin{
			gk: {
				BackendInit: ir.BackendInit{
					InitEnvoyBackend: InitEnvoyBackend,
				},
				RawBackends:     col,
				Backends:        bcol,
				Endpoints:       ec2Endpoints.Endpoints,
				ExtraConditions: ec2Endpoints.DiscoveryStatus,
			},
		},
		ContributesPolicies: map[schema.GroupKind]sdk.PolicyPlugin{
			wellknown.BackendGVK.GroupKind(): {
				Name:                      "backend",
				NewGatewayTranslationPass: NewTranslationPass,
			},
		},
		ExtraHasSynced: ec2Endpoints.HasSynced,
	}
}

// InitEnvoyBackend is an ir.BackendInit hook that configures out from the
// kgateway.BackendSpec embedded in the backend object, whatever kind owns it. It is
// exported so a plugin owning such a kind can use it directly, or wrap it to add
// cluster configuration of its own.
func InitEnvoyBackend(_ context.Context, in ir.BackendObjectIR, out *envoyclusterv3.Cluster) *ir.EndpointsForBackend {
	spec := SpecOf(in.Obj)
	if spec == nil {
		logger.Error("failed to resolve backend spec", "backend", in.ResourceName())
		return nil
	}
	beIr := IrOf(in.ObjIr)
	if beIr == nil {
		logger.Error("failed to resolve backend ir", "backend", in.ResourceName())
		return nil
	}
	ProcessForEnvoy(spec, beIr, out)
	return nil
}

type backendPlugin struct {
	ir.UnimplementedProxyTranslationPass
	needsDfpFilter map[string]bool
	needsGcpAuthn  map[string]bool
}

var _ ir.ProxyTranslationPass = &backendPlugin{}

// NewTranslationPass returns the translation pass for the kgateway.BackendSpec
// fields of a backend object, whatever kind owns it. It is exported so a plugin
// owning a kind that embeds kgateway.BackendSpec can delegate to it for the embedded
// fields; a pass is per-gateway-translation state, so a caller composing it with its
// own must hold one instance per translation, not one per plugin.
func NewTranslationPass(_ ir.GwTranslationCtx, _ reporter.Reporter) ir.ProxyTranslationPass {
	return &backendPlugin{}
}

func (p *backendPlugin) Name() string {
	return ExtensionName
}

func (p *backendPlugin) ApplyForBackend(pCtx *ir.RouteBackendContext, in ir.HttpBackend, out *envoyroutev3.Route) error {
	spec := SpecOf(pCtx.Backend.Obj)
	if spec == nil {
		return nil
	}
	if spec.DynamicForwardProxy != nil {
		if p.needsDfpFilter == nil {
			p.needsDfpFilter = make(map[string]bool)
		}
		p.needsDfpFilter[pCtx.FilterChainName] = true
	}

	if spec.Gcp != nil {
		if p.needsGcpAuthn == nil {
			p.needsGcpAuthn = make(map[string]bool)
		}
		p.needsGcpAuthn[pCtx.FilterChainName] = true

		// Set host rewrite for GCP backends (only if not already set by another policy)
		routeAction := out.GetRoute()
		if routeAction == nil {
			routeAction = &envoyroutev3.RouteAction{}
			out.Action = &envoyroutev3.Route_Route{
				Route: routeAction,
			}
		}
		// Set auto host rewrite if not already configured
		if routeAction.GetHostRewriteSpecifier() == nil {
			routeAction.HostRewriteSpecifier = &envoyroutev3.RouteAction_AutoHostRewrite{
				AutoHostRewrite: &wrapperspb.BoolValue{Value: true},
			}
		}
	}

	return nil
}

// called 1 time per listener
// if a plugin emits new filters, they must be with a plugin unique name.
// any filter returned from route config must be disabled, so it doesnt impact other routes.
func (p *backendPlugin) HttpFilters(_ ir.HttpFiltersContext, fc ir.FilterChainCommon) ([]filters.StagedHttpFilter, error) {
	result := []filters.StagedHttpFilter{}

	var errs []error
	if p.needsDfpFilter[fc.FilterChainName] {
		pluginStage := filters.DuringStage(filters.OutAuthStage)
		f := filters.MustNewStagedFilter("envoy.filters.http.dynamic_forward_proxy", dfpFilterConfig, pluginStage)
		result = append(result, f)
	}
	if p.needsGcpAuthn[fc.FilterChainName] {
		pluginStage := filters.BeforeStage(filters.RouteStage)
		f := filters.MustNewStagedFilter(gcpAuthnFilterName, getGcpAuthnFilterConfig(), pluginStage)
		result = append(result, f)
	}
	return result, errors.Join(errs...)
}

// called 1 time (per envoy proxy). replaces GeneratedResources
func (p *backendPlugin) ResourcesToAdd() ir.Resources {
	resources := ir.Resources{}
	// Add GCP metadata cluster if any GCP backends are present
	if len(p.needsGcpAuthn) > 0 {
		resources.Clusters = []*envoyclusterv3.Cluster{getGcpAuthnCluster()}
	}
	return resources
}
