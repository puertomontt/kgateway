package backend

import (
	"errors"
	"fmt"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoytlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	envoywellknown "github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/kube/krt"

	"github.com/kgateway-dev/kgateway/v2/api/v1alpha1/kgateway"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/krtcollections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/collections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// This file is the seam a plugin outside this package uses to reuse the in-tree
// translation of kgateway.BackendSpec for a kind of its own that embeds it.
//
// The alternative — a downstream plugin fabricating a *kgateway.Backend from its
// own object and handing that to in-tree code — is what produces the split-identity
// problem tracked in solo-io/gloo-gateway#2658: in-tree code that stamps its own
// GroupKind on anything it derives (a ReferenceGrant "from" identity, a log line, a
// status source) then attributes it to the wrong kind, and nothing about the resource
// the user actually wrote reveals which identity applies to which field.
//
// So nothing here takes an object. ConstructIR takes the caller's ir.ObjectSource and
// the embedded spec; every identity it derives is the caller's own.

// SpecSource is implemented by a backend object that embeds kgateway.BackendSpec,
// letting in-tree translation reach the embedded fields without knowing the kind.
//
//	func (b *EnterpriseBackend) BackendSpec() *kgateway.BackendSpec { return &b.Spec.BackendSpec }
type SpecSource interface {
	BackendSpec() *kgateway.BackendSpec
}

// IrSource is implemented by a backend IR that embeds a *BackendIr, letting in-tree
// translation reach the embedded IR without knowing the kind.
//
//	func (i *enterpriseBackendIr) BackendIr() *backend.BackendIr { return i.embedded }
type IrSource interface {
	BackendIr() *BackendIr
}

// SpecOf returns the kgateway.BackendSpec embedded in a backend object, or nil if
// the object neither is a *kgateway.Backend nor implements SpecSource.
func SpecOf(obj any) *kgateway.BackendSpec {
	switch t := obj.(type) {
	case *kgateway.Backend:
		return &t.Spec
	case SpecSource:
		return t.BackendSpec()
	}
	return nil
}

// IrOf returns the *BackendIr embedded in a backend IR, or nil if the IR neither is
// a *BackendIr nor implements IrSource.
func IrOf(objIr any) *BackendIr {
	switch t := objIr.(type) {
	case *BackendIr:
		return t
	case IrSource:
		return t.BackendIr()
	}
	return nil
}

// Constructor translates a kgateway.BackendSpec into a BackendIr. It is safe to
// share between plugins; it holds only read dependencies.
type Constructor struct {
	// backends resolves priorityGroups references. May be nil, in which case
	// priorityGroups references are reported as unresolvable.
	backends              krt.Collection[*kgateway.Backend]
	secrets               *krtcollections.SecretIndex
	enableAwsEc2Discovery bool
}

// NewBackendCollection builds a collection of in-tree Backends under the given KRT
// collection name. Exported so a plugin outside this package can supply the
// collection Constructor resolves priorityGroups references against; name must be
// unique across collections.
func NewBackendCollection(commoncol *collections.CommonCollections, name string) krt.Collection[*kgateway.Backend] {
	cli := kclient.NewFilteredDelayed[*kgateway.Backend](
		commoncol.Client,
		wellknown.BackendGVR,
		kclient.Filter{ObjectFilter: commoncol.Client.ObjectFilter()},
	)
	return krt.WrapClient(cli, commoncol.KrtOpts.ToOptions(name)...)
}

// NewConstructor returns a Constructor reading secrets and settings from commoncol.
// backends is the collection priorityGroups references resolve against, and may be
// nil for a caller that does not expose priorityGroups.
func NewConstructor(
	commoncol *collections.CommonCollections,
	backends krt.Collection[*kgateway.Backend],
) *Constructor {
	return &Constructor{
		backends:              backends,
		secrets:               commoncol.Secrets,
		enableAwsEc2Discovery: commoncol.Settings.EnableAwsEc2Discovery,
	}
}

// HasSynced reports whether the Constructor's own dependencies have synced.
func (c *Constructor) HasSynced() bool {
	if c.backends == nil {
		return true
	}
	return c.backends.HasSynced()
}

// ConstructIR translates spec into a BackendIr. src identifies the object the spec
// came from and is used for every derived identity — error messages, log lines and
// the namespace references resolve in — so a caller owning a different kind reports
// as its own kind rather than as Backend.
//
// Errors are collected on the returned BackendIr rather than returned, so that a
// partially translatable spec still produces the IR for the parts that did
// translate; read them with BackendIr.Errors.
func (c *Constructor) ConstructIR(
	krtctx krt.HandlerContext,
	src ir.ObjectSource,
	spec *kgateway.BackendSpec,
) *BackendIr {
	var beIr BackendIr
	if spec == nil {
		return &beIr
	}
	switch {
	case len(spec.PriorityGroups) > 0:
		pgIr, errs := buildPriorityGroupsIr(krtctx, c.backends, src, spec.PriorityGroups)
		beIr.priorityGroupsIr = pgIr
		beIr.errors = append(beIr.errors, errs...)
	case spec.Static != nil:
		staticIr, err := buildStaticIr(spec.Static)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
		}
		beIr.staticIr = staticIr
	case spec.DynamicForwardProxy != nil:
		dfpIr, err := buildDfpIr(spec.DynamicForwardProxy)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
		}
		beIr.dfpIr = dfpIr
	case spec.Aws != nil:
		c.constructAwsIR(krtctx, src, spec.Aws, &beIr)
	case spec.Gcp != nil:
		gcpIr, err := buildGcpIr(spec.Gcp)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
		}
		beIr.gcpIr = gcpIr
	}
	return &beIr
}

func (c *Constructor) constructAwsIR(
	krtctx krt.HandlerContext,
	src ir.ObjectSource,
	aws *kgateway.AwsBackend,
	beIr *BackendIr,
) {
	switch {
	case aws.Lambda != nil:
		region := defaultAwsRegion(aws.Region)
		invokeMode := getLambdaInvocationMode(aws)

		secret, err := c.loadAWSSecret(krtctx, src, aws)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}

		lambdaArn, err := buildLambdaARN(aws, region)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}

		endpointConfig, err := configureLambdaEndpoint(aws)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}

		var lambdaTransportSocket *envoycorev3.TransportSocket
		if endpointConfig.useTLS {
			// TODO(yuval-k): Add verification context
			typedConfig, err := utils.MessageToAny(&envoytlsv3.UpstreamTlsContext{
				Sni: endpointConfig.hostname,
			})
			if err != nil {
				beIr.errors = append(beIr.errors, err)
				return
			}
			lambdaTransportSocket = &envoycorev3.TransportSocket{
				Name: envoywellknown.TransportSocketTls,
				ConfigType: &envoycorev3.TransportSocket_TypedConfig{
					TypedConfig: typedConfig,
				},
			}
		}

		lambdaFilters, err := buildLambdaFilters(
			lambdaArn, region, aws.Auth, secret, invokeMode, aws.Lambda.PayloadTransformMode)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}

		beIr.awsIr = &AwsIr{
			lambdaIr: &LambdaIr{
				lambdaEndpoint:        endpointConfig,
				lambdaTransportSocket: lambdaTransportSocket,
				lambdaFilters:         lambdaFilters,
			},
		}
	case aws.Ec2 != nil:
		if !c.enableAwsEc2Discovery {
			beIr.errors = append(beIr.errors, errAwsEc2DiscoveryDisabled)
			return
		}
		secret, err := c.loadAWSSecret(krtctx, src, aws)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}
		ec2Ir, err := buildEc2Ir(aws, secret)
		if err != nil {
			beIr.errors = append(beIr.errors, err)
			return
		}
		beIr.awsIr = &AwsIr{ec2Ir: ec2Ir}
	}
}

// loadAWSSecret resolves the AWS auth secret. The lookup is same-namespace and
// deliberately does not consult a ReferenceGrant; src supplies the namespace and the
// identity reported on failure.
func (c *Constructor) loadAWSSecret(
	krtctx krt.HandlerContext,
	src ir.ObjectSource,
	aws *kgateway.AwsBackend,
) (*ir.Secret, error) {
	if aws.Auth == nil || aws.Auth.Type != kgateway.AwsAuthTypeSecret {
		return nil, nil
	}
	if aws.Auth.SecretRef == nil {
		return nil, fmt.Errorf("aws auth secretRef is required when type is %q", kgateway.AwsAuthTypeSecret)
	}
	if c.secrets == nil {
		return nil, errors.New("aws secret lookup is unavailable")
	}

	secretName := aws.Auth.SecretRef.Name
	secret, err := c.secrets.GetSecretWithoutRefGrant(krtctx, secretName, src.Namespace)
	if err != nil {
		logger.Error(
			"referenced AWS secret does not exist or could not be loaded",
			"kind", src.Kind,
			"backend", fmt.Sprintf("%s/%s", src.Namespace, src.Name),
			"secret", fmt.Sprintf("%s/%s", src.Namespace, secretName),
			"error", err,
		)
		return nil, err
	}
	return secret, nil
}

// ProcessForEnvoy fills out from an already-constructed BackendIr. It is the body of
// the in-tree InitEnvoyBackend hook, exported so a plugin owning a kind that embeds
// kgateway.BackendSpec installs the same cluster configuration for the embedded
// fields and adds its own on top.
//
// A nil spec or beIr is a no-op, as is a spec whose backend type did not translate.
func ProcessForEnvoy(spec *kgateway.BackendSpec, beIr *BackendIr, out *envoyclusterv3.Cluster) {
	if spec == nil || beIr == nil {
		return
	}
	// TODO: propagated error to CRD #11558.
	switch {
	case len(spec.PriorityGroups) > 0:
		if beIr.priorityGroupsIr == nil {
			return
		}
		processPriorityGroups(beIr.priorityGroupsIr, out)
	case spec.Static != nil:
		processStatic(beIr.staticIr, out)
	case spec.Aws != nil:
		if beIr.awsIr == nil {
			return
		}
		if err := processAws(beIr.awsIr, out); err != nil {
			logger.Error("failed to process aws backend", "error", err)
			beIr.errors = append(beIr.errors, err)
		}
	case spec.DynamicForwardProxy != nil:
		processDynamicForwardProxy(beIr.dfpIr, out)
	case spec.Gcp != nil:
		if err := processGcp(beIr.gcpIr, out); err != nil {
			logger.Error("failed to process gcp backend", "error", err)
			beIr.errors = append(beIr.errors, err)
		}
	}
}

// AppProtocolFor returns the app protocol the spec implies. Only static backends
// carry one.
func AppProtocolFor(spec *kgateway.BackendSpec) ir.AppProtocol {
	if spec != nil && spec.Static != nil && spec.Static.AppProtocol != nil {
		return ir.ParseAppProtocol(new(string(*spec.Static.AppProtocol)))
	}
	return ir.DefaultAppProtocol
}

// CanonicalHostnameFor returns the canonical hostname the spec implies, or the empty
// string when it implies none. Only static backends carry one.
func CanonicalHostnameFor(spec *kgateway.BackendSpec) string {
	if spec == nil || spec.Static == nil || len(spec.Static.Hosts) == 0 {
		return ""
	}
	return spec.Static.Hosts[0].Host
}
