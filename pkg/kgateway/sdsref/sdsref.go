// Package sdsref implements kgateway's SDS-backed certificate references.
//
// Certificate material normally reaches Envoy inline: kgateway reads a Kubernetes
// Secret and embeds the bytes in the listener or cluster it generates. Some
// environments do not permit private keys to live in the Kubernetes API at all.
// For those, a reference Secret can instead point at a Secret Discovery Service
// server (typically a SPIFFE/SPIRE agent) that serves the material to Envoy
// directly over a Unix domain socket:
//
//	apiVersion: v1
//	kind: Secret
//	metadata:
//	  name: spire-server-cert
//	type: gateway.kgateway.dev/sds
//	stringData:
//	  secretName: spiffe://example.org/ns/default/sa/gateway
//	  url: unix:///run/spire/sockets/agent.sock
//
// The reference holds no key material, only the SDS resource name to request and
// the socket to request it from. It is carried in a Secret rather than a
// dedicated CRD so that Gateway API `certificateRefs`, which admits only
// `kind: Secret` for Core conformance, can reference it unchanged. Envoy
// Gateway uses the same approach with its own type string.
package sdsref

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoytlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

const (
	// SecretType marks a Secret as an SDS reference rather than a carrier of
	// certificate material. Secrets of this type hold no key material.
	SecretType corev1.SecretType = "gateway.kgateway.dev/sds"

	// SecretNameKey is the Secret data key holding the SDS resource name that
	// Envoy requests from the server. For SPIRE this is typically a SPIFFE ID.
	SecretNameKey = "secretName"

	// ValidationContextNameKey is the Secret data key holding the SDS resource
	// name of the trust bundle. It is only needed where a single reference has
	// to supply both a certificate and a trust bundle, as with
	// BackendConfigPolicy's tls.secretRef; a listener names its CA through
	// caCertificateRefs instead, and that reference uses SecretNameKey.
	ValidationContextNameKey = "validationContextName"

	// URLKey is the Secret data key holding the SDS server location. Only
	// unix:// is supported; see Parse.
	URLKey = "url"

	// unixScheme is the only supported SDS server scheme. Reaching an SDS
	// server over the network additionally requires transport security and
	// authority configuration that this reference does not model, so a
	// network scheme is rejected rather than silently sent in plaintext.
	unixScheme = "unix://"

	// clusterNamePrefix prefixes every synthesized SDS cluster name, so SDS
	// clusters are recognizable in config dumps and Envoy stats.
	clusterNamePrefix = "sds_"

	// maxReadablePrefixLength bounds the human-readable portion of a
	// synthesized cluster name; uniqueness comes from the hash suffix.
	maxReadablePrefixLength = 48

	// sdsConnectTimeout is the connect timeout for synthesized SDS clusters.
	sdsConnectTimeout = 10 * time.Second
)

// IsRef reports whether the Secret is an SDS reference rather than a Secret
// carrying certificate material.
func IsRef(secret *ir.Secret) bool {
	return secret != nil && secret.Type == SecretType
}

// Parse reads an SDS reference out of a Secret of SecretType. It returns an
// error if the Secret is missing either key or names an unsupported scheme.
//
// Note that Parse deliberately performs no reachability check on the socket:
// the socket lives in the proxy pod, not in the control plane, so kgateway
// cannot tell whether it exists. A reference to a socket that is absent or
// serves nothing produces a listener or cluster whose TLS material never
// arrives; see BuildSecretConfig for how that failure surfaces.
func Parse(secret *ir.Secret) (*ir.SDSConfig, error) {
	name := strings.TrimSpace(string(secret.Data[SecretNameKey]))
	validationContext := strings.TrimSpace(string(secret.Data[ValidationContextNameKey]))
	if name == "" && validationContext == "" {
		return nil, fmt.Errorf("SDS reference secret %s/%s: at least one of %q or %q must be set",
			secret.Namespace, secret.Name, SecretNameKey, ValidationContextNameKey)
	}

	rawURL := strings.TrimSpace(string(secret.Data[URLKey]))
	if rawURL == "" {
		return nil, fmt.Errorf("SDS reference secret %s/%s: missing or empty %q", secret.Namespace, secret.Name, URLKey)
	}

	if !strings.HasPrefix(rawURL, unixScheme) {
		return nil, fmt.Errorf("SDS reference secret %s/%s: %q must use the %s scheme, got %q",
			secret.Namespace, secret.Name, URLKey, strings.TrimSuffix(unixScheme, "://"), rawURL)
	}

	path := strings.TrimPrefix(rawURL, unixScheme)
	if path == "" {
		return nil, fmt.Errorf("SDS reference secret %s/%s: %q has no socket path", secret.Namespace, secret.Name, URLKey)
	}

	return &ir.SDSConfig{
		SecretName:            name,
		ValidationContextName: validationContext,
		SocketPath:            path,
	}, nil
}

// ClusterName derives the name of the Envoy cluster used to reach the SDS
// server at socketPath. It is a pure function of the path so that every
// reference to the same socket resolves to a single shared cluster.
func ClusterName(socketPath string) string {
	hash := sha256.Sum256([]byte(socketPath))
	suffix := hex.EncodeToString(hash[:16])

	readable := strings.Trim(strings.ReplaceAll(socketPath, "/", "_"), "_")
	for len(readable) > maxReadablePrefixLength {
		_, size := utf8.DecodeLastRuneInString(readable)
		readable = readable[:len(readable)-size]
	}
	if readable == "" {
		return clusterNamePrefix + suffix
	}
	return fmt.Sprintf("%s%s_%s", clusterNamePrefix, readable, suffix)
}

// BuildCluster returns the Envoy cluster Envoy uses to reach the SDS server at
// socketPath.
//
// The cluster is delivered over CDS rather than written into the proxy
// bootstrap, so referencing an SDS server requires no change to the proxy
// deployment beyond mounting the socket. Envoy only requires a *statically*
// defined cluster for the management server it bootstraps against; an SDS
// subscription is created lazily when a listener or cluster referencing it is
// warmed, by which point this cluster has been delivered. Both Envoy Gateway
// and Istio synthesize their SDS clusters the same way.
func BuildCluster(socketPath string) *envoyclusterv3.Cluster {
	name := ClusterName(socketPath)
	return &envoyclusterv3.Cluster{
		Name:                 name,
		ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_STATIC},
		ConnectTimeout:       durationpb.New(sdsConnectTimeout),
		Http2ProtocolOptions: &envoycorev3.Http2ProtocolOptions{},
		LoadAssignment: &envoyendpointv3.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*envoyendpointv3.LocalityLbEndpoints{{
				LbEndpoints: []*envoyendpointv3.LbEndpoint{{
					HostIdentifier: &envoyendpointv3.LbEndpoint_Endpoint{
						Endpoint: &envoyendpointv3.Endpoint{
							Address: &envoycorev3.Address{
								Address: &envoycorev3.Address_Pipe{
									Pipe: &envoycorev3.Pipe{Path: socketPath},
								},
							},
						},
					},
				}},
			}},
		},
	}
}

// BuildSecretConfig returns the SdsSecretConfig that fetches resourceName from
// the SDS server at socketPath. A single reference can produce two of these —
// one for the certificate and one for the trust bundle — so the resource name is
// passed explicitly rather than read from the reference.
//
// InitialFetchTimeout is deliberately left unset, which means Envoy's default
// rather than "wait forever". A reference may name a resource the SDS server
// does not serve; blocking indefinitely would stall warming of the whole
// listener or cluster. The cost of that choice is that an unreachable SDS
// server yields a listener that activates without its certificate and fails
// handshakes, rather than one that never activates. Istio makes the same
// trade-off for the equivalent config, with the same reasoning.
func BuildSecretConfig(resourceName, socketPath string) *envoytlsv3.SdsSecretConfig {
	return &envoytlsv3.SdsSecretConfig{
		Name: resourceName,
		SdsConfig: &envoycorev3.ConfigSource{
			ResourceApiVersion: envoycorev3.ApiVersion_V3,
			ConfigSourceSpecifier: &envoycorev3.ConfigSource_ApiConfigSource{
				ApiConfigSource: &envoycorev3.ApiConfigSource{
					ApiType:                   envoycorev3.ApiConfigSource_GRPC,
					TransportApiVersion:       envoycorev3.ApiVersion_V3,
					SetNodeOnFirstMessageOnly: true,
					GrpcServices: []*envoycorev3.GrpcService{{
						TargetSpecifier: &envoycorev3.GrpcService_EnvoyGrpc_{
							EnvoyGrpc: &envoycorev3.GrpcService_EnvoyGrpc{
								ClusterName: ClusterName(socketPath),
							},
						},
					}},
				},
			},
		},
	}
}
