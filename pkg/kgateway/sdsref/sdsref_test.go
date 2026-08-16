package sdsref_test

import (
	"testing"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/sdsref"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

const testSocket = "/run/spire/sockets/agent.sock"

func refSecret(data map[string]string) *ir.Secret {
	byteData := make(map[string][]byte, len(data))
	for k, v := range data {
		byteData[k] = []byte(v)
	}
	return &ir.Secret{
		ObjectSource: ir.ObjectSource{Namespace: "default", Name: "sds-ref"},
		Type:         sdsref.SecretType,
		Data:         byteData,
	}
}

func TestIsRef(t *testing.T) {
	assert.True(t, sdsref.IsRef(refSecret(nil)), "a secret of the SDS type is a reference")
	assert.False(t, sdsref.IsRef(&ir.Secret{Type: corev1.SecretTypeTLS}), "a kubernetes.io/tls secret is not a reference")
	assert.False(t, sdsref.IsRef(&ir.Secret{}), "a secret with no type is not a reference")
	assert.False(t, sdsref.IsRef(nil), "a nil secret is not a reference")
}

func TestParse(t *testing.T) {
	tests := []struct {
		name   string
		data   map[string]string
		expect *ir.SDSConfig
		err    string
	}{
		{
			name: "certificate only",
			data: map[string]string{
				sdsref.SecretNameKey: "spiffe://example.org/ns/default/sa/gateway",
				sdsref.URLKey:        "unix://" + testSocket,
			},
			expect: &ir.SDSConfig{
				SecretName: "spiffe://example.org/ns/default/sa/gateway",
				SocketPath: testSocket,
			},
		},
		{
			name: "validation context only",
			data: map[string]string{
				sdsref.ValidationContextNameKey: "spiffe://example.org",
				sdsref.URLKey:                   "unix://" + testSocket,
			},
			expect: &ir.SDSConfig{
				ValidationContextName: "spiffe://example.org",
				SocketPath:            testSocket,
			},
		},
		{
			name: "both names",
			data: map[string]string{
				sdsref.SecretNameKey:            "spiffe://example.org/ns/default/sa/gateway",
				sdsref.ValidationContextNameKey: "spiffe://example.org",
				sdsref.URLKey:                   "unix://" + testSocket,
			},
			expect: &ir.SDSConfig{
				SecretName:            "spiffe://example.org/ns/default/sa/gateway",
				ValidationContextName: "spiffe://example.org",
				SocketPath:            testSocket,
			},
		},
		{
			name: "surrounding whitespace is trimmed",
			data: map[string]string{
				// stringData written as a YAML block scalar picks up a trailing newline.
				sdsref.SecretNameKey: "spiffe://example.org/ns/default/sa/gateway\n",
				sdsref.URLKey:        "  unix://" + testSocket + "\n",
			},
			expect: &ir.SDSConfig{
				SecretName: "spiffe://example.org/ns/default/sa/gateway",
				SocketPath: testSocket,
			},
		},
		{
			name: "no names at all",
			data: map[string]string{sdsref.URLKey: "unix://" + testSocket},
			err:  `SDS reference secret default/sds-ref: at least one of "secretName" or "validationContextName" must be set`,
		},
		{
			name: "empty name",
			data: map[string]string{
				sdsref.SecretNameKey: "   ",
				sdsref.URLKey:        "unix://" + testSocket,
			},
			err: `SDS reference secret default/sds-ref: at least one of "secretName" or "validationContextName" must be set`,
		},
		{
			name: "missing url",
			data: map[string]string{sdsref.SecretNameKey: "spiffe://example.org/ns/default/sa/gateway"},
			err:  `SDS reference secret default/sds-ref: missing or empty "url"`,
		},
		{
			name: "network scheme rejected",
			data: map[string]string{
				sdsref.SecretNameKey: "spiffe://example.org/ns/default/sa/gateway",
				sdsref.URLKey:        "https://sds.example.com:8443",
			},
			err: `SDS reference secret default/sds-ref: "url" must use the unix scheme, got "https://sds.example.com:8443"`,
		},
		{
			name: "url with no socket path",
			data: map[string]string{
				sdsref.SecretNameKey: "spiffe://example.org/ns/default/sa/gateway",
				sdsref.URLKey:        "unix://",
			},
			err: `SDS reference secret default/sds-ref: "url" has no socket path`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sdsref.Parse(refSecret(tc.data))
			if tc.err != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tc.err)
				assert.Nil(t, got, "no config should be returned alongside an error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expect, got)
		})
	}
}

func TestClusterName(t *testing.T) {
	name := sdsref.ClusterName(testSocket)

	assert.Equal(t, name, sdsref.ClusterName(testSocket), "cluster name must be a pure function of the socket path")
	assert.NotEqual(t, name, sdsref.ClusterName("/run/other/agent.sock"), "distinct sockets must not collide")
	assert.Contains(t, name, "run_spire_sockets_agent.sock", "the path should stay readable in the name")

	// A path long enough to be truncated still yields a unique, bounded name.
	longA := "/very/long/path/that/exceeds/the/readable/prefix/limit/by/quite/a/lot/a.sock"
	longB := "/very/long/path/that/exceeds/the/readable/prefix/limit/by/quite/a/lot/b.sock"
	assert.NotEqual(t, sdsref.ClusterName(longA), sdsref.ClusterName(longB),
		"truncation must not make two long paths collide, since the hash covers the whole path")
}

func TestBuildCluster(t *testing.T) {
	c := sdsref.BuildCluster(testSocket)

	assert.Equal(t, sdsref.ClusterName(testSocket), c.GetName())
	assert.Equal(t, envoyclusterv3.Cluster_STATIC, c.GetType(),
		"the socket is a fixed local path, so no discovery is involved")
	assert.NotNil(t, c.GetHttp2ProtocolOptions(), "SDS is a gRPC service and requires HTTP/2")
	assert.Equal(t, c.GetName(), c.GetLoadAssignment().GetClusterName())

	endpoints := c.GetLoadAssignment().GetEndpoints()
	require.Len(t, endpoints, 1)
	lbEndpoints := endpoints[0].GetLbEndpoints()
	require.Len(t, lbEndpoints, 1)
	assert.Equal(t, testSocket, lbEndpoints[0].GetEndpoint().GetAddress().GetPipe().GetPath(),
		"a unix socket is addressed as a pipe, not a socket_address")
}

func TestBuildSecretConfig(t *testing.T) {
	cfg := sdsref.BuildSecretConfig("spiffe://example.org/ns/default/sa/gateway", testSocket)

	assert.Equal(t, "spiffe://example.org/ns/default/sa/gateway", cfg.GetName(),
		"the resource name is what Envoy asks the SDS server for")

	apiConfig := cfg.GetSdsConfig().GetApiConfigSource()
	require.NotNil(t, apiConfig, "SDS must be fetched from the server, not over ADS")
	assert.Equal(t, envoycorev3.ApiConfigSource_GRPC, apiConfig.GetApiType())
	assert.Equal(t, envoycorev3.ApiVersion_V3, apiConfig.GetTransportApiVersion())
	assert.Equal(t, envoycorev3.ApiVersion_V3, cfg.GetSdsConfig().GetResourceApiVersion())
	assert.True(t, apiConfig.GetSetNodeOnFirstMessageOnly(),
		"the node identity only needs sending once per stream")

	require.Len(t, apiConfig.GetGrpcServices(), 1)
	assert.Equal(t, sdsref.ClusterName(testSocket), apiConfig.GetGrpcServices()[0].GetEnvoyGrpc().GetClusterName(),
		"the config source must name the synthesized transport cluster")

	assert.Nil(t, cfg.GetSdsConfig().GetInitialFetchTimeout(),
		"left at Envoy's default so a resource the server does not serve cannot stall listener warming")
}

func TestSDSConfigEquals(t *testing.T) {
	base := &ir.SDSConfig{SecretName: "cert", ValidationContextName: "ca", SocketPath: testSocket}

	assert.True(t, base.Equals(&ir.SDSConfig{SecretName: "cert", ValidationContextName: "ca", SocketPath: testSocket}))
	assert.False(t, base.Equals(&ir.SDSConfig{SecretName: "other", ValidationContextName: "ca", SocketPath: testSocket}))
	assert.False(t, base.Equals(&ir.SDSConfig{SecretName: "cert", ValidationContextName: "other", SocketPath: testSocket}))
	assert.False(t, base.Equals(&ir.SDSConfig{SecretName: "cert", ValidationContextName: "ca", SocketPath: "/other.sock"}))
	assert.False(t, base.Equals(nil))

	var nilCfg *ir.SDSConfig
	assert.True(t, nilCfg.Equals(nil))
	assert.False(t, nilCfg.Equals(base))
}
