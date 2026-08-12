package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

func httpListener(name gwv1.SectionName, port gwv1.PortNumber, hostname *gwv1.Hostname) gwv1.Listener {
	return gwv1.Listener{
		Name:     name,
		Port:     port,
		Protocol: gwv1.HTTPProtocolType,
		Hostname: hostname,
	}
}

func httpRoute(namespace string, hostnames ...string) *ir.HttpRouteIR {
	return &ir.HttpRouteIR{
		ObjectSource: ir.ObjectSource{
			Group:     gwv1.GroupVersion.Group,
			Kind:      wellknown.HTTPRouteKind,
			Namespace: namespace,
			Name:      "route",
		},
		Hostnames: hostnames,
	}
}

func TestAttachOutcomeForListener(t *testing.T) {
	gw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "gw"},
	}

	tests := []struct {
		name        string
		route       ir.Route
		ref         gwv1.ParentReference
		listener    gwv1.Listener
		wantOutcome attachOutcome
		wantAttach  bool
		wantErr     bool
	}{
		{
			name:        "kind not allowed by listener",
			route:       &ir.TcpRouteIR{ObjectSource: ir.ObjectSource{Kind: wellknown.TCPRouteKind, Namespace: "gw-ns", Name: "route"}},
			ref:         gwv1.ParentReference{},
			listener:    httpListener("http", 80, nil),
			wantOutcome: attachOutcome{},
			wantAttach:  false,
		},
		{
			name:        "namespace not allowed by listener (defaults to same namespace)",
			route:       httpRoute("other-ns"),
			ref:         gwv1.ParentReference{},
			listener:    httpListener("http", 80, nil),
			wantOutcome: attachOutcome{},
			wantAttach:  false,
		},
		{
			name:        "port mismatch on parentRef",
			route:       httpRoute("gw-ns"),
			ref:         gwv1.ParentReference{Port: new(gwv1.PortNumber(81))},
			listener:    httpListener("http", 80, nil),
			wantOutcome: attachOutcome{allowedByListener: true},
			wantAttach:  false,
		},
		{
			name:        "sectionName mismatch on parentRef",
			route:       httpRoute("gw-ns"),
			ref:         gwv1.ParentReference{SectionName: new(gwv1.SectionName("other"))},
			listener:    httpListener("http", 80, nil),
			wantOutcome: attachOutcome{allowedByListener: true},
			wantAttach:  false,
		},
		{
			name:     "hostname mismatch",
			route:    httpRoute("gw-ns", "foo.com"),
			ref:      gwv1.ParentReference{},
			listener: httpListener("http", 80, new(gwv1.Hostname("bar.com"))),
			wantOutcome: attachOutcome{
				allowedByListener: true,
				listenerMatched:   true,
				hostnameChecked:   true,
				hostnameOK:        false,
			},
			wantAttach: false,
		},
		{
			name:     "full match with hostname intersection",
			route:    httpRoute("gw-ns", "foo.example.com"),
			ref:      gwv1.ParentReference{},
			listener: httpListener("http", 80, new(gwv1.Hostname("*.example.com"))),
			wantOutcome: attachOutcome{
				allowedByListener: true,
				listenerMatched:   true,
				hostnameChecked:   true,
				hostnameOK:        true,
				hostnames:         []string{"foo.example.com"},
			},
			wantAttach: true,
		},
		{
			name: "TCP route has no hostname check and defaults hostnameOK",
			route: &ir.TcpRouteIR{
				ObjectSource: ir.ObjectSource{Kind: wellknown.TCPRouteKind, Namespace: "gw-ns", Name: "route"},
			},
			ref: gwv1.ParentReference{},
			listener: gwv1.Listener{
				Name:     "tcp",
				Port:     80,
				Protocol: gwv1.TCPProtocolType,
			},
			wantOutcome: attachOutcome{
				allowedByListener: true,
				listenerMatched:   true,
				hostnameChecked:   false,
				hostnameOK:        true,
			},
			wantAttach: true,
		},
		{
			name: "GRPC route reuses HttpRouteIR and checks hostnames",
			route: &ir.HttpRouteIR{
				ObjectSource: ir.ObjectSource{Kind: wellknown.GRPCRouteKind, Namespace: "gw-ns", Name: "route"},
				Hostnames:    []string{"foo.com"},
			},
			ref:      gwv1.ParentReference{},
			listener: httpListener("http", 80, nil),
			wantOutcome: attachOutcome{
				allowedByListener: true,
				listenerMatched:   true,
				hostnameChecked:   true,
				hostnameOK:        true,
				hostnames:         []string{"foo.com"},
			},
			wantAttach: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &gatewayQueries{}
			outcome, err := r.attachOutcomeForListener(krt.TestingDummyContext{}, gw, tt.route, tt.ref, &tt.listener)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOutcome, outcome)
			assert.Equal(t, tt.wantAttach, outcome.attaches())
		})
	}
}

func TestAttachOutcomeForListenerErrorsOnInvalidSelector(t *testing.T) {
	gw := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "gw"}}
	fromSelector := gwv1.NamespacesFromSelector
	listener := gwv1.Listener{
		Name:     "http",
		Port:     80,
		Protocol: gwv1.HTTPProtocolType,
		AllowedRoutes: &gwv1.AllowedRoutes{
			Namespaces: &gwv1.RouteNamespaces{
				From:     &fromSelector,
				Selector: nil, // invalid: selector must be set when From=Selector
			},
		},
	}

	r := &gatewayQueries{}
	_, err := r.attachOutcomeForListener(krt.TestingDummyContext{}, gw, httpRoute("gw-ns"), gwv1.ParentReference{}, &listener)
	require.Error(t, err)
}
