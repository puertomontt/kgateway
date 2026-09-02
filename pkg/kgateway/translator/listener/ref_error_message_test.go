package listener

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kgateway-dev/kgateway/v2/pkg/krtcollections"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

// errInvalidTLSSecret mirrors sslutils.ErrInvalidTlsSecret, which is reported through a
// fmt.Errorf carrying two %w verbs.
var errInvalidTLSSecret = errors.New("invalid TLS secret")

func TestFormatRefErrorMessage(t *testing.T) {
	notFound := &krtcollections.NotFoundError{
		NotFoundObj: ir.ObjectSource{Kind: "Secret", Namespace: "ns", Name: "cert"},
	}
	hinted := &krtcollections.NotFoundError{
		NotFoundObj: ir.ObjectSource{Kind: "Secret", Namespace: "ns", Name: "cert"},
		Hint:        `Secrets are discovered by label (secretDiscoveryMode=LABELED); ensure it is labeled kgateway.dev/watch="true".`,
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
			err:  nil,
			want: "",
		},
		{
			name: "plain error keeps its own text",
			err:  errors.New("invalid TLS secret ns/cert"),
			want: "invalid TLS secret ns/cert",
		},
		{
			name: "bare not found is reworded",
			err:  notFound,
			want: "Secret ns/cert not found.",
		},
		{
			name: "not found keeps its hint",
			err:  hinted,
			want: `Secret ns/cert not found. Secrets are discovered by label (secretDiscoveryMode=LABELED); ensure it is labeled kgateway.dev/watch="true".`,
		},
		{
			name: "not found with no kind falls back to Resource",
			err:  &krtcollections.NotFoundError{NotFoundObj: ir.ObjectSource{Namespace: "ns", Name: "cert"}},
			want: "Resource ns/cert not found.",
		},
		{
			name: "wrapped not found keeps the wrapper's context",
			err:  fmt.Errorf("client certificate for listener https: %w", notFound),
			want: "client certificate for listener https: Secret ns/cert not found",
		},
		{
			name: "wrapped not found keeps the hint too",
			err:  fmt.Errorf("client certificate for listener https: %w", hinted),
			want: `client certificate for listener https: Secret ns/cert not found. Secrets are discovered by label (secretDiscoveryMode=LABELED); ensure it is labeled kgateway.dev/watch="true".`,
		},
		{
			// fmt.Errorf with several %w verbs also exposes Unwrap() []error, but its format
			// string is the message; taking it apart would drop the object it names.
			name: "multi-%w wrapper keeps its format string",
			err:  fmt.Errorf("%w %s/%s: %w", errInvalidTLSSecret, "ns", "bad-cert", errors.New("tls: failed to find any PEM data in key input")),
			want: "invalid TLS secret ns/bad-cert: tls: failed to find any PEM data in key input",
		},
		{
			name: "single-element join is still flattened",
			err:  errors.Join(notFound),
			want: "Secret ns/cert not found.",
		},
		{
			name: "every leaf of a joined error is reported",
			err:  errors.Join(notFound, errors.New("invalid TLS secret ns/bad-cert")),
			want: "Secret ns/cert not found.; invalid TLS secret ns/bad-cert",
		},
		{
			name: "nested joins are flattened",
			err: errors.Join(
				errors.Join(notFound, errors.New("first")),
				errors.New("second"),
			),
			want: "Secret ns/cert not found.; first; second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatRefErrorMessage(tt.err); got != tt.want {
				t.Errorf("FormatRefErrorMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}
