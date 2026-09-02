package krtcollections

import (
	"testing"

	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/krt/krttest"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1b1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	apisettings "github.com/kgateway-dev/kgateway/v2/api/settings"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
)

const (
	wantSecretHint    = `Secrets are discovered by label (secretDiscoveryMode=LABELED); ensure it is labeled kgateway.dev/watch="true".`
	wantConfigMapHint = `ConfigMaps are discovered by label (configMapDiscoveryMode=LABELED); ensure it is labeled kgateway.dev/watch="true".`
)

// customSecretGK stands in for a Secret kind contributed by a plugin, which has its own watch
// and so is not affected by SecretDiscoveryMode.
var customSecretGK = schema.GroupKind{Group: "custom.example.com", Kind: "MySecret"}

func TestSecretIndexNotFoundHint(t *testing.T) {
	tests := []struct {
		name    string
		mode    apisettings.DiscoveryMode
		kind    schema.GroupKind
		wantErr string
	}{
		{
			name:    "ALL leaves the message unqualified",
			mode:    apisettings.DiscoveryAll,
			kind:    coreSecretGK,
			wantErr: "Secret ns/missing not found",
		},
		{
			name:    "LABELED explains that an unlabeled Secret looks missing",
			mode:    apisettings.DiscoveryLabeled,
			kind:    coreSecretGK,
			wantErr: "Secret ns/missing not found. " + wantSecretHint,
		},
		{
			name:    "LABELED does not blame the setting for a kind it does not filter",
			mode:    apisettings.DiscoveryLabeled,
			kind:    customSecretGK,
			wantErr: "MySecret ns/missing not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newSecretIndexForHintTest(t, tt.mode)

			_, err := idx.GetSecret(krt.TestingDummyContext{}, From{Namespace: "ns"}, gwv1.SecretObjectReference{
				Group: new(gwv1.Group(tt.kind.Group)),
				Kind:  new(gwv1.Kind(tt.kind.Kind)),
				Name:  "missing",
			})
			if err == nil {
				t.Fatal("GetSecret() expected a not-found error, got none")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("GetSecret() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfigMapIndexNotFoundHint(t *testing.T) {
	tests := []struct {
		name    string
		mode    apisettings.DiscoveryMode
		wantErr string
	}{
		{
			name:    "ALL leaves the message unqualified",
			mode:    apisettings.DiscoveryAll,
			wantErr: "ConfigMap ns/missing not found",
		},
		{
			name:    "LABELED explains that an unlabeled ConfigMap looks missing",
			mode:    apisettings.DiscoveryLabeled,
			wantErr: "ConfigMap ns/missing not found. " + wantConfigMapHint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := krttest.NewMock(t, nil)
			cfgmaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
			idx := NewConfigMapIndex(cfgmaps, newRefGrantIndexForHintTest(mock), tt.mode)
			cfgmaps.WaitUntilSynced(nil)

			_, err := idx.GetConfigMap(krt.TestingDummyContext{}, From{Namespace: "ns"}, gwv1.ObjectReference{
				Kind: "ConfigMap",
				Name: "missing",
			})
			if err == nil {
				t.Fatal("GetConfigMap() expected a not-found error, got none")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("GetConfigMap() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// newSecretIndexForHintTest builds an empty SecretIndex holding both the core Secret kind and
// a plugin-contributed one, so that every lookup misses and only the hint differs.
func newSecretIndexForHintTest(t *testing.T, mode apisettings.DiscoveryMode) *SecretIndex {
	t.Helper()

	mock := krttest.NewMock(t, nil)
	secretCol := krttest.GetMockCollection[*corev1.Secret](mock)
	toIR := func(kctx krt.HandlerContext, s *corev1.Secret) *ir.Secret {
		return &ir.Secret{
			ObjectSource: ir.ObjectSource{Kind: "Secret", Namespace: s.Namespace, Name: s.Name},
			Obj:          s,
			Data:         s.Data,
		}
	}
	secrets := map[schema.GroupKind]krt.Collection[ir.Secret]{
		coreSecretGK:   krt.NewCollection(secretCol, toIR),
		customSecretGK: krt.NewCollection(secretCol, toIR),
	}

	idx := NewSecretIndex(secrets, newRefGrantIndexForHintTest(mock), mode)
	secretCol.WaitUntilSynced(nil)
	for !idx.HasSynced() {
	}
	return idx
}

func newRefGrantIndexForHintTest(mock *krttest.MockCollection) *RefGrantIndex {
	refGrantCol := krttest.GetMockCollection[*gwv1b1.ReferenceGrant](mock)
	refGrantCol.WaitUntilSynced(nil)
	return NewRefGrantIndex(refGrantCol, apisettings.ReferenceGrantPermissive)
}
