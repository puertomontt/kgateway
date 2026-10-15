package setup

import (
	"strings"
	"sync"

	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	xdsserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/genproto/googleapis/rpc/status"

	"github.com/kgateway-dev/kgateway/v2/pkg/kgateway/xds"
	"github.com/kgateway-dev/kgateway/v2/pkg/logging"
	"github.com/kgateway-dev/kgateway/v2/pkg/metrics"
)

const (
	gwNameLabel      = "gateway_name"
	gwNamespaceLabel = "gateway_namespace"
	typeURLLabel     = "type_url"
)

var (
	envoyLogger       = logging.New("xds/envoy")
	envoyXdsSubsystem = "envoy_xds"
	xdsRejectsTotal   = metrics.NewCounter(
		metrics.CounterOpts{
			Subsystem: envoyXdsSubsystem,
			Name:      "rejects_total",
			Help:      "Total number of xDS responses rejected by envoy proxy",
		}, []string{gwNamespaceLabel, gwNameLabel, typeURLLabel})
	xdsRejectsCurrent = metrics.NewGauge(
		metrics.GaugeOpts{
			Subsystem: envoyXdsSubsystem,
			Name:      "rejects_active",
			Help:      "Number of xDS responses currently rejected by envoy proxy",
		}, []string{gwNamespaceLabel, gwNameLabel, typeURLLabel})
)

func init() {
	// xdsRejectsCurrent is not scoped: its series are deleted when the last stream
	// rejecting a resource recovers or closes.
	metrics.RegisterGatewayScoped(xdsRejectsTotal, gwNamespaceLabel, gwNameLabel)
	metrics.RegisterNamespaceScoped(xdsRejectsTotal, gwNamespaceLabel)
}

type resourceKey struct {
	Namespace       string
	Name            string
	ResourceTypeUrl string
}

type resourceState struct {
	errors map[resourceKey]struct{}
}

func newResourceState() resourceState {
	return resourceState{
		errors: make(map[resourceKey]struct{}),
	}
}

type logNackCallback struct {
	xdsserver.CallbackFuncs
	streamState map[int64]resourceState
	// activeRejects counts, per resource, the streams currently rejecting it. It
	// mirrors xdsRejectsCurrent so the series can be deleted when it reaches zero.
	activeRejects map[resourceKey]int

	lock sync.Mutex
}

var _ xdsserver.Callbacks = (*logNackCallback)(nil)

func newLogNackCallback() *logNackCallback {
	return &logNackCallback{
		streamState:   make(map[int64]resourceState),
		activeRejects: make(map[resourceKey]int),
	}
}

// OnStreamClosed implements server.Callbacks.
func (l *logNackCallback) OnStreamClosed(streamID int64, node *envoycorev3.Node) {
	l.lock.Lock()
	streamState := l.streamState[streamID]
	delete(l.streamState, streamID)
	l.lock.Unlock()

	for k := range streamState.errors {
		l.onErrorGone(k)
	}
}

// OnStreamRequest implements server.Callbacks.
func (l *logNackCallback) OnStreamRequest(streamID int64, req *discoveryv3.DiscoveryRequest) error {
	// get gateway and typeURL from request
	role := req.GetNode().GetMetadata().GetFields()[xds.RoleKey].GetStringValue()
	parts := strings.SplitN(role, xds.KeyDelimiter, 3)
	if len(parts) != 3 {
		return nil
	}
	namespace := parts[1]
	name := parts[2]

	// note, with locality, name will include name~hash~ns
	if localityParts := strings.SplitN(name, xds.KeyDelimiter, 3); len(localityParts) == 3 {
		name = localityParts[0]
	}

	typeUrl := req.GetTypeUrl()
	key := resourceKey{
		Namespace:       namespace,
		Name:            name,
		ResourceTypeUrl: strings.TrimPrefix(typeUrl, "type.googleapis.com/"),
	}

	if req.ErrorDetail != nil {
		if !l.handleError(streamID, key) {
			// Log NACK only once per resource
			return nil
		}
		l.onNewError(key, req.ErrorDetail)
	} else {
		errorGone := l.handleNoError(streamID, key)
		if errorGone {
			l.onErrorGone(key)
		}
	}
	return nil
}

func (l *logNackCallback) onNewError(key resourceKey, err *status.Status) {
	labels := toLabels(key)
	xdsRejectsTotal.Inc(labels...)

	l.lock.Lock()
	l.activeRejects[key]++
	xdsRejectsCurrent.Set(float64(l.activeRejects[key]), labels...)
	l.lock.Unlock()

	envoyLogger.Warn("xds error", "gateway_name", key.Name, "gateway_ns", key.Namespace, "resource", key.ResourceTypeUrl, "error", err.Message)
}

func (l *logNackCallback) onErrorGone(key resourceKey) {
	l.lock.Lock()
	defer l.lock.Unlock()

	n := l.activeRejects[key] - 1
	if n > 0 {
		l.activeRejects[key] = n
		xdsRejectsCurrent.Set(float64(n), toLabels(key)...)
		return
	}
	delete(l.activeRejects, key)
	xdsRejectsCurrent.DeletePartialMatch(toLabels(key)...)
}

func (l *logNackCallback) handleNoError(streamID int64, key resourceKey) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	streamState := l.streamState[streamID]
	_, hadKey := streamState.errors[key]
	delete(streamState.errors, key)
	return hadKey
}

func (l *logNackCallback) handleError(streamID int64, key resourceKey) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	streamState := l.streamState[streamID]
	if streamState.errors == nil {
		streamState = newResourceState()
		l.streamState[streamID] = streamState
	}
	if _, exists := streamState.errors[key]; exists {
		return false
	}
	streamState.errors[key] = struct{}{}
	return true
}

func toLabels(key resourceKey) []metrics.Label {
	return []metrics.Label{
		{
			Name:  gwNamespaceLabel,
			Value: key.Namespace,
		},
		{
			Name:  gwNameLabel,
			Value: key.Name,
		},
		{
			Name:  typeURLLabel,
			Value: key.ResourceTypeUrl,
		},
	}
}
