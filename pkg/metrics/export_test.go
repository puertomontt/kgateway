package metrics

// ResetScopedMetrics clears the gateway- and namespace-scoped registrations so a test
// can register its own metrics without seeing those of earlier tests.
func ResetScopedMetrics() {
	scopedLock.Lock()
	defer scopedLock.Unlock()

	gatewayScoped = nil
	namespaceScoped = nil
}
