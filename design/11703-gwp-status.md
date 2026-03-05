# EP-11703: GatewayParameters Reference Status

- Issue: [#11703](https://github.com/kgateway-dev/kgateway/issues/11703)

## Background

`GatewayParameters` previously had no resource-owned status that showed where the object was referenced.
At the same time, parameter reference failures were surfaced through `Gateway` status (`Accepted=False`, `InvalidParameters`), which tightly coupled reference handling to Gateway reconciliation behavior.

This EP changes direction:

- `GatewayParameters` status reports direct references from `Gateway` and `GatewayClass`
- `GatewayParameters` status does not perform object validation
- `Gateway` `InvalidParameters` behavior is preserved for deployer parameter-reference/configuration failures

## Motivation

Operators need to quickly answer: "what is currently pointing to this `GatewayParameters` object?"

A direct reference inventory on `GatewayParameters.status` gives this answer while keeping existing `Gateway` invalid-parameter signaling intact.

## Goals

- Add a structured `GatewayParameters.status.parents[]` field modeled after Gateway API route `parents` status style.
- Report direct references from:
  - `Gateway.spec.infrastructure.parametersRef`
  - `GatewayClass.spec.parametersRef`
- Include per-parent conditions to indicate direct reference presence.
- Implement a dedicated `GatewayParameters` reconciler that owns this status.
- Keep status updates idempotent, sorted, deduplicated, and conflict-safe.

## Non-Goals

- Validating `GatewayParameters` content in status.
- Emitting `Accepted=True/False` on `GatewayParameters`.
- Expanding inherited usage via `GatewayClass` into "effective Gateway usage."
- Reporting health outcomes of referenced consumers.

## Implementation Details

### API

Update `api/v1alpha1/kgateway/gateway_parameters_types.go`:

- `GatewayParametersStatus` now has:
  - `parents []gateway.networking.k8s.io/v1.RouteParentStatus`

Each parent entry includes:

- `parentRef`: points to either a `Gateway` or `GatewayClass`
- `controllerName`: current controller name
- `conditions`: includes `Referenced=True` with reason `DirectReference`

### Controller

Add `pkg/kgateway/controller/gateway_parameters.go` and register it in
`pkg/kgateway/controller/controller.go` via `NewBaseGatewayController`.

Reconciler behavior:

1. Watch `GatewayParameters` events.
2. Watch `Gateway` events and enqueue referenced `GatewayParameters` (direct references only).
3. Watch `GatewayClass` events and enqueue referenced `GatewayParameters`.
4. On reconcile, fetch current `GatewayParameters`.
5. Build desired `status.parents` from indexed direct references:
   - direct Gateway references
   - direct GatewayClass references
6. Sort and dedupe by parent key.
7. Update status with retry-on-conflict, skipping no-op updates.

### Deployer and Gateway Controller Behavior

Reference-reporting status on `GatewayParameters` is independent from `Gateway` status semantics:

- `pkg/kgateway/deployer/gateway_parameters.go` continues returning parameter-reference/configuration errors for invalid or missing references.
- `pkg/kgateway/controller/gw_controller.go` continues setting
  `Gateway Accepted=False Reason=InvalidParameters` for those failures, and restores `Accepted=True` when issues are resolved.
- `pkg/reports/status.go` keeps the `InvalidParameters` guard to avoid status races with reporter-written accepted conditions.

## Status Semantics

`GatewayParameters.status.parents[]` reports direct references only.

Reference examples:

- `Gateway` parent:
  - group: `gateway.networking.k8s.io`
  - kind: `Gateway`
  - namespace: `<gateway-namespace>`
  - name: `<gateway-name>`
- `GatewayClass` parent:
  - group: `gateway.networking.k8s.io`
  - kind: `GatewayClass`
  - name: `<gatewayclass-name>`

Condition shape per parent:

- type: `Referenced`
- status: `True`
- reason: `DirectReference`
- observedGeneration: current `GatewayParameters` generation

## Test Plan

Unit/integration coverage:

- `pkg/kgateway/controller/controller_test.go`:
  - verifies `GatewayParameters.status.parents` includes direct `Gateway` and `GatewayClass` references
  - verifies parents are pruned after reference resources are deleted
- `pkg/deployer/deployer_test.go`:
  - preserves invalid/missing reference error expectations for Gateway deployment path
- `test/e2e/features/deployer/suite.go`:
  - preserves missing `GatewayParameters` flow where `Gateway` is initially `Accepted=False` and recovers to `Accepted=True`

## Alternatives

- Keep validation-first `GatewayParameters Accepted` status model.
  - Rejected in this phase; it does not directly solve reference observability.
- Add only a scalar summary count (for example, total references) without parent details.
  - Rejected; a parent list is more actionable for operators.

## Follow-Ups

- Optional summary fields (for example, reference counts by kind) on top of `parents`.
- Optional filtering by controller ownership when mixed controllers coexist.
- Optional expansion of parent condition taxonomy if we later need richer reference metadata.
