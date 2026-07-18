# Typed permission migration

Permission pauses continue to expose the legacy `question`, `options: ["Approve", "Deny"]`, and
`allow_custom` fields. New clients should additionally read the nested `permission_request` object.
It carries stable snake-case values for risk, reason, effective mode and allowed decisions.

Phase 1 intentionally advertises only `allow_once` and `deny_once`. A legacy `Approve` is converted
to a one-shot grant keyed by the stable session id and consumed by the parked tool re-execution; it
cannot authorize another session or a later invocation. Hard-dangerous and configured always-ask
prompts are distinguished by `reason_code`. Explicit deny rules are evaluated before bypass and
return a denial rather than an overridable prompt.

Remembered session/workspace/global scopes, matcher-id validation, durable rule CRUD/CAS, and remote
policy propagation remain tracked by #601. Clients must not display those choices until Bamboo
includes them in `allowed_decisions`.
