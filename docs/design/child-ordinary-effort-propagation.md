# Local Child ordinary reasoning effort

Bamboo #1346 fills the existing local `BambooRuntime` RunSpec field. It introduces
no transport field or persistence protocol. External Claude/Codex and remote
actor adapters retain their existing behavior.

SubAgent creation already resolves explicit effort before the selected role's
model preference; Plan resolves its own planner preference. Both save the result
in the Child's `Session.reasoning_effort`. The actor adapter sends that field,
without deriving effort from a potentially inherited parent model reference.
Root ordinary effort and product Ultra do not supply a Child default.

Each Bamboo worker Run validates the supplied string against the canonical
ordinary enum (`none`, `low`, `medium`, `high`, `xhigh`, `max`) before activation
seeding or provider execution. Invalid values, including `ultra`, return the
static terminal error `invalid RunSpec reasoning_effort`. No supplied content is
included in that error. Accepted values are applied to the fresh activation
Session and actual ExecuteRequestBuilder. Each supported pooled-worker Run
installs its own effort, so a later Child cannot retain an earlier override.

Explicit `none` means Disabled and remains a concrete request value. Omission
means no per-call override: the existing provider's own default applies. The
current isolated worker provider configuration has an unset effort default;
therefore an omitted Run emits no reasoning parameter. This does not copy the
host's provider/default configuration into the worker.

Provider-specific mappings and parameter-removal fallbacks are unchanged. A
configured or persisted effort is not proof of the provider request. This slice
does not claim native Ultra or support for every effort on every provider.

## Acceptance evidence

`tests/child_ordinary_effort.rs` uses the production Root route, Plan scheduler,
local actor adapter and same-source `CARGO_BIN_EXE_bamboo` child, replacing only
the remote model with a loopback SSE endpoint. It observes Root High and a
distinct planner model's Low at the actual HTTP provider boundary. The existing
Plan workspace selector is supplied explicitly (#1347 remains separate).

The same test also provisions one real reusable Bamboo worker and connects for
successive Runs of independent logical Children using Low, High, Disabled and
omission. It verifies the exact live PID stays unchanged, the actual wire updates
on each Run, omission clears the old override, and invalid/Ultra values cause no
provider request. Worker unit
coverage additionally verifies all six ordinary values, invalid canonical
strings, fresh persistence and unchanged Child/Standard identity. Historical
#1345 default-based evidence remains unchanged.

Repeated activation of one existing logical Child is not covered or repaired:
the existing worker rebuilds its creation timestamp, while the existing V2
writer rejects a changed Child creation identity. The existing None/None warm
control reproduces this failure. Its evidence is preserved as a concrete
adjacent issue, without claiming a green full worker suite or adding a creation
identity transport/recovery protocol here.

No new tool/Root/task authority, packet, launch handshake, recovery journal,
remote capability system or all-provider fallback repair is included.
