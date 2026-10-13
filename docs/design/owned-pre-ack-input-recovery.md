# Local Actor pre-ACK input recovery

`SubAgent.run(reset_to_last_user=false)` can recover one expired owned Inbox
claim for an Ultra Root's zero-tool local Child. The previous activation must
be Failed or expired, with Host-persisted local `owned-initial-release-v1`
placement evidence. Its exact input must already be checkpointed and have no
permanent Host ACK receipt. Missing runners do not prove that old workers stopped.

The normal run request reloads the current Child and preserves its history.
The runner acquires the actual replacement Inbox owner, claims a new Actor
attempt, and requires Storage's complete current-prefix `AlreadyCheckpointed`
readback. Only the verified target in the worker copy loses the reserved
bookkeeper key; Host Main remains intact and the ordinary Domain matcher is
unchanged. Typed startup, current permission posture and actual Host ACK must
still succeed before the existing release permits provider admission.

Focused fixtures include real Store/Inbox refusal cases and a live old worker
that remains unable to enter its provider. The native fixture uses real serve,
SubAgent, worker and Host-persisted claims: a readable but unwritable admitted
folder causes actual ACK failure, then cold run(false) waits for the unchanged
physical lease to expire. It checks one correction, one replacement reply and
ACK before replacement provider admission. These fixtures have not yet run.

Already released input, legacy/unknown placement provenance, live claims,
multiple inputs, reset, read-only tools and remote recovery remain unsupported.
This slice does not complete the broader #791, #1055 or #1341 acceptance.
