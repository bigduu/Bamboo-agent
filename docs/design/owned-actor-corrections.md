# Owned local Actor corrections

The explicit Ultra Root + fresh local Bamboo zero-tool named profile route can
consume one bounded text correction during its current Actor activation. The
normal parent SubAgent `send_message` tool delivers the real Host Inbox item;
no worker ID or request metadata becomes authority.

On the first completed plain worker Terminal, the Host claims the actual pending
owned Inbox item, then uses the existing fenced transcript append to commit
that Assistant. It then uses `ActorInputCheckpoint` to commit the typed User and
admission cursor atomically. Rejected or unconfirmed checkpoints do not dispatch
a second Run, invoke a provider, ACK, or fall back to ordinary saves.

The second Run carries the committed prefix and typed initial delivery. Its
existing checked worker startup admission confirms exact target, envelope,
generation and Host run before the Host owned ACK. Broker Run correlation and a
new native execution epoch reject old frames; public Host feed sequencing stays
unchanged. The same Actor fence, attempt and original lease/watchdog remain in
force. There is no second Actor claim/start or early parent success.

Before dispatching the second Run, its permission audit witness is refreshed
from the actual Host checkpoint readback. The posture and current Host policy
must still match the original ceiling; changes reject continuation. The existing
durable audit comparison remains active for the second worker bootstrap.

Only one continuation (two worker Runs) is supported. Text is limited to 8 KiB,
without parts. Input lease expiry never exceeds the original Actor lease or one
hour from its initial claim. Direct legacy WS continuation, raw steering,
tools, reasoning, remote/nested activation and Succeeded-Actor restart remain
unsupported. Later terminal races or extra queued input can remain unconfirmed;
durable history/queue are preserved. No renewal/reclaim, automatic recovery,
worker release or ExactlyOnce claim is introduced. Checked startup relies on
the separately delivered #1407 admission failure handling.

## Failed zero-tool retry with a new generation

A terminal Failed zero-tool Child that has no committed Assistant/Tool, native
transcript, old owned claim, cursor or private checkpoint marker can accept one
new bounded input through normal `SubAgent.send_message(auto_run=true)`. Its
actual Inbox must contain exactly one eligible new generation, newer than the
failed activation. The existing Directory claim/start creates a new attempt
and lease owner on the same logical Child/birth; it is not renewal of the old
activation. Live, expired-live, Succeeded and ambiguous old state reject.
Read-only Glob never enters this retry path; it retains its fresh-only admission.

Before sending the retry Run, the Host claims that actual input and invokes the
same `ActorInputCheckpoint` consumer as the Running correction. It adopts the
committed Session, sends the pre-input prefix plus existing typed delivery, and
binds its permission audit witness to that actual readback under the unchanged
posture and current Host policy checks. It requires exact checked startup
confirmation before owned ACK. A rejected or
unconfirmed checkpoint dispatches no Run/provider and performs no ACK or
ordinary save. The successful plain reply is appended with the new fence;
rejected worker reasoning/cache content is never Host canonical history.

Fresh required-context bus workers receive unique physical mailbox IDs. A killed
worker's unACKed Run stays in its old mailbox instead of being replayed by the
retry process. Logical Child identity/birth, cache namespace, typed input targets
and permissions are unchanged; legacy pooled/direct/remote routes are unchanged.

Only one retry worker Run is enabled here; extra pending input stays durable
and unsupported. Same-generation checkpoint/old-claim recovery is separate:
its actual server Run caller must first omit legacy full-save/reset, then reuse
this input consumer with current expired physical replacement claim and exact
`AlreadyCheckpointed`/startup/ACK. Current birth/lineage, prefix and lease checks
remain necessary; no lease renewal, automatic restart, worker release, remote,
reassignment or ExactlyOnce is claimed by this path.
