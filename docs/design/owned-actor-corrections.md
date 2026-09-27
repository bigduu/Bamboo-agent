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

Only one continuation (two worker Runs) is supported. Text is limited to 8 KiB,
without parts. Input lease expiry never exceeds the original Actor lease or one
hour from its initial claim. Direct legacy WS continuation, raw steering,
tools, reasoning, remote/nested activation and completed-Actor restart remain
unsupported. Later terminal races or extra queued input can remain unconfirmed;
durable history/queue are preserved. No renewal/reclaim, automatic recovery,
worker release or ExactlyOnce claim is introduced. Checked startup relies on
the separately delivered #1407 admission failure handling.
