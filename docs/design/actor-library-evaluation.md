# Actor library decision for #791

Decision date: 2026-09-29.

## Current delivery boundary

Finish the canonical local ActorSession, direct parent requests, durable
SessionInbox admission, and parent/child wait and resume path first. Keep the
already supported pinned WSS worker path bounded and fail closed. Automatic
remote placement, migration, and failover remain outside the current delivery
slice until a durable activation owner can fence transcript writes and Inbox
acknowledgements across processes.

`ActorSession` and its Session history remain the durable agent identity.
`SessionInbox` remains the durable message authority. A worker, mailbox address,
or library actor reference is capacity or transport, not a second agent identity.

## Libraries evaluated

| Library | Useful capability | Fit with Bamboo |
| --- | --- | --- |
| [Ractor / ractor_cluster](https://docs.rs/ractor_cluster/latest/ractor_cluster/) | Typed actors, supervision, remote references and RPC | Best candidate for a small execution/supervision prototype. Its [runtime semantics](https://github.com/slawlor/ractor/blob/main/docs/runtime-semantics.md) say reconnects may lose messages; application-level acknowledgement, retry and idempotency are still required. |
| [Coerce](https://docs.rs/coerce/latest/coerce/) | Remote actors, sharding, persistence and snapshots | Its own ActorSystem and persistence model would overlap Bamboo's current Session, directory and Inbox authority. A full migration would be a separate architecture change. |
| [Kameo remote](https://docs.rs/kameo/latest/kameo/remote/index.html) | Remote actor references over libp2p | Its peer-to-peer network differs from Bamboo's broker-mediated topology. |
| [Actix](https://actix.rs/docs/whatis/) | In-process actors and supervision | Does not replace Bamboo's cross-process durable delivery. `actix-web` in the server is not itself an Actix Actor runtime. |

No library here supplies the full combination of Bamboo's stable Session
identity, direct-parent authority, checkpoint-before-ack admission, Project
boundary, and cross-process activation fencing. Replacing the broker or Inbox
now would require proving these contracts again and would delay the local #791
path.

If a later prototype adopts a library, place it behind the execution or
supervision seam and keep Bamboo's Session and Inbox as the only durable
authorities. Compare it against the existing implementation with the same
restart, duplicate delivery, parent request, and worker replacement tests.
