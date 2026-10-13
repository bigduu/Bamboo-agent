# SessionInbox ACK failure stops provider execution

A durable transcript checkpoint and admission cursor are retained when ACK fails. They do not confirm that ACK completed. The shared round prelude stops before Project, memory, prompt refresh and provider execution; it may publish message events for input that is already durable. Both new-input and permanently admitted duplicate ACK failures return the same recovery message without internal filesystem details.

Worker initial delivery uses `Agent::admit_session_inbox_at_safe_boundary_checked` before sending admission confirmations or starting its provider. The existing count-only SDK method remains compatible, but its partial count cannot prove successful ACK and must not be used as an execution barrier.

Retry this activation through the existing admission path. A fresh store reader recovers the durable cursor and exact typed message, removes a remaining claim, and reuses the permanent receipt without appending the input again. The focused fixtures inject errors before and after real filesystem ACK publication and assert zero provider calls on failure and one input after cold retry.

This change does not activate owned Inbox leases, ActorDirectory execution, worker release, or a new journal. It makes no ExactlyOnce or whole-runtime completion claim.
