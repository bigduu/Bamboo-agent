# Compact SubAgent caller

The advertised `SubAgent` schema has `intent`, `target`, `message`, reserved
`reply_to`, and optional `role`. The default intent is `chat`. A message without a target
creates a direct durable Child of the current Root and keeps the complete task
body. A target chat delivers corrective input through the existing SessionInbox.
Runtime continues to own activation and parent waiting.

Only a new Child can select `role`: the shipped defaults are `explorer`,
`implementer`, and `reviewer`, with the existing Project → Global → builtin
catalog precedence. Omission keeps `worker`; unknown names retain the existing
legacy-label fallback. Invalid or duplicate catalog definitions fail closed.
Role names are explicit and are never inferred from the task body. A continuation,
inspection, or control call with `role` is rejected; it cannot rebind a Child's
frozen profile. Model/workspace/host parameters remain hidden. The four-field
example in #791 does not impose a closed field count; this optional logical role
selection keeps the accepted named-profile consumer reachable from the LLM.

`inspect` without a target returns an observed owned tree with at most 32 nodes
and depth 4. A target inspection supports overview, paginated message previews,
message content, the latest result, and an error indicator. For pagination, pass
a JSON object containing `view`, `cursor`, and optionally `message_id` as the
message. History cursors retain the existing persisted-prefix checks. The whole
serialized ToolResult is limited to 8 KiB; structured physical runtime fields,
tool arguments, and raw execution errors are excluded from the compact result.
User and Child-authored transcript text is content, not an identity certificate.

`control` accepts `cancel` and `retry`; retry runs the same logical Child rather
than creating another one. A corrective message uses target chat. Compact calls
normalize before launch-owner classification, preserving the existing detached
owner, admission gate, cancellation compensation, and runtime wait signal.
Legacy action calls retain their original arguments and output shape.

This caller covers Root to direct local durable Child execution. It does not
resolve ParentRequests, reassign remote actors, or authorize deep lifecycle
mutation. Ordinary Root broker tools remain registered until a remote facade
replacement is available. Tree/status results are observations, not claims or
activation authority. Required-packet typed-result selectors remain available
through the compatible legacy inspection route.

The source tests include real V2 storage, SessionInbox, adapter ownership checks,
and the pre-commit cancellation barrier. Their no-op runner proves caller and
admission bookkeeping, not native worker execution. Native acceptance is recorded
separately when run against the compiled host and its `current_exe` worker.
