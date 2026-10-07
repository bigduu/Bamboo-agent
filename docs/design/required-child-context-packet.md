# Required Child ContextPacket v1 (#1342)

`SubAgent.create.context_packet` opts a fresh one-shot task into complete instruction delivery. Absent packets retain legacy behavior. A packet binds content; it grants no tools, permissions, role, lifecycle, or task-change authority.

## Input and bounds

The strict version-1 object requires `objective`, `constraints`, `acceptance`, `non_goals`, `necessary_user_instructions`, and `recorded_decisions`. Every acceptance entry and objective must be nonblank; the other arrays may be empty. Optional `source_user_message_ids` and `background_message_ids` select durable parent messages. Unknown fields and null packets reject.

| Material | UTF-8 byte limit / behavior |
| --- | --- |
| Entire opt-in tool input / packet | 32 KiB; reject |
| Complete required text, task brief and selected user text | 16 KiB; reject |
| Each unescaped input line | 2,048; required rejects, optional whole entry omitted |
| Escaped six-part assignment plus admitted background | 24 KiB; reject |
| Optional background | Eight whole entries / 4 KiB total; omit whole entries and count |
| Selectors | 16 required User IDs / 64 optional IDs; duplicates reject |

No history is selected by default. Required selectors resolve complete plain User text; multimodal/tool-call sources reject. System or non-plain optional entries are omitted. Creation reports background counts; provider fitting records additional whole-entry omissions in diagnostic metadata.

## Host binding and compatible route

Before persistence/enqueue, the host reloads the durable parent lifetime, resolves IDs/content SHA256 values, bounds the assignment, and delegates preflight to the same first matching registered runner used for execution. Its immutable launch facts must be trusted built-in factory/default `current_exe`, default `subagent-worker` arguments, local BambooRuntime and an available mailbox bus. `required_child_context_v1` compatibility is probed before creation and activation. Hot config cannot substitute for actual runner facts.

Unknown embeddings, custom binary/args/profiles, Claude/Codex, remote/scheduled placement, resident and context fork are unsupported. Compatibility is not continuous SHA authentication or cryptographic loaded-image attestation of an administrator-replaced executable.

The immutable-assignment digest binds parent ID/created_at, Child ID, selected source IDs/digests, complete assignment, optional background/count and the actual host-derived Child TokenBudget snapshot. Callers cannot supply that snapshot. `None` retains normal model defaults; final fit respects the stricter host/model safe input cap, without claiming the derived reserve equals either original margin.

## Worker and provider boundary

Required runs use fresh, non-reusable workers. Existing RunSpec Message metadata transports the host-authored binding; mutable metadata is not an authority grant. Strict worker decoding rejects malformed messages, missing/modified binding or assignment, and wrong logical parent/Child before seeding/provider execution; installation applies the bound budget.

Every round projects the complete PromptIR and provider-visible tools. Optional background alone may be omitted as whole Messages. Lossy summary/archive/manual compaction/forced-overflow paths cannot discard required content or invoke a summarizer. Responses continuation is disabled. After reconciliation/checkpoint/reprepare, the actual provider-bound body and known tool footprint must fit the safe input cap and byte budget; otherwise the run fails before the next provider invocation. `never_compress` alone is not delivery proof.

`SubAgent.update` rejects assignment replacement before mutation; create a new Child. Authorized live guidance remains additive. This is not TaskCAS and does not control later message semantics. Same-Child retry recovery is unsupported, automatic first-frame retry is disabled, and existing worker birth validation remains fail closed (#1348). No live/resident/remote assignment protocol or second journal is introduced.

## Acceptance boundary

The CLI fixture runs this build's actual `bamboo serve` artifact and default `current_exe` worker; only the external provider is fake. Parent source messages/background are fixture-authored in the trusted store and loaded by a cold host restart; live external edits are not tested. Natural Child create/update-rejection/run checks canonical content, Unicode, optional counts, the actual Child budget snapshot, invocation and durable reopen. Normal creation binds `None` (no explicit override, existing model defaults); it does not inherit a stale persisted Root budget. Tiny-budget/huge-guidance cases explicitly author the trusted host store between create/run as fault fixtures, not public mutation APIs; Tiny binds the actual Child's 128-token budget; HugeGuidance binds its 64K budget and retains the complete System text. Both require zero Child-provider calls. The large default model window is not presumed to overflow from byte size alone. Required overflow and startup-unsupported/live-config flip require zero Child persistence. Unit tests separately cover explicit budget capture/installation, strict decoding, altered/missing binding, whole optional omission, later rounds, lossy-path rejection and final known-footprint safety.
