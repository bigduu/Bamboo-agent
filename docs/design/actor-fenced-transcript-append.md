# Explicit Actor transcript append (V2 v1)

`SessionStoreV2::append_actor_transcript(ActorTranscriptAppend)` is an opt-in storage operation. There are no production runner/provider callers or generic `Storage` fallback. It is a bounded part of #1351, not complete runtime ownership or exactly-once delivery for #925/#791.

## Admission

The caller supplies the existing complete `ActorActivationFence`, exact Session `expected_created_at`, full ordered `expected_messages`, and complete typed `expected_provider_transcript`. Read these from a durable snapshot. A differing prefix returns `PrefixConflict`; reload and decide explicitly. IDs cannot be blank, duplicated or collide with any durable prefix ID, even for equal content. There is no automatic rebase/replay or permanent operation receipt.

New normalized messages are only Assistant plain text, optionally Commentary or FinalAnswer. Tool calls/results, reasoning/signatures, content parts/OCR, metadata (including explicit null), and compression/protection flags are unsupported. At least one new message is required. Historical messages retain their original roles/fields/JSON bytes. Missing stable historical id/created_at fails closed; defaults are not adopted as a migration.

Optional native groups must anchor to a new Assistant message on the durable lane's already-bound exact family/protocol/provider-instance boundary. All-none and legacy automatically unbound routes are rejected. Only typed Provider-origin OpenAiMessage, server OpenAiToolSearchCall/OpenAiToolSearchOutput, AnthropicText, AnthropicServerToolUse/AnthropicToolSearchToolResult are admitted. Existing validators enforce Model/ToolResult author, completed discovery pairing/order, group identity and item schema. A standalone text item is not a discovery group. Host/developer input, generic tool calls/results, thinking, client execution, route switches and caller-selected sequence/epoch are unsupported. Counters must advance without overflow and the whole candidate must strict-decode successfully.

## Publication and uncertainty

The operation holds lifecycle shared -> Task shared -> exact Session maintenance and physical file locks. It strictly reads existing regular main/runtime, initialized marker and Actor files, binding birth/id/kind/root/parent/depth and ordinary authority, current own Project/metadata observation, complete live Reserved/Running fence and Root pair/proof. Supervisor is unsupported. It does not initialize, repair or refresh any authority and does not grant tool authority. It uses own lifetime revocation checks, not an all-ancestor lifetime guarantee.

Standard serde RawValue spans preserve original main control-plane/unknown keys, message prefix entries and native group prefix entries byte for byte. Only the messages array and, when provided, native groups/counters are patched. Runtime, proofs, marker, Actor, attachments, updated_at, admission, model context, summary, Task, Root mode and policy are not written. The returned Session is decoded from the confirmed main readback plus unchanged runtime projection; stale Root budget cleanup and search/index/cache refresh are not performed by this API.

One owned blocking std job retains the actual locks through temp write/file sync, replace, directory sync, readback and cleanup. Caller abort or Tokio runtime shutdown after that job starts cannot release its locks before termination. Immediately BeforeReplace, after any barrier, it checks unchanged source bytes and the live fence using fresh host Utc::now. There is no network/provider await.

`BeforePublication`/admission failures mean this operation did not replace main. After replacement, directory- sync/readback/join failure is `OutcomeUnconfirmed`: main may already contain the append. Reload before taking any further action; do not repeat the old request or interpret uncertainty as an unchanged transcript.

## Acceptance boundaries

Tests use real files, pre-created independent Stores, FileExt lock probes and actual blocking Before/AfterReplace barriers, including delayed lease expiry, successor ordering, caller abort and whole-runtime shutdown. Same OS process independent Stores exercise the file locks; this is not an OS multi-process kill experiment. Native platform results are reported with the final gate receipt; Windows replacement is only source-portability coverage unless separately run.

Legacy full/runtime/fallback/clear paths retain #1350 protection. Independent Task and Management publications (#1354/#1355) remain separate prerequisites for global runtime opt-in; #1356 startup reconstruction is independently accepted. This API does not certify Inbox ACK, inputs, cancellation/provider effects, remote placement or arbitrary control-plane rewrites.
