# ViewImage Vision fallback boundary

Tracking: Bamboo #2014 / PR #2017. This document defines the behavior boundary;
source-bound validation results and resource handoffs are recorded in the PR.

## Two different image inputs

| Input with the same explicitly non-Vision main model | Behavior |
| --- | --- |
| An image uploaded directly into the conversation, including a stored attachment URL | Reject with the existing “does not support Vision” error. Do not resolve a Vision target or send image bytes to either model. |
| A model-issued `ViewImage` call for a local image | Execute the existing tool permission/file checks, send the tool result to the explicitly configured Vision model, then return labeled text to the original main model. |

Configuring a Vision model does not grant permission to forward conversation
uploads. The fallback belongs to the tool invocation, not to general context
preparation, session history, the upload handler, or the startup validator.
Resuming a text-only model with raw historical image parts keeps the existing
rejection. Previously completed ViewImage fallback results are already text.

The following original boundaries are unchanged from accepted base
`bf6b20bab2832b45d4b0927e971cea64866f2c77`:

- `session_app/execute/validation.rs` rejects image-bearing sessions when the
  actual executing model has an explicit Vision opt-out.
- `runtime/runner/round_lifecycle/context_preparation/transforms.rs` rejects
  remaining raw image parts before attachment resolution or provider dispatch.
- The HTTP startup regression continues to expect `BAD_REQUEST` for that model.

## Tool-only routing

The only new production call of `resolve_vision_target` is at the resolved
`ViewImage` execution boundary in `tool_execution/per_call.rs`. Root authority
and argument policy run first. Missing/disabled/unadmitted/non-Vision targets
fail before file reads. File permissions and hooks stay in the existing
executor; only successful image results enter `describe_tool_images` before
terminal lifecycle events and tool-result persistence.

The target is an exact `defaults.vision` provider instance/model, or the active
instance's explicit `vision_model`. It never guesses fast/chat, a model name,
or another provider account. The shared factory supports native provider
protocols and preserves the selected role's reasoning preference. The Session
main model and provider do not change.

An effective Vision-capable main model keeps the direct tool-image path.
Missing/null capabilities retain default-on support and existing independent
legacy hook compatibility; the new tool-only resolver does not participate in
that legacy general-message transform.

Without a global Vision role, the resolver captures the executing run's actual
provider instance, including a session override. It never substitutes the
process-wide default account for that instance's explicit `vision_model`.

The strict atomic error contract belongs to fresh ViewImage results. The
independent legacy hook retains per-image degradation: a request, stream or
description failure produces a sanitized omitted-image placeholder and the
remaining images continue. Each description has a bounded deadline; the tool
path also bounds the complete rewrite. Configuration/capability checks still
fail before dispatch, and omitted placeholders never echo source URLs, provider
errors or partial descriptions.

Images and returned descriptions are untrusted data. Description text has a
clear Vision-model label and JSON quoting. Provider errors are sanitized;
failed, empty, overlong or timed-out descriptions are explicit tool errors.
A semantic `Done` event finishes a valid description without waiting for
transport EOF; an open connection after completion is not a timeout.
The request/stream remain inside cancellation scopes, with bounded time/output
and the existing cumulative encoded tool-image budget.

## Offline regression coverage

`vision_same_nonvision_main_rejects_uploads_but_describes_viewimage_output`
uses the same main provider/effective model and valid Vision target for both
paths. Data-URL and attachment-URL conversation images fail both the original
startup validator and the real pipeline, with zero target resolutions, zero
main-model calls and zero Vision calls. The subsequent real Builtin `view_image`
call resolves the target once, makes one mock Vision request, returns labeled
text to the main model, and preserves its model identity. An enabled legacy
Vision hook does not bypass the upload rejection.

Other mocks cover direct tool images, unconfigured/non-Vision targets,
exact instance isolation, native OpenAI/Anthropic/Gemini requests to local fake
HTTP services, failure, empty/overlong output, timeout, tool cancellation and
budget limits. Historical-image automatic fallback tests were removed because
that behavior is outside the requested boundary.

Run these tests with fake providers and loopback HTTP services only; never use
configured paid model credentials. The PR must record the exact source,
commands, executed case counts and results. Formatting and static checks alone
do not establish compilation or test acceptance.
