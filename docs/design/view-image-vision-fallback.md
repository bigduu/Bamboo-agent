# ViewImage Vision fallback boundary

Tracking: Bamboo #2014 / PR #2017. This document defines the behavior boundary;
source-bound validation results and resource handoffs are recorded in the PR.

## Two different image inputs

| Input with an explicitly non-Vision main model | Behavior |
| --- | --- |
| A conversation upload or stored historical image | When explicit auxiliary Vision configuration is valid, startup permits the run and the prepared context contains a text reference instead of raw image parts. The original Session image remains intact. Missing or invalid configuration fails startup with retained-image guidance. |
| A model-issued `ViewImage` call for a local image or retained conversation reference | Execute the existing tool permission/file checks, send the authorized image to the explicitly configured Vision model, then return labeled text to the original main model. |

Startup checks the explicit target configuration without constructing or
calling its provider. A configured target does not itself trigger an image
request. Context preparation projects image parts into transient references
before dispatch to the non-Vision main; it does not rewrite durable history.
Only a permitted `ViewImage` call can resolve a retained reference and request
a description. The original image bytes are never forwarded to that main model.

The independent legacy image hook retains its own opt-in behavior when the
main model has no explicit capability declaration. It is not a substitute for
the explicit target required by the non-Vision reference path.

## Tool-only routing

The only new production call of `resolve_vision_target` is at the resolved
`ViewImage` execution boundary in `tool_execution/per_call.rs`. Root authority
and argument policy run first. Missing/disabled/unadmitted/non-Vision targets
fail before file reads. File permissions and hooks stay in the existing
executor; only successful image results enter `describe_tool_images` before
terminal lifecycle events and tool-result persistence.

The target is an exact `defaults.vision` provider instance/model, or the active
instance's explicit `vision_model`. It never guesses fast/chat, a model name,
or another provider account. An explicitly saved `defaults.vision` role is
honored even when the primary-chat `features.provider_model_ref` rollout is
omitted or false. Only an unset global Vision role permits the active-instance
compatibility setting; an invalid explicit role fails closed. The shared factory supports native provider
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
A `ViewImage` call may include an optional `prompt` describing the user's
question or the details to examine. The configured Vision model receives that
focus as untrusted, JSON-quoted context alongside the image; omitting it preserves the
general description. For example: `{"path":"/tmp/screenshot.png","prompt":"Describe the sidebar creation control"}`.
The tool call does not impose a separate output-token
limit on protocols that allow the field to be omitted. Anthropic requires
`max_tokens`: the native adapter respects an explicit provider setting and
otherwise uses 16,384 tokens, matching the Bodhi proxy's default.
An empty description records a sanitized provider finish reason when available;
unreported reasons remain `unavailable`, and unrecognized provider strings are
reported only as `other`.
A semantic `Done` event finishes a valid description without waiting for
transport EOF; an open connection after completion is not a timeout.
The request/stream remain inside cancellation scopes, with bounded time/output
and the existing cumulative encoded tool-image budget.

## Offline regression coverage

Saved-role regressions cover the primary-chat rollout flag omitted, false and
true, including pure startup preflight, exact provider/model/reasoning identity,
a valid legacy target that must not replace an invalid saved role, and live role
changes between calls. Loopback fake providers verify that an already resolved
request retains its complete target while the next request reads the new pair;
disabled or explicitly non-Vision targets make no additional request.

The HTTP regressions `vision_nonvision_new_upload_reaches_main_with_references`
and `vision_nonvision_history_followup_reaches_main_with_references` exercise
the real startup path. With configured auxiliary Vision, the main provider
receives a retained reference and the original image message is preserved;
without configuration the historical-image follow-up is rejected. Reference
tests also cover retained inline/attachment images and reject forged
cross-session or malformed references before any attachment or file is read. The loop test
`vision_nonvision_upload_and_history_followup_use_only_requested_reference_fallback`
checks new and historical inline/attachment images, no raw images for the
main model, and one auxiliary request after an explicit ViewImage call.

Other mocks cover direct tool images, unconfigured/non-Vision targets,
exact instance isolation, native OpenAI/Anthropic/Gemini requests to local fake
HTTP services, failure, empty/overlong output, timeout, tool cancellation and
budget limits. Historical images are not automatically described by an auxiliary model;
the explicit ViewImage call remains the description boundary.

Run these tests with fake providers and loopback HTTP services only; never use
configured paid model credentials. The PR must record the exact source,
commands, executed case counts and results. Formatting and static checks alone
do not establish compilation or test acceptance.
