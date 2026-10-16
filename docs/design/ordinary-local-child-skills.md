# Ordinary local Child finite catalog/read

Issue #1947 binds Server-assembled ordinary same-process Tokio Child executions
to catalog/read. It depends on the actual Child Actor admission and writer and
on #1946's separate strict empty-host path. Custom/SDK constructors and deployed
workers do not silently acquire this Server service.

The Engine creates an opaque `LocalChildSkillAuthority` only from its real
prepared Execution. No serialized fence, stored input ID or public constructor
can create it. It checks the exact birth, lineage, assigned Project, current
canonical Actor fence/ancestor chain and private execution cancellation through
the existing guarded event port, without publishing an event or adding a writer.
The app factory receives this proof before installing its existing SkillManager,
SessionRepository, Project store, permission policy and registered base chain.

The finite host observes the common Engine Q callback. Only an actual current
ordinary User record can establish catalog/read context, synchronously borrowed
from that execution's current scope at each metadata/Tool call. Historical User
messages, initial stored assignment, absent input and NoNew do not supply it.
An unprepared typed selection fails closed: this child has no preappend F
producer and does not turn request fields into explicit invocation authority.
Initial-task preparation and other current input kinds remain later work.

Catalog/list/read recheck the current Source publication, host None versus empty
ceiling, mode, workspace/Project, disabled Skills and permission revision. Reader
response bytes are narrowed by the actual dispatch output cap; the existing
generic runner keeps complete JSON/cursor pages through true EOF. Termination,
cancellation and authority changes deny retained use. Legacy `load_skill` and
`read_skill_resource` are removed from this finite schema, ownership and
dispatch surface together; the legacy activation gates remain inert. The outer
local Child tool ceiling, disabled/read-only policy, approvals and resource
ownership remain on their existing path. Strict required-context, named-profile
and Ticket executions continue to use an empty host.

Runtime fixtures use the actual AppState assembly, native spawn scheduler,
canonical Inbox, same-process Agent, Actor writer and registered tool chain with
disposable data and a supplied fixture provider. They test full Skill/reference
EOF followed by an allowed task, current input, old state/NoNew, empty ceiling,
unprepared selection, disabled Reader, live Source/policy/cancellation and
retained/cross-Session rejection. Current-source OpenAI Chat, Responses,
Anthropic and Gemini envelope tests are separate conversion evidence; accepted
Native Main/Ultra and strict Child tests remain separate runtime regressions.

No OpenAI wire semantics enter the Engine proof or `SkillExecutionHost` port.
Inbound OpenAI Chat/Responses, Anthropic Messages/legacy Complete and Gemini
generate/stream remain distinct forwarders without fabricated Main reservations.
Outbound openai, anthropic, gemini, copilot and bodhi adapters are unchanged;
Copilot auth, Bodhi proxy and live account/provider behavior are not exercised.
Full three-platform acceptance, broader Child inputs/custom hosts, compatible
activation and official public legacy retirement remain open in #1595/#1536/#1533.
Deployed/cluster work stays paused; no paid model, credential, permission-policy,
frozen-main or product-release change belongs to this delivery.
