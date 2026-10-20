# Strict local Child empty Skills host

Issue #1945 migrates only the existing no-Skills contract of the Host's native
Tokio Child backend. Required-context, named-profile and Ticket-plan executions
already request an empty selected Skill set. They now use a private finite
`SkillExecutionHost` with empty metadata instead of the legacy Instruction gates.
Ordinary local Children retain their existing path and remain a separate positive
Reader migration. Native Main/Ultra behavior from #1944 is unchanged.

## Authority and currentness

`LocalTaskChildRunner` binds the host only after its real Router run, canonical
Child admission, `ChildActorRuntimeWrite`, owned persistence and running checkpoint
exist. The host captures that already prepared Execution's fence and cancellation
token; no transport field, persisted input ID or public constructor creates it.

The Engine's bounded current-input batch is correlated with its actual Session
and execution IDs. Absence and NoNew rounds are supported as data, with no Skill
or Source grant in either case. Before returning empty metadata, the host checks
the exact Child birth, parent birth, root, spawn depth, Project, strict restriction
and current Actor fence/ancestor chain through the existing guarded event port.
No event is published. The future's existing finish guard and private cancellation
token revoke retained use; no second writer, grant map or lifecycle protocol is
introduced.

The actual Child callable boundary removes the four official Skill interfaces
from the current registered positive ceiling: `load_skill`,
`read_skill_resource`, `skills_list`, and `skills_read`. List, exact ownership,
final dispatch and permission-only checks consume that same set. Existing
read-only narrowing, disabled tools, forced parent approvals and owned resource
cleanup remain in the same chain. No replacement Reader is registered.

## Provider and validation boundary

This host operates on domain Session/Actor/input types through the common Engine
and `LLMProvider`; it adds no OpenAI, Anthropic or Gemini wire semantics. Inbound
compatible forwarding APIs and outbound provider adapters remain separate, as
recorded in [the Native Ultra protocol matrix](native-ultra-skills.md).

The local regression family uses the production spawn, same-process Agent,
canonical Inbox and Actor writer, with fixture providers and registered tools.
Coverage includes a real newly admitted Child input, NoNew after an allowed task,
empty Skills schemas, inert legacy activation metadata, final answer, cancellation
and retained-host denial, plus ordinary Child isolation and all callable/permission
boundaries. Conversion evidence from #1944 is retained under its exact inputs;
this change does not claim new provider-network, account or full platform proof.

No broker, Worker executable, deployed/cluster path, paid-model run, product
release, credential or permission-policy change belongs to this slice. Ordinary
Child positive Source/Reader authority, compatible API runtime activation, legacy
public exports and complete configured-provider/three-platform acceptance remain
open under #1595/#1536/#1533.
