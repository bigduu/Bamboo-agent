# Native Ultra Main finite Skills runtime

Issue #1943 is a bounded child of #1595/#1536/#1533. Ordinary Native Main and
finite default SDK execution already use the registered Skills host. This change
moves local Native HTTP Ultra Root execution through the same host without
granting Ultra access to Skills.

## Execution boundary

`registered_main` checks the ordinary Root identity and, for Ultra, a nonzero
host-selected Root authority revision and existing orchestration compatibility.
It is eligibility, not authority. Binding still requires the existing real
`SessionExecutionReservation`, actual Server Root executor, live runner identity,
cancellation token, permission policy and accepted-input observation. The host
captures both the authority revision and the Ultra mode; an inconsistent
same-revision mode or subsequent durable mode selection invalidates it.

Native admission uses its real reservation and canonical checkpoint/ACK input.
The same registered producer is used by the existing Root coordinator callback.
The typed preappend factory remains limited to ordinary Main; Ultra rejects a
selected Skill before admission.

Ultra metadata rendering checks its durable Session and current execution owner,
then returns an empty Skills context before resolving a Source publication.
The finite executor exposes only the actually registered intersection with
`ROOT_ORCHESTRATION_TOOLS` and rejects tools outside that set. In particular,
`skills_list`, `skills_read`, `load_skill` and `read_skill_resource` remain absent.
Existing finite execution disables legacy Instruction selection, first-call and
answer-suppression gates. Historical metadata stays inert; no history-derived
Skill grant is introduced.

## Protocol and provider scope

An inbound API is distinct from the model provider used by a Native execution.
The current route registry provides:

| Inbound surface | Behavior | This child |
| --- | --- | --- |
| Native Bamboo chat/execute and Root coordinator | Canonical Session/Inbox plus real execution reservation | Local Ultra host binding |
| `/openai/v1/chat/completions`, `/openai/v1/responses` | Provider forwarding | No Main reservation is fabricated |
| `/anthropic/v1/messages`, `/anthropic/v1/complete` | Provider forwarding | Separate forwarding contract |
| `/gemini/v1beta/models/{model}:generateContent` and `:streamGenerateContent` | Provider forwarding | Separate forwarding contract |

`bamboo-llm::provider_factory::AVAILABLE_PROVIDERS` contains `openai`, `anthropic`,
`gemini`, `copilot`, and `bodhi`. OpenAI and Copilot have Chat-compatible and
Responses paths. Anthropic uses Messages; Gemini uses its native content format.
Bodhi explicitly routes to OpenAI, Anthropic or Gemini proxy formats. A configured
base URL or provider name alone does not establish protocol equivalence.

The host consumes domain Session/input/tool types through `SkillExecutionHost`
and the common Engine pipeline, before calls to `LLMProvider`. It adds no OpenAI
message semantics, endpoint handling or provider-specific permission decisions.
Protocol differences remain in `bamboo-llm` conversion and provider adapters.

## Regression matrix and limits

| Contract | Local regression |
| --- | --- |
| Native Ultra real reservation and current input | Actual chat/execute with a local fixture provider |
| Ultra canonical registered inventory and ordinary task | Exact tool-set comparison, actual allowed Read, completed answer |
| Ultra selected Skill denial | Rejection before Inbox or User mutation |
| Captured/durable Root mode and cancellation | Same-revision negative control, durable revision change, retained-host revocation |
| Legacy Instruction state | Historical pending selection does not inject Skill content or require `load_skill` |
| Ordinary Native Main | Existing finite Skills HTTP fixture family |
| OpenAI Chat, Responses, Anthropic, Gemini resource envelopes | Existing `skills_read_all_four_real_provider_envelopes_concatenate_raw_files_to_true_eof` converter regression |

These local tests use disposable data and fixture providers. Provider conversion
coverage is not a live account, network-service or model-behavior acceptance.
Copilot authentication and Bodhi proxy integration, configured-provider runtime
acceptance, the full three-platform matrix, child/deployed-worker binding,
compatible API activation and official legacy export retirement remain separate
work. Cross-machine/cluster implementation stays paused. This child does not
complete the migration trackers or change product release inputs.
