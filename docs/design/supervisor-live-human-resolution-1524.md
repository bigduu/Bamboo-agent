# Real-model Human resolution through an isolated Host

This is the bounded acceptance child [#1524](https://github.com/bigduu/Bamboo-agent/issues/1524)
of Supervisor tracker #1481, starting from the ordinary #1508 merge
`0052de8cf526324ff64a0c59bdfb8df9f674ea5a`. The earlier real-model test exercised
proposals over a synthetic service. Deterministic Host/Native/browser evidence
is separate. Neither is relabelled as this live Host acceptance.

## Scope and configuration

The ignored `ticket_live_host` case starts an owned local Host with ordinary
Human `/chat`, durable Inbox, Supervisor tools and canonical Ticket state.
Mutation is enabled only in the temporary fixture; dispatch remains disabled.
Synthetic payment objects have no external executor. No Worker or shell action,
production data, Lotus edit, root pointer update or deployment is part of this
slice. #1522/#1523's sealed Child-report projection stays with its Runtime owner.

The fixture requires an explicitly selected existing configuration root. It
reads the configured chat provider/model and decrypts its existing credential
only in the test process. The bridge holds that credential in memory while the
temporary Host uses a dummy key. No provider migration/save or key generation
is called. The existing provider, credential and key file bytes are checked
after execution without printing them. The bridge permits only selected-model
Supervisor requests, forwards real upstream response bytes and fails on upstream
errors; it has no generated-response fallback.

## Acceptance and evidence

Twelve independent Chinese samples cover chatter, vague agreement, a question
answer without a useful reference, duplicate-name ambiguity, cross-topic
answer/steer, four intents in one message, exact A approval/B rejection, changed
amount, conditional approval, quoted malicious text and a reference without a
grant. The multi-intent sample creates C as a draft; it requests no dispatch.
Wrong or extra approvals must be zero, other pending questions must survive,
and user-required acceptance stays enabled.

Each first attempt is saved before semantic assertions. Setup reads all seeded
Tickets/requests from one fixed, complete bounded HTTP projection while the Host
is running. After the model turn, the owned Host stops before canonical FileStore
validation of the complete manifest. It then
restarts and receives the identical Human message: ingress seq, saved proposal,
group receipts and Ticket state must replay unchanged, without another model
request. The existing writer-epoch restart invalidates open/approved approval
objects; replay must retain that expiry and cannot revive the old grant. Questions
and explicit denials survive unchanged. Failures retain the isolated Host log and first-attempt JSON. A single
filtered sample is partial evidence, never the complete twelve-case result.

The ordinary transport test checks authenticated credential separation, exact
SSE-byte forwarding, selected-model rejection and sanitized upstream errors.
It is labelled a synthetic transport check; it does not establish model quality.
The live target remains ignored in ordinary CI so missing credentials cannot
silently select a mock. Startup uses the existing 120-second Host allowance;
the new live completion has a bounded 240-second allowance and each upstream
request a 120-second deadline. Existing product-test deadlines are unchanged.

```sh
cargo test --locked -p bamboo-agent --test ticket_live_host
BAMBOO_TICKET_MODEL_CONFIG_ROOT=/explicit/existing/read-only/config \
BAMBOO_TICKET_LIVE_HOST_EVIDENCE=/absolute/fresh/evidence/directory \
cargo test --locked -p bamboo-agent --test ticket_live_host \
  live_chinese_human_resolution_through_actual_host -- --ignored --exact --nocapture
```

`BAMBOO_TICKET_LIVE_HOST_CASE=answer_e` optionally selects one diagnostic sample.
Do not rerun failures until a source change or a specific external failure
justifies it; retain the original attempt and keep result provenance separate.

## Acceptance status and evidence boundary

An initial twelve-case run at `ffa0c9c7d60f719f07a1b75f8429ffcc26367a6f`
made 36 real `gpt-6-sol` Responses requests and observed zero wrong approvals.
Its old 12/12 fixture result did **not** establish complete acceptance: assertions
allowed `needs_clarification` for nonempty correct proposals. Final canonical
state review found answerE, approveA and denyB remained Open; only 9/12 settled
as intended. That original run and its Host logs remain preserved. The fixture
now requires actual committed groups, canonical receipts and final request/Work
states, including report C as one draft Work. Production Human-evidence handling
is tracked separately in [#1527](https://github.com/bigduu/Bamboo-agent/issues/1527).
The latest validated source, gates and results are recorded in the linked PR
[#1525](https://github.com/bigduu/Bamboo-agent/pull/1525); this checkpoint does not
claim CI/review completion, default enablement, statistical correctness or
external exactly-once effects.

The first live chatter attempt did persist the correct zero-operation resolution
through three successful real `gpt-6-sol` Responses requests. Its overall test
failed because the initial fixture stopped/restarted before the Human turn,
invalidating seeded approvals by the existing writer-epoch policy. The failure
is preserved. The fixture now reads its baseline via the fixed HTTP snapshot
and checks safe approval expiry during the required post-resolution replay;
no production restart/approval semantics are changed.
