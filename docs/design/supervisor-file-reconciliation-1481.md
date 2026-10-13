# Explicit local file-effect reconciliation (#1481, P5/A11)

This local-only follow-up starts at `1afbd889` in the isolated branch
`bamboo/feat/1481-file-reconciliation`. A workspace replacement can complete
before publication of its success receipt fails. Stopping the owned process
alone therefore cannot release its Started effect or resource claim.

Schema 4 freezes the full canonical Write request and original Worker subject in
the existing immutable Assignment revision, before replacing the workspace file.
It adds no journal or second authority. Schema-3 uncertain effects lack this
request and remain quarantined; offline transfer preserves the complete schema.

The explicit User-only Host port observes the original target through the same
no-follow directory descriptors. Its reviewed request binds the fixed commit,
seq, epoch, Assignment revision, effect fingerprint and intended content hash.
Consumption requires the existing Runtime's durable actual stopped-Run fact and
exact currently observed bytes. Only metadata changes: the observed-content
receipt, original Worker operation receipt and User receipt publish atomically.
This establishes observed current content, not a physical execution count.

Unresolved effects retain claims. When all effects are resolved, the attempt
becomes failed (or cancelled), and the Work stays paused/blocked until explicit
resume and fresh-generation dispatch. No Submission or acceptance is synthesized.
A new tool-call ID cannot start another file write while any effect is uncertain.
Missing/mismatched bytes, unsafe paths and absent actual stop proof cannot be
cleared. No provider retry or arbitrary external-effect reconciliation is added.

Offline commands require the existing canonical Supervisor and lifetime Ticket
writer lock. A live Host cannot be bypassed. Ordinary mutations and Run permits
remain disabled offline; only this exact User acknowledgement can publish.
Uncertain HEAD/directory failures preserve read-only health until verified reopen.
The port is absent from the six LLM tools and HTTP API.

```sh
bamboo tickets file-reconcile-plan --data-dir <host-data> \
  --assignment-id <assignment> --effect-id <worker-file/id> \
  --operation-id file-reconcile/<id> --evidence '<operator observation>' > plan.json
jq -e '.request // error("keep quarantined")' plan.json > request.json
# Inspect the plan and exact request before explicit acknowledgement.
bamboo tickets file-reconcile --data-dir <host-data> --request request.json
```

Domain evidence: 74/74 Ticket tests passed, including process-exit/OS-lock faults,
complete-schema transfer and semantic receipts. After adding the fresh-call write
fence, all 12 file/reconciliation tests passed again; strict all-target Ticket
Clippy passed. Cases cover required stop, subject/CAS/replay, unchanged inode,
partial multi-effect recovery, changed bytes, EACCES, post-HEAD uncertainty and
prior-schema quarantine. Logs are `/tmp/1481-file-reconcile-domain3.log`,
`/tmp/1481-file-reconcile-domain4.log` and `/tmp/1481-file-reconcile-clippy2.log`.

Actual Host/native/CLI acceptance remains pending at this checkpoint. The first
compile exited 101 before running tests: reading an unchanged Engine
`prompt_context/plan_mode.rs` timed out with OS error 60. Its 5713 original bytes
were rematerialized from this worktree's HEAD and verified as the identical Git
blob `cfc379278302cf61171cddcba39f081865e0e092`; its implementation did not change.
The same test is being retried. The test-only Host injects ENOSPC at final file
receipt publication and checks actual owned PID reap, offline observation/replay,
explicit generation 2, acceptance and restart. Normal builds read no fault env.
Cold fixture startup now has a bounded 120s deadline after recorded HTTP worker
startup around 41s exceeded the former 45s readiness allowance.

Independent review is authorized for current #1481 diffs using the existing
OpenAI service. Earlier sessions ended without findings. A retry inherited
full-access defaults and was stopped; subsequent commands explicitly set
`sandbox_mode="read-only"` and `approval_policy="never"`, with no persistent
configuration or credential changes. Current review/full-workspace/browser
conclusions remain pending. Feature defaults stay off; production historical
Root/Inbox handoff remains with the existing #791 owners.

## Independent review corrections

The explicit read-only Codex review of `cb10f27b` against stable `685328ec`
completed with six findings. The local follow-up rejects case aliases of `.git`
and `.bamboo`; fails closed on conditional English approval clauses; replays
exact owned-stop checkpoints for every existing terminal status before result
ACK; delivers hook-augmented queued text/images while preserving raw Human
provenance; fits complete candidate contracts/questions within the byte budget
with explicit omissions; and retains existing ordinary file mode bits during
atomic content replacement. New files remain private, and special mode bits are
not copied. These changes add no keyword-based semantic dispatcher or new
Inbox/Root ownership protocol.

Current Ticket all-target regression passes 76/76 and strict all-target Clippy
passes. This includes 18 semantic and 13 file/reconciliation tests, plus existing
file-authority, offline-transfer and lifecycle checks. Logs and exit markers:
`../1481-evidence/1481-reviewfix-domain.log` and
`../1481-evidence/1481-reviewfix-clippy.log`. Engine terminal-stop recovery and
HTTP hook/provenance checks are running separately. Their controlled fixtures
do not establish actual OS process reap.

The third actual-native attempt exited 101 before any tests ran because an
unchanged `src/codex_cli_executor.rs` read timed out with OS error 60. The second
attempt was interrupted without an exit marker. A verified Git-blob-identical
source rematerialization is recorded in
`../1481-evidence/1481-committed-source-materialization.json`; it changed no
implementation bytes. Current native, full-workspace, built browser and fresh
review results remain pending, rather than inheriting earlier-head results.

The first review-follow-up Engine compile failed before tests with a missing
constant import in the new test module (`E0425`). The import is corrected in a
separate local follow-up. The original runner terminated; a detached `/tmp`
worktree at the same current commit is prepared for subsequent acceptance,
without changing the implementation branch or other sessions. HTTP ingress,
actual native, full-workspace and browser tests have not run at this checkpoint.

The temporary acceptance checkout at `7e05ed43` subsequently passed all eight
Engine terminal-recovery tests and the integrated HTTP hook/provenance case.
The first HTTP compile encountered ENOSPC; a subsequent fixture needed the
normal loopback peer identity before its owner-access assertion could run.
Both earlier failures remain in the evidence logs.

Native4 ran three actual Host/native tests: isolated coding and live-PID lease
fencing passed. The offline-file case reached the stopped PID, observed bytes,
idempotent acknowledgement and restarted Host, then failed an overly strict
empty-history assertion. Its committed snapshot proves the old generation-1
Submission is stale, while Work remains blocked/paused with no current or
accepted Submission. The test now checks that the offline acknowledgement and
replay neither create nor rewrite Submission history, permits only stale old
results, and verifies that generation 2 alone supplies the accepted reference.
This assertion correction changes no production behavior; its focused native
rerun is pending. Evidence: `../1481-evidence/1481-native4-7e05.log` and
`../1481-evidence/1481-native4-failed-snapshot.json`.

Native5 passed that focused actual Host/native/CLI case in 22.24s, exit 0.
The original coding and live-PID lease cases had already passed Native4.
The current complete workspace run excludes only analytics and is collecting
its terminal result. It found a reproducible Ticket adapter regression:
the bounded file wrapper selected Agent defaults instead of the explicitly
selected per-Run tool executor. A test with empty Agent defaults and a trusted
Task-only Run therefore exposed no Task schema. The wrapper now retains the
Run-selected executor before applying its existing Task/Read/Write ceiling and
Host-bound permission/context checks. No authority or tool allowance expands.
The existing worker-loop/private-plan regression directly covers this fix.
Evidence: `../1481-evidence/1481-workspace-f00bd52e.log` and
`../1481-evidence/1481-adapter-failure-backtrace.log`; post-fix worker verification
remains pending. No repeated full-workspace or review run was started.

The continuing workspace run found one further fixture mismatch: the strict
Root catalog test expected the six new `work_*` entries from the shared
orchestration allowlist, but its static executor never registered those schemas.
The baseline test itself is unchanged; the allowlist additions belong to #1481.
This is tracked as our integration failure, not pre-existing #1120 debt.
The fixture now supplies all six schemas while retaining the exact allowlist
assertion and forbidden-tool checks. Production catalog filtering is unchanged.
Focused verification of this fixture is queued after the running worker/browser
acceptance, avoiding any source/binary replacement during actual Host tests.

The same original workspace run also caught our Ticket HTTP error adapter
returning a flat error string. It now uses the existing canonical `json_error`
helper, preserving status codes and exact conflict/authority messages. A wire
regression covers 409, 403, 422, 423, 503 and cursor-gap 410 envelopes; the existing
native-source tripwire remains unchanged. Lotus already parses nested canonical
messages, including exact revision conflicts. This is a bounded integration fix,
not a new error protocol. Validation is queued after the original three runners;
their earlier source/binary tags and failures remain preserved.

## MacBook bounded review corrections

The independent review of 32d6e4a identified three required fixes. Reused immutable objects now flush their parent directory before any HEAD acknowledgement, including retries after object/manifest/commit rename followed by failed directory durability. Explicit stated approval amounts must equal the complete action value; substring values and ambiguous multi-value clauses remain pending clarification. User-required acceptance shares the conditional-language safeguard used for approvals.

PASS: all 78 Ticket tests with all targets and test-utils, format check, and strict Clippy (no warnings), at 32d6e4a plus the four-file verified overlay. The new failure/retry fixture retains the old published snapshot until each reused directory flush succeeds and then verifies the reopened new snapshot. Decimal/extended/negative amounts and conditional acceptance do not authorize a mutation. The initial new-test compilation failure (Snapshot has no PartialEq) is retained; canonical snapshot-byte assertions passed the complete rerun. Actual power-loss and external-action exactly-once behavior remain outside these observations.

## Stable Root ownership dependency adoption

Reuse merged PR #1501 at fbf9067adc3faa1db8f0d8b8ae7d013a4683ec40 and its already merged prerequisites, as a local patch over d99940d6. The original #1479/#1480 baseline remains 685328ec; no owner implementation is rewritten. Fourteen files overlap with #1481; thirteen apply without conflict. The chat workflow conflict retains the stable post-transaction activation and the existing queued-ingress duplicate-publication guard. All non-overlapping upstream files must match the stable source exactly. This adoption is not A12 acceptance; the historical Root fixture and affected integration checks are still pending.

## Completed plain historical Root import on stable ownership ports

The historical import adapter consumes the merged #1501/#1488/#1489 Actor and
Root proof without implementing another ownership protocol. It validates a
frozen full file tree, including initialized Actor census, birth, Project and
terminal activation. Root authority deliberately strips messages, so transcript
eligibility separately reads the already validated Main bytes. A plain
zero-tool completed local Root may contribute immutable history only. Initial
provider route metadata is permitted; native provider output groups, resets,
route switches, tool history, compaction, children, unknown and active execution
remain read-only. The legacy logical loop address and physical Actor Run UUID
retain separate meanings. Import never reactivates or transfers the old Run.

The historical preview emits `complete_root_files_v1`; its reviewed hash and
Artifact cover every bounded canonical source file, including Runtime,
Actor/census and Root tool-authority proof. Inert import retains its existing
`canonical_session_v1` format. Old completed Tasks become blocked needs-review,
with no current/accepted Submission. Receipt replay precedes new eligibility
and CAS checks, while a live Ticket writer blocks the offline import.

Actual Host/CLI historical acceptance passed on be748403 plus the two-file
A12 overlay: real /chat and /execute, terminal physical Run, actual Host stop,
Main-history/census/birth/expired-run rejection, reviewed full-tree import,
identical receipt replay, exact original Actor preservation and Host restart
without old Root activation. Evidence:
`../1481-evidence/1481-macbook-a12-be748403-external-v10-historical.log`.
Earlier fixture failures are retained. The four existing offline Host cases and both actual migration/feature rollback
cases also pass on this stable stack (7 tests total). Final integrated regression
and browser checks remain pending at this entry.

## Conditional context validation follow-up

Positive approval and user-required acceptance now retain comma-separated
conditions from the surrounding Human sentence. A model quote such as
`确认验收 A` cannot authorize `如果 CI 通过，确认验收 A`, and trailing English
conditions have the same guard. A separate semicolon-delimited explicit action
keeps its own evidence. Nineteen deterministic semantic tests, format and strict
all-target Ticket Clippy pass at 9c485842 plus the verified two-file overlay.
The tests cover both condition orderings, Chinese/English and example text,
and confirm that the unrelated conditional B clause does not block explicit A
approval. This extends the existing review's conditional approval/acceptance
fix; it adds no model dispatcher or new paid model evaluation.
Evidence: `../1481-evidence/1481-macbook-semantic-context-9c485842-status.json`.

## Exact Work/request title boundary closeout

The negative-only fixture on unchanged 6f500e4c reproduced a wrong approval:
`批准 CA` committed the current A request. The validator now matches complete
Work/request/submission references, rejecting prefix/suffix fragments and a
short title contained in a different known longer title. Explicit no-space
Chinese clauses such as `批准A` and `确认验收A` remain supported. Uncertain
references require clarification; no semantic dispatcher or authority expands.
The reproduction remains as a failure log, and the complete changed semantic
suite passes 20/20 with format and strict all-target Ticket Clippy.
Evidence: `../1481-evidence/1481-macbook-title-repro-6f500e4c-status.json` and
`../1481-evidence/1481-macbook-title-guard-6f500e4c-status.json`.

The single full affected workspace run at 6f500e4c completed unchanged with
9,919 passes, zero failures, 154 ignored cases across 154 targets; analytics is
excluded and live-model tests are ignored. That exact-head evidence is retained.
This later two-source-file guard has its own focused regression above; the
complete workspace is not rerun or claimed for the later head. Final actual
Lotus/Host browser validation follows against the guard commit.

## Final actual browser verification

Actual Host and built Lotus acceptance pass at backend 39bb3272 and frontend
291363e1: Host 1/1, Playwright 1/1, E/B/D/A/C correctly answered, five current
generation-2 submissions, stale approval status 422, and no browser errors.
The test binary and bundle hashes are recorded in
`../1481-evidence/1481-macbook-final-browser-status.json`; source stayed clean.
This completes the supported local closure. Complete Rust regression remains
correctly attributed to 6f500e4c, with the later title/ID guard's 20-case focused
and strict-Clippy results recorded separately. A subsequent documentation-only
commit does not change the tested backend or frontend. Fresh current-head
backend independent/model evaluation is unrun; defaults remain off and no
remote operation was performed.
