# Supervisor rejection and rework contract (#2023)

This is a bounded regression for the delegation-first policy delivered in #1988.
It does not replace #902's real-model behavior evaluation, #1968's capability
tracker, or #1976's broader session-management work.

## Production contract

The trusted Supervisor delegates implementation, compares returned artifacts and
evidence with the assignment's acceptance criteria, rejects an exact failed
submission, communicates a bounded correction, and reinspects the corrected
result. Its technical verification does not grant final User acceptance. The
existing `user_acceptance_required` flag and all tool/identity/permission checks
remain authoritative. This change only clarifies that prompt contract; it does
not add a model quality classifier or broaden a capability.

## Deterministic runtime coverage

`tests/ticket_supervisor_acceptance.rs` starts a real isolated Bamboo HTTP Host
and production native Workers using the existing Ticket fixture. All model
responses come from a local scripted HTTP provider and use synthetic settings.
It repeats the following scenario in Standard and Ultra:

1. The Supervisor resolves a canonical Human instruction, creates a Task-only
   Work with explicit exact-byte/SHA256 acceptance criteria, and dispatches it.
   Its reply distinguishes admission from completion.
2. A real Worker updates its private Task plan and returns deliberately wrong
   bytes. Host postprocessing produces the canonical artifact, exact submission,
   generation/run provenance, evidence, and completed-stop fact.
3. The Supervisor reads `work_search`/`work_inspect`, compares the submitted hash
   with the criterion, rejects that exact submission, and updates only the
   objective with bounded corrective feedback. The acceptance criteria and
   required User acceptance remain intact. A fresh `work_dispatch` creates the
   rework attempt; its actual provider context must contain the correction.
4. The corrected Worker result has fresh assignment/generation identity. The
   Supervisor reinspects its evidence and hash. An attempted raw model accept
   receives 403, and the response says final User acceptance is pending.
5. A new explicit Human acceptance names the Work, exact current submission and
   verified hash. Existing semantic resolution commits acceptance. Host restart
   and exact Human replay retain both submissions and do not rerun either Worker.

The test independently downloads both managed artifacts from the actual Host
and verifies their bytes. On every Supervisor request it checks the actual
provider-visible delegation/verification prompt and the absence of direct
implementation tools. Canonical Supervisor tool calls are restricted to the
five Work orchestration/read tools used by the scenario. Worker requests do not
inherit the Supervisor policy.

The test explicitly starts each review turn. It does not establish autonomous
wake scheduling, arbitrary model judgment, arbitrary artifact/file verification,
UI behavior, paid-provider behavior, or a release/platform certification.
A scripted assistant's prose assertions are not a general runtime guarantee
that a free-form model can never overstate completion.

## Service boundaries

`supervisor_rejection_rework_preserves_exact_evidence_and_user_acceptance` in
`crates/infra/bamboo-tickets/tests/service_contract.rs` exercises the real
file-authoritative service with trusted-host test identities. It covers:

- Missing submission evidence, mismatched managed URI/hash, and missing bytes
- Runtime/Worker rejection denial, wrong submission, and empty rejection reason
- Atomic no-change behavior on rejected operations and immutable rejected history
- Exact rejection replay, fresh rework identity, and fenced old Worker authority
- Stale acceptance/rejection, empty acceptance evidence, and model acceptance denial
- Dependency admission only after explicit User acceptance of the corrected result
- Old rejection replay cannot reverse newer acceptance

These service tests do not impersonate a production User through model JSON.
They use the same established host-only authority constructor as neighboring
service tests, separately from the HTTP test's canonical Human-ingress path.

## Focused validation commands

Use repository Rust 1.95.0, the locked dependencies, an isolated target directory,
and the normal test profile. The API-only Host is intentional; no browser/UI is
required for these cases.

```sh
cargo +1.95.0 test --locked -p bamboo-tickets --test service_contract
BAMBOO_FRONTEND_BUILD_MODE=api-only cargo +1.95.0 test --locked -p bamboo-agent \
  --test ticket_supervisor_acceptance --test ticket_supervisor_tools \
  --test ticket_runtime_mvp -- --test-threads=1
cargo +1.95.0 fmt --all -- --check
git diff --check
```

Source-bound execution results belong in the PR admission record. The presence
of this document or these test definitions alone is not a test-pass claim.
