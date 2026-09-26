# Explicit required-packet child reports

`SubAgent.get` has two opt-in views for a one-shot required-packet Child:

```json
{"action":"get","child_session_id":"child-id","view":"result_binding"}
```

After direct-parent authorization, this returns the actual durable Child's
`child_created_at` and `assignment_sha256`. Required-packet create also returns
both in its existing `context_packet` object. Supply those exact selectors:

```json
{"action":"get","child_session_id":"child-id","view":"typed_result","expected_child_created_at":"2026-09-26T00:00:00Z","expected_assignment_sha256":"<64 lowercase hex>"}
```

Only the trusted tool context supplies parent identity. New views reject paging,
message selectors and unknown arguments. Old views reject the two new selectors;
otherwise their existing paging and nested direct-parent behavior remain intact.

Request this report format in the original packet or assignment before binding:

```json
{"version":1,"outcome":"blocked","summary":"Need a decision","reported_evidence":[{"description":"Source inspected","reference":"src/example.rs","sha256":null}],"reported_verification":[{"check":"test","reported_status":"not_run","details":""}],"proposals":[],"blockers":["Choose an approach"],"open_decisions":[]}
```

The entire Assistant content must be one JSON object. All members are required,
including explicit nulls; duplicate and unknown members, prose/fences/trailing
values, wrong types and unsupported enums reject. Outcomes are completed,
partial, blocked or failed. Verification statuses are passed, failed, not_run or
unknown. Summary is nonblank and at most 2048 UTF-8 bytes. Evidence/verification
and each string array contain at most 16 entries. Evidence description/reference,
verification details and array entries are at most 1024 bytes; checks are at most
256. Nullable references are nonblank when present; nullable SHA256 is lowercase
64-character hex. References are opaque reported claims: Inspect performs no I/O
on their paths or URLs and never executes proposals.

The successful response separates `child_report` from `host_observation` with
`kind=durable_snapshot`. Child-reported blocked can coexist with completed Host
status. Inspect checks the current binding, parent birth/lineage/Project and every
recorded source ID/content, including omitted optional sources. It selects the
latest Assistant only, requires the existing completed/after-last-User predicate,
and rejects nonplain, Commentary, tool-call or compressed replacements. It never
falls back to an older parseable report.

Parent/Child full Store reads are separate snapshots. This is neither atomic live
authority, callback/run attribution, verified evidence nor a new permission grant.
No worker is provisioned, retried or adopted by Inspect. Legacy/unbound and
resident state is unsupported; propagated Store decoding failures are ordinary
tool errors. Reads retain existing full-Store semantics: an ordinary Child's
corrupt runtime sidecar can fall back to its valid Main snapshot. This slice
does not add a stricter raw-file reader or repair that adjacent fallback.

Expected content failures return only view/version/available=false/reason:
`typed_result_unsupported`, `result_binding_invalid`, `stale_result_selector`,
`stale_parent_context`, `report_absent`, `report_not_current_final`,
`report_malformed`, or `result_budget_exceeded`. A stale selector never returns
replacement selectors automatically. Explicit discovery is a separate decision.

Raw Assistant parsing is capped at 8192 bytes. Both the escaped compact payload
and the actual serialized ToolResult (including its result String's second escape
layer and display fields) must fit 8192 bytes. Overflow returns the small
unavailable envelope, without shortening claims or dropping provenance. Use
unchanged legacy result/message paging for larger or malformed prose reports.

Acceptance fixtures extend the real compiled native serve/current_exe worker
path and then stop/reopen its Store for actual SubAgent tool inspection. Only the
remote model is faked. The host test thread's explicit stack size is fixture-only;
production native host and worker retain default stack settings. Source fixtures
and static assertions are not test execution receipts.
