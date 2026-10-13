# Existing model-context representation checkpoint (#1363)

`SessionStoreV2::checkpoint_actor_model_context` is storage-only, no production
caller/admission/ACK. Current Running Actor/full normalized expected/schema1/same
scope+epoch+prepared vector+reset history are required. None/reset/initialization/
extension/repair are unsupported. Pure digest/render witnesses do not establish
Task/Role/Project/provider truth or permission; Host generator binding is separate.
Each borrowed Main/Runtime raw ledger span is capped at4MiB before typed decode
of those exact buffers; private unique-key closed-schema checks leave old readers
unchanged. Checked bounded Write caps candidate JSON at4MiB including Already.
Independent limits:256 events,2MiB rendered text, exact unique Snapshot witnesses
(max256, metadata None, original untrimmed title+content≤2MiB before rendering).
Escaping over JSON cap rejects whole, never truncates. No full Session/request/
process memory bound. Checked revisions/IDs/baselines, fixed-vector anchors and
ordered suffixes are verified; historical Snapshot text stays byte-exact.

Only Runtime.model_context_state changes; Main/native/admission/compact CP,
other Runtime/summary/compression, Actor/proof/index/attachments stay byte-exact.
Physical side messages/native/admission must be empty/default/None before both
New/Already. Full expected uses serde Values and Root stale-budget normalization,
not skipped-index Eq. Owned lifecycle→Task→Session Arcs cover complete filesystem
jobs/cleanup through caller abort/started-job runtime stop, never provider await.
Fresh UTC after BeforeReplace rechecks source/Running lease. Postreplace/sync/
readback errors are OutcomeUnconfirmed: reload actual Session/current fence for
exact Already/no witnesses/no write, never replay a stale fence automatically.
Tests use same-process Stores and real reconcile→V2→cold reopen/fixture scope;
no production/exactly-once claim. #1350 ordinary context rejection stays in force.
