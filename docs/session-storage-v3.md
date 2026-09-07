# Session storage V3 shadow evaluation

`bamboo_storage::v3::SessionStoreV3` provides an opt-in SQLite WAL store for captured session snapshots. The server and SDK continue using their existing V2 authority. Opening a V3 database does not switch a session, launch a run, modify V2 files, or migrate user data.

The store keeps messages in ordered rows and stores control state separately. A full snapshot transaction commits changed message rows together with the provider transcript and inbox admission proof. A runtime-only transaction accepts the message-free snapshot returned by `load_runtime`; it does not load or rewrite message rows or provider history.

Full snapshots use a pair of history/runtime revisions. Runtime writes compare only their runtime revision, while immutable creation identity and Project metadata revision checks remain required. History pagination includes a revision, so a client cannot silently combine pages from different histories. Independent SQLite connections compete through an immediate transaction; stale writers receive `V3Error::Conflict`.

## Captured-tree import

Capture one complete logical tree as a JSON array of current `Session` values. Then run:

```sh
cargo run -p bamboo-storage --example v3_shadow -- session-tree.json shadow.sqlite
```

The target must have no existing session ids from the import. The importer checks the root, parent closure, duplicate ids and cycles, then inserts the tree in one transaction. Any conflict rolls back the entire import. The example reloads and compares every imported snapshot using canonical JSON and reports success only when all snapshots match. This comparison describes the captured source; it is not a live cutover guarantee when another process continues writing V2.

SQLite connections are retained by the store. The API is synchronous for offline/shadow workloads; async applications must invoke it on a blocking worker. The database has a distinct application id and schema version and rejects unrelated databases.

## Current boundary

This slice implements the transactional store, revisioned reads/writes, and verified shadow import. It does not select runtime read/write authority. Attachments remain unchanged message payloads or existing references; referenced files are not copied into a new attachment store. Search indexing, fork/delete lifecycle, live catch-up and authority switching remain on the current V2 path. V3 has no destructive rollback command. Do not treat disabling a future selector as a rollback procedure.

Tests cover V2 captured-tree equivalence, reopening, admission proof retention, per-row edits and truncation, revisioned pagination, concurrent CAS, Project/creation fences, and whole-tree rollback. Synthetic measurements must be reported with their history size and build mode; these tests establish consistency, not production performance.
