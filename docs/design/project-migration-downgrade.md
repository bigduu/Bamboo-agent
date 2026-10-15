# Project migration, interrupted publication, and older readers

`project.json` is authoritative; `projects/index.json` is derived. This contract
applies to the schema-1/2 to schema-3 migration in `bamboo-projects`, including
the sidebar fields introduced by #1966, and the future-version rejection in
#1985. It does not introduce a downgrade, rollback, or automatic merge protocol.

## Publication boundary

Migration runs under the existing per-Project `.project.lock`. In order, the
current reader preserves the exact legacy bytes in `project.json.migrated-vN`,
writes the schema-3 recovery backup, advances the monotonic revision floor, and
atomically replaces the primary with schema 3. Existing writers complete this
migration before a sidebar/import mutation can be acknowledged.

After completed publication, the primary and recovery backup are both schema 3.
The archived schema-2 ProjectStore at
`48e1283fe6ce94078a2cb9554e75f0552c3fc52c` rejects both copies; ordinary old `get`
and `update` cannot rewrite their canonical bytes or the revision floor. Its
startup may still rewrite the derived index or create diagnostic quarantine
files. A current reader rebuilds the index from the supported manifests.

A newer primary version is a separate boundary: #1985 returns a typed
`UnsupportedManifestSchema` before recovery and preserves its canonical files.
This does not modify already-shipped readers.

## Interrupted publication

A schema-2 primary containing sidebar data was produced experimentally, by an
intermediate writer, or by manual extension; the archived official schema-2 DTO
has no sidebar field. Before the primary is replaced, that old binary can still
read it and discard fields it does not understand:

| Durable file-state prefix | Old `get` | Old metadata `update` |
| --- | --- | --- |
| Legacy primary, schema-3 backup, old revision floor | Reads the legacy primary | Can rewrite both copies without unknown fields |
| Legacy primary, schema-3 backup, advanced revision floor | Revision normalization can rewrite both copies without unknown fields | Can also rewrite the legacy data |
| Schema-3 primary and backup | Rejects; canonical copies stay unchanged | Rejects before the mutation callback |

A revision floor is a CAS fence, not a format fence to that old reader. Reversing
the primary/backup order instead leaves an old-reader backup recovery path.
An added marker alone cannot constrain an unmodified old binary. Stop all old
writers during migration and do not switch to an older binary after an
interrupted migration. Transparent mixed-version writes before publication are
outside this contract; supporting them requires a separately designed cutover
or coordinated old-writer change.

## Current-reader retry and preserved evidence

The first legacy snapshot is retained as raw bytes, including fields the old
reader does not understand. A current-reader retry compares the still-legacy
primary with the existing snapshot before replacing that snapshot or publishing
any backup, revision floor, or primary:

- If no snapshot exists, preserve the current legacy bytes through the existing
  atomic writer, then continue migration.
- If the snapshot is byte-for-byte identical, resume migration. The revision
  floor remains monotonic; a completed schema-3 manifest supports normal updates.
- If the bytes differ, return `LegacyMigrationConflict` and preserve all copies.
  The mutation callback is not invoked. Startup may skip the conflicted Project
  while rebuilding its derived index; direct access continues to report the
  conflict. Registry overlap checks still fail closed on unreadable authority.
- A snapshot must be a confined regular file. A symlink or other non-regular
  path is rejected without replacing it or writing through it.

This prevents a current retry from erasing the original snapshot after an old
reader has already rewritten the primary. It does not recover fields into an
active manifest, discard the old writer's metadata changes, or treat diagnostic
bytes as rollback authority. Preserve the whole Project home and resolve the
divergence explicitly before retrying; an automatic choice between the two
copies would be a different recovery protocol.

Corrupt-primary recovery continues to use its supported backup under the
existing revision-floor rules. It does not merge data from a forensic snapshot;
the snapshot remains preserved diagnostic evidence. A backup already rewritten
by an old reader can therefore lack its former unknown fields even though the
original snapshot is retained. Restoring those fields into a canonical manifest
requires an explicit reconciliation decision, outside this guard.

## Verification and assumptions

The compatibility harness uses the actual archived schema-2 ProjectStore and
domain sources, verifies their exact bytes against the Git commit, and runs
public `get`/`update` against disposable data. Fixtures represent the durable
file-state prefixes above and are installed after opening each reader, so the
`get` case exercises revision normalization directly. Baseline and repaired
receipts retain primary, backup, floor, original snapshot and source hashes.

Package regressions cover identical-prefix resume, a divergent legacy primary,
startup/index skipping, preserved original and current edits, mutation rejection,
and file confinement. The existing schema migration, corrupt-primary recovery,
future-version and CAS tests remain required.

Run the manual archived-reader check only with a coordinated build window and
an independently owned target and artifacts directory:

```sh
python3 scripts/project-migration-compat/reproduce.py \
  --repository "$PWD" \
  --legacy-source /path/to/unmodified/48e1283/source \
  --artifacts /path/to/a/new/owned/run \
  --target /path/to/a/dedicated/build/target
```

The script verifies the two archived crates and their workspace manifest against
the exact Git commit, builds both drivers offline with one job and private
intermediate/temp directories, and retains generated lockfiles, source and binary
hashes, toolchain, commands/logs and all six fixture results. It always rebuilds
the selected source before reporting a result. A baseline reader that overwrites
the original snapshot produces a nonzero RED result; the repaired retry guard
must preserve that snapshot and reject every divergent legacy state without
changing its other canonical files. The standalone driver lockfiles are separate
from the repository's locked package regression; neither result substitutes for
the other or for release acceptance.

These are process-interruption/file-state tests, not power-loss certification.
The implementation retains its existing atomic file replacement and fsync
ordering; it is not a transaction spanning all files. Cross-process guarantees
assume writers honor the same lock, the supported local filesystem semantics,
and a stable Project home. Uncooperative file editors, manual restoration,
data lost before the first snapshot, damaged/lost snapshots, older binaries
changing their own snapshot files, or
filesystem/platform behavior outside those assumptions are not covered. No
personal data root, existing App, provider, or published artifact is used by the
harness. Release-platform and packaged-artifact acceptance remain separate.
