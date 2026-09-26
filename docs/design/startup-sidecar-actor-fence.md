# Startup Child sidecar reconstruction fence

`SessionStoreV2::migrate_runtime_sidecars` is the existing startup migration;
`bamboo_server::app_state::init::init_storage` is its sole production caller.
Startup still reports migration failures nonfatally. It does not reconstruct a
protected missing Child runtime from an older embedded main projection.

## Final candidate boundary

The existing index and migration marker are routing hints. There is no outer
Task guard. Each candidate acquires lifecycle shared, Task shared, then the
exact Session writer guard. Before opening a hinted target, its id and canonical
relative layout must agree. Under the final guard set, the migration checks the
real target directory, runtime presence and raw main file again. Only an actual
`NotFound` means absent; other errors and nonregular targets reject. An existing
regular runtime is skipped, preserving concurrent legitimate publication.

The raw main must match the hint's target kind/root/parent/depth/birth and its
canonical path. Local typed Actor identity validation rejects invalid lineage.
The index never supplies the reconstructed context. Existing Root lifetime,
Root context and Supervisor checks remain in place; missing Root or Supervisor
authority cannot be reconstructed here.

The accepted default-writer observational classifier admits both Actor files
absent (legacy) or complete valid matching inert Cold / attempt zero / no
activation authority. Activated, completed, failed, cancelled, retired (also
retired attempt zero), partial, corrupt, nonregular or mismatched authority
rejects before runtime publication. Classification calls no Actor ensure,
repair or refresh operation. Arbitrary external erasure of both Actor files
while retaining a Session remains indistinguishable from legacy and is outside
this contract.

## Physical publication and partial completion

The existing runtime projection still removes messages, native provider
transcript and Inbox admission data. Main history remains byte-identical.
Runtime replacement uses the accepted complete std filesystem job (unique temp,
write, file fsync, replace, directory fsync and cleanup). That job owns the same
Arc guard holder through physical completion, including caller abort or runtime
shutdown. Guards release in Session, Task, lifecycle order. A successor activation
claim cannot pass a parked job and then receive its late stale publication.

This guarantee applies to each started candidate job, not the entire async loop.
A cancelled loop may not start later candidates or marker publication. The
existing marker remains last, with its existing tmp/rename protocol. A failed
run may leave earlier sidecars completed; rerun skips them and processes the
remaining admissible candidates. An error after replace may have changed bytes
without confirmation. There is no all-file rollback or new recovery journal.
Missing or malformed main retains the existing skip policy; the migration marker
is not a comprehensive authority audit.

## Verification boundary

Focused tests exercise the public migration, actual raw files, scan-to-lock races,
physical FileExt probes across caller abort and runtime shutdown, precreated
independent Stores and real successor claims, replacement errors/cleanup and
partial rerun. A server integration test invokes the unchanged public initializer
and verifies protected runtime stays absent with nonfatal startup behavior.
Execution receipts must state the tested platform; source portability is not
native Windows/Linux evidence. Existing Windows replacement behavior is retained.

This writer fence does not redesign Child history reads, which may still fall
back to main when runtime is absent. Task CAS/undo (#1354), Supervisor management
(#1355), default saves (#1350), fenced append (#1351), Inbox, runtime/provider
admission, global caches and additional migration protocols remain independent.
