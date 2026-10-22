# Shared browser storage boundary preparation

This is a proposed adapter contract, not an enabled production backend. The
current shared browser still has no hard storage bound. The open capacity
finding on PR #2102 continues to block merge. No image creation, attachment,
quota mutation, production migration or native writer acceptance follows from
the Node tests in this change.

## What changed with shared mode

There is one physical browser and one persistent account profile per canonical
Bamboo data root. New chats no longer multiply persistent profiles. Clean
contexts use that browser and do not create another durable account profile.
This removes a growth multiplier, but a single page can still retain large
IndexedDB/cache data. A maximum chat count, faster scan, cache-size flag or
admission reservation is not a limit on filesystem allocation.

The minimum hard-boundary route therefore needs one bounded envelope for that
physical owner's profile **and all its other writable paths**. A fixed backing
also needs a total allocation limit; a sparse image's virtual size alone does
not establish its physical footprint. Old per-chat directories remain untouched
and are not silently imported, evicted or counted as newly bounded data.

## Prepared component

`scripts/browser-storage-envelope.cjs` supplies an inert Mac plan and a read-only
inspector. The proposed backing is 536870912 bytes, with a single APFS volume
quota of 402653184 bytes. The one shared account profile and all temporary clean
context data share that envelope, so each is bounded by the same aggregate
ceiling. This is a candidate based on the small synthetic experiment, not an
approved production default or two independent per-profile quota levels.
`imageBytes` is the logical backing-file bound, not proof that host snapshots,
clones, filesystem metadata or retained old extents fit in that many physical
bytes. The actual backing allocation and those overheads need separate target
storage acceptance. No backup exclusions or host snapshot policy are changed.

The plan maps profile, HOME, XDG cache/config/data, TMPDIR/TEMP/TMP and downloads
to that volume. The inspector joins actual `hdiutil imageinfo/info` fields with
`diskutil info/apfs list` and filesystem path identities. It rejects missing or
wrong quota, full volumes, sparse/segmented/oversized backing, wrong image or
volume identity, an ordinary directory, aliases/symlinks, a path on another
device, wrong ownership and unknown fields. Query errors never select a plain
directory fallback. A missing backing fails before OS queries or file creation.

The exported result deliberately says `launchAuthorized: false`,
`writerExitProven: false`, `detachAuthorized: false`. A caller-provided JSON
object, previous receipt or successful structural validation cannot authorize
launch or become a replacement for the trusted adapter's live, leased OS
inspection. The script has no provision, attach, detach, quota-setting or sudo
execution operation. Its proposed command arrays are review material only.

## Remaining production work

1. A native owner must hold a stable process-shared storage lease outside the
   mount, create/open the one backing under an explicitly approved policy, and
   bind current OS identity to opened paths before browser readiness. Existing
   unbounded data must cause an explicit migration-required refusal, not an
   automatic copy, truncation, reinitialization or second backing allocation.
2. `BrowserSession::spawn_protocol` currently allocates its scratch under the
   ordinary host temporary directory and inherits HOME. It must use the verified
   envelope for every owned writable path. Profile data may move only through
   the approved adapter; stable lifecycle/reset lock inodes stay outside the
   mount. The three-file published host format must remain coordinated.
3. Unknown-writer/close-pending metadata must remain durably writable **outside**
   the potentially full profile filesystem. Otherwise ENOSPC can prevent the
   fence itself. Metadata remains a small fixed per-account record, with no page
   payload, rather than an unbounded journal. Existing marker/lease recovery and
   reset ordering must be retained and revalidated for the new paths.
4. Quota/full/I/O errors must preserve existing data and report an unconfirmed
   operation. No automatic eviction, changed timeout, retry or unbounded
   directory fallback is acceptable. APFS-full recovery and checkpoint failure
   need actual Chromium tests; a native file writer reaching ENOSPC is not that
   acceptance.
5. Node/Node-PGID termination still does not prove all Chromium writers stopped.
   Keep the existing detached-writer counterexample and quarantine. No detach,
   cleanup, migration or new owner may rely on Node exit alone. Hard capacity
   would remain enforced while an unknown writer is quarantined; it does not
   establish process-domain containment or grant permission to force-unmount.
6. Windows/Linux need a proven platform provider or an explicitly accepted
   unsupported/fail-closed behavior. This Mac proposal cannot certify their
   native runtime. No driver, privilege grant, system quota or mount setup on
   those platforms is authorized by this preparation.

## Concrete next grant, not executed

A bounded actual-Chromium storage experiment could use only a fresh internal
fixture at `<task-8>/evidence/browser-capacity-envelope-20261022/actual-chromium-fixture/data`.
Its image would be `browser-storage-v1/account.dmg`, its private mountpoint
`browser-storage-v1/mounted`, relative to that fixture. The exact preview is
generated by `macSharedStoragePlan`; fixed logical backing is 536870912 bytes
and APFS quota 402653184 bytes. It must refuse an existing target or insufficient
space, read back all identities, put synthetic browser/profile/cache/temp/
download paths inside the envelope, and use only finite synthetic writes.

This is a **new** grant request: it includes creation + mounting + actual
Chromium child processes and bounded site-storage writes, followed by normal
unmount only when all owned writers are confirmed stopped. It does not include
killing Node while leaving an uncontained native writer. Authentication,
permission failure, uncertain completion or busy detach must stop and retain
the fixture; no sudo, forced detach, overwrite, reuse of the old experiment,
real profile, outer-disk operation or production layout change. The new retained
backing adds 512 MiB nominally to the previous two retained 512 MiB images.

Production auto-mounting and existing-data migration require a separate concrete
decision. This experiment, even if later approved and successful, would only
answer storage behavior for its tested Mac fixture.
