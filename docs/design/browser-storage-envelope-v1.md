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

## Approved actual Chromium experiment

The user approved one fresh internal fixture on 2026-10-22 at 13:45:54 UTC.
The plan was used at
`<task-8>/evidence/browser-capacity-envelope-20261022/actual-chromium-fixture/data`,
with `browser-storage-v1/account.dmg` and `browser-storage-v1/mounted`.
Creation preflight required nonexistent targets. Actual OS readback confirmed
the 536870912-byte UDRW backing, 402653184-byte APFS quota and mapped private directories. This
tested the read-only inspector against native OS replies, not injected metadata.

Four sequential real Playwright/Chromium launches used one synthetic profile.
Chromium first wrote 134217728 bytes of Cache API payload. A bounded synthetic
filler in the same volume then reached kernel `ENOSPC` (`errno -28`) after
262443008 bytes. At that point a strict IndexedDB write of a 4 MiB blob was
rejected as `DataError: Failed to write blobs (IOError)`. Chromium also reported
failure to write its temporary disk-cache index. The predeclared driver required
`QuotaExceededError`; its frozen result therefore remains
`PARTIAL_CHROMIUM_FULL_OUTCOME_NOT_PROVEN`. A driver exit of zero does not turn
that acceptance result into a pass, and the test was not rerun to change it.

After normal browser close, only the explicit synthetic filler was removed.
The profile, cookies and cache were retained. Two subsequent normal launches
verified the baseline cookie/localStorage/IndexedDB data and a newly committed
recovery marker. All four actual browser roots emitted exit and close events
with code zero. Their observed native processes/groups were absent before
normal `hdiutil detach /dev/disk10` succeeded; readback found no remaining image
attachment or mount. No forced stop, forced unmount or administrator operation
was used. This normal-close fixture does not prove arbitrary detached-writer
containment, crash durability, another operating system or the Bamboo product
integration.

Logical and current allocated backing sizes were both 536870912 bytes through
the experiment and after detach. The retained backing SHA256 is
`2f6d73ef855db3c26a5f1b6ce59379fdf20b1299f04e36f4e1edbfeac457ae55`.
The new file adds 512 MiB nominally to the two previous retained 512 MiB images.
Raw capacity errors, browser process events, recovery probes and detach
readback are retained under the fixture's parent evidence directory.

The experiment supplies evidence for this Mac layout; it does not implement
production storage bounds. Production auto-mounting, default storage policy,
platform support and any existing-data migration remain separate concrete
decisions. The capacity finding stays open until the runtime enforces the
boundary and the remaining production checks above pass. Real profile data was
not read, copied, migrated or deleted.
