# Shared browser storage boundary preparation

The source now includes macOS first-use preparation, bounded-storage admission
and explicit native-root waiting. Historical head `d9154725` had no hard bound;
that description does not apply to the current implementation. The later
`b6df19bb` startup-permission slice passed its compile, Clippy, fmt and eight
focused Rust checks. Those results do not qualify a newly merged dependency
graph or the product's actual first-use/restart path. The failed product
first-use fixture and the partial Chromium capacity experiment remain separate
evidence; neither is a passing product acceptance.

The remaining merge assessment concerns those actual behaviors, not merely
whether an old finding is outdated. No production migration or all-writer
containment acceptance follows from Node tests, and a successful compile-only
CI check is not a full Rust test run. The later compatibility source restores
explicit temporary clean on Windows/Linux without APFS or a persistent profile;
shared remains unsupported. The new source's Rust/native acceptance is pending.

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

`scripts/browser-storage-envelope.cjs` re-exports the canonical plan and read-only
inspector included in `browser-runtime/host.cjs`; requiring either does not load
Playwright or launch a native process. The proposed backing is 536870912 bytes, with a single APFS volume
quota of 402653184 bytes. The one shared account profile and all temporary clean
context data share that envelope, so each is bounded by the same aggregate
ceiling. These are the current macOS source defaults, not two independent
per-profile quota levels or a claim of completed native acceptance.
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

## Local production-admission candidate

The production `BrowserManager::with_profile_root` selects bounded storage.
`browser/storage.rs` prepares and admits only a private fixed backing and mounted
path set. `browser/storage/prepare.rs` is the Mac native preparation owner.
It pins the backing file and opened directories
with the existing stable account lease. The old derived account directory holds
only control locks/fences, outside the quota filesystem. An old cookie/database
entry causes `browser_storage_migration_required`, without reading or moving it.
All profile, HOME/XDG, temporary and download paths are passed to the host inside
the envelope; no production ordinary-directory fallback exists.

Before loading Playwright, the host independently reads native backing/quota
identity through the canonical inspector and rejects inconsistent environment,
control or scratch paths. Readiness must advertise the exact fixed-storage
capability, so an older v2 host cannot silently ignore the boundary. A missing
Mac provider returns `browser_storage_provider_not_ready`; unqualified Windows
or Linux persistent implementations return `browser_storage_provider_unsupported`.
The separate explicit clean provider advertises only `clean`, validates its
owned temporary path, and never accepts persistent/profile settings. The native
parent rejects shared before creating a chat directory or host. It uses the
existing v2 scope routing and clean contexts in one temporary Chromium instance,
not an unbounded persistent-account fallback. A new compatibility marker and
exact readiness check reject old hosts before use. This provider makes no
hard-quota or all-descendant-exit claim; its platform runtime checks remain open.
The default Mac shared path now includes first-use creation and clean-reboot
reattachment in source. It no longer depends on a manually prepared mount.
Preparation runs in a blocking worker that owns the actual account/reset lease,
so cancelling an HTTP waiter does not release locks under a running disk step.
An absent/empty unregistered root may create one 536870912-byte UDIF/UDRW image,
with a single 402653184-byte quota set at initial APFS format. No overwrite flag,
sudo, existing-volume resize or default data migration is available.

A private bounded pending record is synced before create or attach. A command
failure, timeout or uncertain result keeps that fence: a new opener and reset
cannot blindly replay the operation. A ready record binds the backing inode and
volume UUID. After a normal reboot, only that same registered file may be
reattached; a missing/replaced file is never recreated as an empty account.
An existing unregistered image is refused rather than silently adopted. A
crash with an unfinished launch or disk mutation retains its fence; this is
clean-restart recovery, not an automatic repair of an unconfirmed old writer.
Read-only OS inspection rejects an image attached elsewhere, duplicate
attachments, additional APFS volumes, sparse/segmented backing and wrong quota.
Existing role directories are validated, not repaired or replaced. Only a
newly formatted, native-verified empty volume receives new private role folders.
Privacy tightening applies only to that new volume/new image, never to an old
user directory. A non-writable-by-others owned data-root mode of 0755 is accepted
without changing it; storage and control directories remain private 0700.

Preparation readiness is distinct from writer admission. A full reattached
volume can become ready for explicit reset, while live launch inspection still
refuses before the active launch fence, Node or Chromium starts. The free-space
floor is a pressure check, not a quota claim. The kernel volume quota and fixed
backing supply the candidate hard bound. The Node inspector is still read-only;
all create/attach execution stays with the native leased owner.

Ordinary Rust unit fixtures cannot run image mutations: their compile-time-only
runner records an attempted step and refuses it. Real source-matched CLI
preparation must be qualified separately against approved isolated targets;
component compilation and lifecycle tests do not replace that acceptance. No
test needs production deployment or removal of an old failure fence.

Before spawning a native writer, the account owner now writes and syncs a
bounded launch UUID fence outside quota. A new process refuses any old active
fence even after all previous flock descriptors have disappeared. The host
requires that exact generation and a private, bounded marker. The UUID is
lifecycle correlation, not authentication or proof of writer exit. Only the
same lease may clear it after the native close barrier. Normal completion also
requires the managed process group to be absent; EPERM, live members or an
unknown group retain quarantine. This does not prove escaped descendants.

## Explicit reset source candidate

The reset gate is retained by the blocking purge worker, including after its
HTTP waiter is cancelled. Before canonical history can be wiped, the Mac owner
pins existing backing/role directories, rejects unknown active-writer fences,
joins live native image/quota/UUID readback, and durably writes a pending reset
record outside quota. A full volume can be inspected for reset without making
it eligible for launch. Windows/Linux have no qualified reset provider.

Only a successful canonical wipe reaches erasure. It unlinks contents of the
seven known role directories using held directory descriptors and no-follow
relative operations. It preserves the role directories, the fixed image and
stable lease inodes. Unexpected devices, replaced pins or an incomplete
traversal fail closed. Unknown top-level data is refused rather than silently
left behind under a success receipt. No image is detached or reformatted.

Failure retains the pending record and refuses a new browser. A subsequent
explicit reset may retry under the same exclusive gate and freshly verified
volume UUID; startup does not automatically replay erasure. Completion replaces
the pending record atomically with a bounded completion record. A crash leaves
either the durable pending fence or the identity of already-completed erasure.
Later admission binds that UUID to fresh native readback. The private Node
inspection helper is read-only and does not load Playwright; it grants neither
erasure nor owner authentication. Actual erasure remains in the native owner.

Pinned Playwright is loaded only after a scoped spawn hook is installed. The
hook captures the actual native ChildProcess returned for the one expected
profile; another profile or a second physical root is refused. Normal cleanup
waits that root's close event and zero status after Playwright close fulfills.
Production also reads its actual PID/PGID and requires a dedicated host group
before overriding Playwright's detached root launch with group inheritance.
This targets the preserved Node-group counterexample; it does not use
`--single-process` or merge renderer/GPU processes. Unconfirmed cleanup requests
a stop through the captured ChildProcess handle, never a stale numeric PID.
Playwright's own detached-group force-kill assumption is no longer sufficient;
the captured-handle path and native owner's existing deadline/group fallback
therefore remain required. No close deadline is increased by this candidate.
Missing capture, abnormal close or a missing close event preserves scratch and
an unconfirmed outcome. This is a stronger root-wait boundary; it **does not**
prove arbitrary detached descendants exited. Forced paths retain the existing
durable quarantine and cannot use root exit as a permission to detach an image.

Trusted filesystem `ENOSPC`/`EDQUOT` errors are capacity refusals; `EIO` and the
observed Chromium `DataError: Failed to write blobs (IOError)` remain unconfirmed
I/O. An origin `QuotaExceededError` is a separate origin limit, and page text is
never promoted to native ENOSPC. Checkpoint failures preserve the old in-memory
receipt, retire the shared writer and do not automatically retry the write.

Existing synthetic page/EOF component fixtures now select explicit test-only
storage admission. The Rust fixture constructor is absent from production
builds; `test-host-fixture.cjs` is excluded from the three-file published host.
Those real-Chromium page tests still do not establish native quota acceptance.

## Remaining production work

1. Qualify this frozen native admission/reset/lifecycle source with the queued
   affected Rust tests, original compile/Clippy flags and fmt. Node results do
   not compile Rust. Reuse old page/frontend acceptance only for unchanged input.
2. Qualify the actual managed Chromium writer shutdown path against the retained
   approved synthetic fixture. No test requires deploying on a real profile.
   Reset filesystem failure/retry tests use private synthetic directories; the
   backing and old synthetic baseline must not be erased to run those tests.
   The three-file published host format and existing locks/epochs stay intact.
3. Revalidate the outside-quota control fence and stable account lock through
   full-volume errors, restart and unknown writer recovery. Keep metadata
   bounded; do not turn it into a page-data journal or clear it on uncertainty.
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
7. Qualify the implemented Mac first-use/reboot path and its cancellation/failure
   boundary before enabling it. This is a runtime proof requirement, not a
   requirement to perform a production operation for code merge. Windows/Linux
   still have no storage provider: refusal preserves their files but disables
   the new browser path, and is not nonbreaking cross-platform support. The
   minimal current delivery is Mac shared/clean persistence with its required
   bound; no Windows/Linux driver, quota provider or platform rollout is added.
   Do not call a deliberate unsupported boundary a completed implementation for
   those systems, or silently revert shared mode to disposable storage.

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
