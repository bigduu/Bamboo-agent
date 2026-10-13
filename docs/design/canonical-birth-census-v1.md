# Canonical birth census v1

`SessionStoreV2::canonical_birth_census` is an internal observational port. It has no production acting caller and is not a generic Storage trait or public DTO. It scans the actual same-id Root slot and every Root's same-id Child slot without consulting the index or caller-supplied kind/path. Every physical candidate counts before eligibility; two candidates conflict even if one is empty. Empty, partial, marker-only, Actor-only, revocation-only or residual evidence never becomes vacancy.

## Supported evidence

Only a complete ordinary Root/Child Main/Runtime pair can produce `PresentIdentity`. Both bounded buffers must agree on id, kind, root, parent, depth, birth, exact typed Project and ordinary authority. Empty Root root-id normalization is allowed only at its own Root slot. Full compact framing/flat validation uses the captured Main buffer; this port never calls the old async loader or a Runtime fallback. Raw Project precedence is preserved, while malformed higher-priority values, duplicate Project fields and whitespace are rejected. Actor derivation's compatibility trimming is not authority.

Actor record and initialized marker are either both absent or a closed, validated matching pair, including an independent Project comparison. Missing members do not reset Cold0/Retired0. The same-id revocation uses a census-only three-field decoder and reports recorded evidence without deciding whether the birth is live. Existing witness writers/readers remain unchanged.

Root/Supervisor proof presence, sealed identity, management state, Ultra, unknown authority/residue and unsupported file types are Unsupported. Every actual `save_session` Root publishes a Root proof, including Standard, and is therefore Unsupported. A proofed parent Root does not exclude the target Child's own ordinary candidate. The proof-free Root positive is explicitly a synthetic legacy physical test fixture published after precreated Stores finish migration; it neither removes an actual proof nor claims to be current production output.

Known inert items are `attachments/`, Root `children/`, `.search-index-revision` and `token-usage.jsonl`, with exact directory/regular-file types. Their payloads are not read recursively. No business files, index, evidence or private profile fields are written or repaired.

## Bounded reads and ownership

Hard limits are 4,096 yielded Root entries, 16,384 filesystem probes/work visits, 8 MiB each for Main/Runtime, 64 KiB each for Actor/marker/revocation, and 16 MiB shared actual returned bytes. Individual overflow detection may consume one extra byte, charged once to the shared cap. No further read is issued when shared capacity is exhausted; unconfirmed EOF fails. Counters are checked and stat lengths do not replace actual read accounting. These are input/work bounds, not kernel deadlines or parsing allocator limits.

The API acquires existing lifecycle shared → Task shared → exact same-id Session maintenance guards. The existing Arc holder drops Session → Task → lifecycle. One complete started std filesystem job owns its clone through enumeration, reads, errors and actual completion; caller cancellation or whole Tokio shutdown cannot release that clone early. Acquisition and the whole async API are not promised to finish after cancellation. Existing lock-file mechanics are excluded from the no-business-write assertion.

`VacantObserved` is a point observation while those guards are held, not a reservation, create grant or proof that an id was never used. `PresentIdentity` is not current permission, active Project, ancestor authorization, live lease/incarnation, Root mode, effective IR, model/tool policy or profile binding. Cooperative existing writers are the consistency boundary; arbitrary external mutation, retained-FD/no-follow security, remote routes and new persistence/recovery protocols are excluded.

## Verification

Focused fixtures use actual canonical temporary files and two precreated independent Stores. They cover real Child saves versus proofed Root rejection, synthetic post-init legacy pairs, stale/missing/incorrect index, duplicate/incomplete candidates, strict pair/Project/witness checks, actual byte/probe limits and growth, and started-job barriers with separate FileExt handles and later legitimate writers across caller abort and whole-runtime shutdown. Source review does not imply these fixtures have executed. Cargo gates require the coordinated sole window; no production Actor, admission, worker, role application or ContextRefs route is enabled by this API.
