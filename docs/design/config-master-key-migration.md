# Proposal: explicit configuration master keys

- Status: proposed; no implementation or migration is enabled by this document.
- Tracking Issue: [#1558](https://github.com/bigduu/Bamboo-agent/issues/1558), under [#1552](https://github.com/bigduu/Bamboo-agent/issues/1552).
- Evidence date: 2026-10-05.
- Audited source: `dev` at `418e26b01b74bfc34ef4ceb1f2888468edfe1dfc`.
- Scope: Bamboo configuration encryption, its existing credential transaction machinery, and the necessary browser-fingerprint caller seam. No general credential service, provider-token redesign, permissions redesign, or Bodhi/Lotus UI implementation.

## Problem and source evidence

The current key selection is valid `BAMBOO_CONFIG_ENCRYPTION_KEY`, then `.bamboo_encryption_key`, then SHA-256 of a public context and machine identifier, then a random fallback. Both persistence attempts discard their errors. The production `OnceLock<Vec<u8>>` caches one key for the process, rather than a key bound to an explicitly opened data directory. Invalid environment keys are silently ignored. See [selection and cache](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/encryption.rs#L31-L94) and [environment/file decoding and derivation](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/encryption.rs#L120-L186).

Machine identifiers and the public derivation context are not secret entropy. A backup containing both ciphertext and the co-located master-key file also contains its decryption material. Moving future keys into a native credential store improves the separation between configuration backups and keys. It does not protect against a compromised process or OS account with access to that store.

The migration cannot make previously leaked backups safe. A copied legacy key, or a predictable key derivable for that backup, remains usable on the old bytes after local cleanup. Rotate affected provider/API credentials separately if they may have been exposed; this proposal does not claim retrospective protection.

The existing ciphertext is AES-256-GCM encoded as `hex(nonce):hex(ciphertext)` without a key identifier. `CredentialEntry` records `key_version = 1`; its resolver still calls the global decrypt function. See [cipher operations](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/encryption.rs#L387-L442) and [credential records/resolution](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/credential_store.rs#L109-L245).

Reuse the atomic store, revisions, owner-only permissions, and recoverable credential manifest/journal described in [the implemented configuration ADR](config-facade-and-credential-store.md). Existing credential extraction has multiple domain planners sharing a lock and recovery boundary; key rotation must not bypass them. Relevant [transaction source](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/credential_migration.rs#L1-L110).

One external consumer requires its own prerequisite: [persistent browser fingerprints](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-permission/src/tool_permissions.rs#L42-L112) read the same key and independently verify the old file or environment key. Removing the file without adapting this seam would reject durable approval replays. The direct getter in the Copilot auth handler is test-only. Legacy encryption consumers also remain in `config_crypto.rs`, `cluster_fabric.rs`, `section_facade.rs`, the migration planner and server `config_manager.rs`; their format support must be inventoried before activation.

Caller failure handling is a prerequisite, not an adapter detail. [Legacy proxy/provider hydration](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/config_crypto.rs#L196-L234) currently warns and returns without a failure result; a later proxy refresh can remove its encrypted field when plaintext hydration left `None`. [Cluster hydration](https://github.com/bigduu/Bamboo-agent/blob/418e26b01b74bfc34ef4ceb1f2888468edfe1dfc/crates/infra/bamboo-config/src/cluster_fabric.rs#L860-L889) also warns and continues. Credential resolution currently reduces decrypt failure to a validation error, and metadata validation may reduce it to `configured = false`. The new key-source errors must not enter those paths as an absent secret or corrupt JSON.

## Decision: explicit, fallible key sources

Introduce a small data-directory-bound resolver in `bamboo-config`. Encryption, decryption and the persistent-key proof API return errors when key resolution fails. Replace internal uses of the infallible global byte-vector getter; do not preserve it by returning dummy bytes, an ephemeral key, or a panic on an expected storage error. Handle any public API compatibility change explicitly in the caller-seam Issue.

Carry typed `KeyUnavailable`/`KeyLocked`/`KeyMissing` separately from malformed ciphertext and AEAD authentication failure through the loader, facade and mutation boundary. Key availability failure preserves exact encrypted bytes, refs and the trusted snapshot, marks key health unavailable, and rejects replacement/migration requiring decryption. Never reinterpret encrypted text as plaintext, quarantine a structurally valid document solely because its key is locked, or persist blanks/UI masks/defaults after a hydration failure. Only explicit valid replace/clear actions can change a secret. A provider/network runtime must not start with ciphertext substituted for its token. Error-path tests perform a load followed by a save/update and assert byte/ref preservation.

Persist a secret-free source selection. Provisioning (`bamboo init` or an embedding host) selects `environment`, `macos_keychain`, `windows_credential_manager`, or `linux_secret_service`; ordinary startup uses that saved choice. Native backend construction is explicit and target-specific. Do not guess deployment mode from `DISPLAY`, TTY presence, a successful probe, or a library default, and do not silently switch stores.

| Deployment | Provisioning choice | Startup and failure behavior |
| --- | --- | --- |
| macOS desktop / Bodhi sidecar | Explicit login Keychain selection; the host and CLI sharing a data directory use the same identity | A locked/denied/unavailable store blocks secret operations. Unlock/authentication belongs to explicit interactive provisioning or user action; server requests do not introduce dialogs. |
| Windows desktop | Explicit Credential Manager selection for the running OS account, with `Local` persistence | Service-account/profile changes are separate identities. A missing/unavailable entry never causes regeneration once ciphertext exists. |
| Linux desktop | Explicit Secret Service selection and persisted collection policy | Missing D-Bus/service, locked collection, prompt cancellation or ambiguous entries are errors. No fallback to kernel keyutils or a key file. |
| Headless CLI/server/container on any OS | Prefer an operator-provisioned 32-byte environment key; native storage is allowed only when explicitly provisioned for that process account/session | No noninteractive unlock/password prompt. A new directory without an available selected source fails with a category-only provisioning error. |
| Existing legacy installation | Compatibility reader only, until an explicit migration succeeds | Preserve the existing bytes and source; no automatic key generation or rewrite merely because legacy decryption fails. |

Keep deliberate external keys external: a valid environment key on first provisioning selects `environment`, and migration does not automatically copy it into a native store. On subsequent startup the saved source governs. A present malformed environment key fails closed. Supplying a key to a directory already bound to a native source requires an explicit source-change operation; it cannot silently replace the selected native key. Changes to an existing environment key require explicit rotation, not an environment-variable edit followed by ciphertext writes.

Capture only the named configuration variables once during resolver initialization. Never enumerate or log the environment. The resolver cache is bound to `(data_dir_id, key_id, source)` and owned by the runtime/credential-store context; late environment changes cannot mutate a running resolver. Distinct opened data directories cannot accidentally share the current global cache.

A later OS-store lock does not erase a key or credentials already held by an existing process. The availability checks above apply when resolving/verifying the selected source; this proposal does not promise immediate revocation of cached runtime material. Preserve an existing trusted runtime when reconstruction fails, while rejecting secret writes whose required key cannot be verified.

## Library and platform choices

These are candidates verified against official documentation on the evidence date, not dependencies added by this PR. Implementation must recheck exact versions, Rust 1.95 compatibility, target features, locked metadata and platform packaging.

Use fallible OS entropy for 32-byte master keys and 12-byte GCM nonces, for example `getrandom::fill` with the normal platform backend. Propagate entropy failure and never substitute machine identifiers or a test/custom insecure backend. [getrandom 0.4.3](https://docs.rs/getrandom/0.4.3/getrandom/) documents OS sources and error handling.

Use `keyring-core` and only the selected platform store crates. [keyring 4.2.0](https://docs.rs/keyring/4.2.0/keyring/) explicitly directs applications controlling backend selection to that arrangement; the old keyring v3 feature recipe is not the proposed dependency model. Construct entries from the owned selected store; avoid process-global default-store changes for data directories with different policies. [Core lifecycle/API](https://github.com/open-source-cooperative/keyring-rs/wiki/Keyring-Core) supports store-owned entry construction. Its mock/sample stores are tests only; [core thread-safety documentation](https://docs.rs/keyring-core/1.0.0/keyring_core/) does not promise reliable concurrent operations on a single native entry.

| Target | Candidate | Explicit policy and required verification |
| --- | --- | --- |
| macOS | `apple-native-keyring-store` 1.0.2, `keychain` feature | Use the User/login domain for the current CLI and sidecar. Do not enable `protected` as an automatic substitute: its sandbox/provisioning requirements differ. Validate signed packaged sidecar and CLI access together, as well as locked/denied behavior. [Store documentation](https://docs.rs/apple-native-keyring-store/1.0.2/apple_native_keyring_store/), [User-domain source](https://github.com/open-source-cooperative/apple-native-keyring-store/blob/78cdfff31e8a6579119b75ff7cbfeae7d4fc7d0a/src/keychain.rs). |
| Windows | `windows-native-keyring-store` 1.1.0 | Generic credential with an explicit collision-free target and `Local` persistence, overriding its Enterprise default. Serialize each entry's native operations; verify process/logon restart and the selected account. [Store documentation](https://docs.rs/windows-native-keyring-store/1.1.0/windows_native_keyring_store/), [Windows persistence semantics](https://learn.microsoft.com/en-us/windows/win32/api/wincred/ns-wincred-credentialw). |
| Linux | `zbus-secret-service-keyring-store` 1.0.1, one selected `rt-tokio-crypto-rust` runtime/crypto feature | Match Bamboo's Tokio usage and avoid a libdbus/OpenSSL-specific backend by default. Pin collection choice and reject ambiguous matches; no first-match selection. Store keys as a versioned UTF-8/base64 record so KDE's UTF-8 constraint is respected. Missing default collection under WSL requires explicit provisioning, not an invented fallback. [Store documentation](https://docs.rs/zbus-secret-service-keyring-store/1.0.1/zbus_secret_service_keyring_store/), [feature/MSRV source](https://github.com/open-source-cooperative/zbus-secret-service-keyring-store/blob/a97612aa64dd0148ad2504b3a9d4d82ca94e070b/Cargo.toml). |

Do not select `linux-keyutils-keyring-store` for durable master keys: its [documented persistence](https://docs.rs/linux-keyutils-keyring-store/1.0.0/linux_keyutils_keyring_store/) is in memory and cleared on reboot. A usable headless deployment needs its explicitly provisioned durable source; testing with an unlocked disposable Secret Service does not certify an arbitrary user's headless host.

Selecting a store crate does not itself enforce a no-dialog policy. Each native adapter must prove that its noninteractive path rejects operations requiring OS authorization/unlock UI rather than invoking it; interactive provisioning is a separate explicit call. If the selected API cannot enforce that policy, keep its noninteractive activation gated and return `key_source_unavailable` instead of assuming a library default is safe. Native acceptance covers denied ACL/access, locked stores and prompt cancellation as well as the already-unlocked case.

## Stable identity and bootstrap

The application namespace is the constant `io.github.bigduu.bamboo.config-master-key.v1`. A random `data_dir_id` is created once and stored in owner-only, secret-free `config-master-key.json`; it is not derived from host ID, executable path, package version or username. A canonical directory path is used for local locking and path safety, not as the keychain identity. Moving a data directory preserves its ID; a fresh directory obtains a new one. A copied backup retains its ID for restoration. Independent clone/rebind is outside the first migration: do not silently regenerate IDs over existing ciphertext or claim a copied installation has a new key identity.

Each immutable generation has a random `key_id`. An entry uses the fixed application service and `<data_dir_id>/<key_id>` account; Windows additionally sets a deterministic, unambiguous target from those fields. IDs use one canonical lowercase encoding. Linux collection choice is part of the saved source descriptor. Never overwrite an old generation during rotation. A native secret is a strictly decoded UTF-8 record containing record version, IDs and exactly 32 decoded key bytes; matching IDs are verified after retrieval.

The local descriptor contains schema version, directory ID, selected source, pending/active key references and a small encrypted key-confirmation record. It never contains raw keys, environment values, key-derived public fingerprints, plaintext credentials, or arbitrary native error strings. Confirmations bind their format, directory ID and key ID as GCM associated data. They detect a replaced environment key even in an otherwise empty directory. They are ciphertext, not a key backup.

Use the existing data-directory migration lock and its documented lock ordering; a native store and the filesystem are **not** one transaction. Bootstrap is a separate implementation slice with these boundaries:

1. Acquire the process operation guard and inter-process migration/advisory lock for the canonical data directory. Reject unsafe directory aliases and unsupported locking; do not lock a symlink-selected untrusted file. Recheck existing metadata under the lock.
2. For genuinely new data, reserve the random directory/key IDs and selected source in an atomically written, synced pending descriptor **before** a native write. Legacy data remains legacy until the separate preflight/migration succeeds.
3. Read the exact native reference. `NoEntry` permits generation only for an unpublished pending bootstrap with no ciphertext using that generation. Generate through OS entropy, write once, then read and validate the exact result before any encryption is enabled. An existing valid record is resumed rather than overwritten. Environment selection validates the supplied key without storing its bytes locally.
4. Publish ready metadata and confirmation only after verification. Failure leaves no active new generation. After an uncertain native write, reread the same reference; do not create another key on retry. Ambiguous/malformed/denied results stop the operation.
5. If active ciphertext or a committed generation exists, `NoEntry` means recovery is needed, never permission to regenerate a replacement key. A readback proves immediate accessibility, not an OS power-loss transaction guarantee; restart acceptance and recovery material cover that limitation.

All cooperating Bamboo processes use this lock. Native libraries do not provide a portable create-if-absent/CAS guarantee against unrelated writers; this design makes no such claim. Entry operations are serialized on an owned blocking worker where required. Cancellation cannot release the file lock while an uncancellable native call continues writing; the operation retains ownership until it settles and its durable state is recorded. Reader/startup code recovers pending state before exposing credential snapshots.

## Versioned reads, preflight and migration

Retain a bounded legacy reader for the exact v1 algorithm and sources. It does not call the current generator or best-effort writer. Capture one valid environment/file/derived legacy candidate using the historical precedence, with no silent skipping of a present malformed selected source. A moved host with a missing old key may be unrecoverable; report that category and preserve every file. Do not try arbitrary key lists or write a new key to make startup succeed.

The new string format is `v2:<key_id>:<nonce_hex>:<ciphertext_hex>`. Keep AES-256-GCM, adding authenticated format/directory/key IDs and a fresh OS-random nonce. The resolver accepts exact known versions and resolves the indicated immutable generation; v2 failure never falls through to v1. `CredentialEntry.key_version = 2` and its key selector must agree with the ciphertext. Unknown versions/IDs, swapped selectors, malformed nonces and authentication failure fail closed. All legacy string consumers must use this reader before any v2 writer is enabled.

Preflight runs under the shared recovery boundary and proves the complete live legacy inventory can be read/decrypted before constructing candidates. Use the existing extraction planners and completion/source attestations for provider, MCP, proxy, environment, notification, connect, access-control, cluster and broker credentials, plus the existing Copilot credential path. Do not write raw keys/plaintext to a staging file. If any relevant source or pending transaction cannot be classified/decrypted, perform no rekey commit. Old backups and quarantine bytes remain recovery inventory, not ordinary live input; keep their required generations until explicit retirement.

Rekey the extracted `credentials.json` through the existing staged manifest/journal protocol. The explicit master-key scope includes its descriptor and credential envelope, preserving refs, source metadata, monotonic revision/CAS and public section revision authority. Do not rewrite each section sequentially or build a second generic transaction coordinator.

The sequence is: recover existing transactions; quiesce credential writers; obtain and verify the new native/environment generation; decrypt all current records; stage newly encrypted candidates and rollback material; verify every candidate round-trip and inventory/ref count; sync stage files; durably commit the filesystem manifest; recover/replace its members idempotently; publish the new resolver/snapshot. New key availability is a precondition outside that filesystem transaction. A per-record key selector allows recovery readers to identify replaced bytes; it does not authorize mixed or unverified snapshots.

| Interruption/failure point | Required outcome |
| --- | --- |
| Legacy preflight or target key acquisition fails | Original metadata/ciphertext unchanged; no activation or legacy deletion. |
| Native write may have happened; no filesystem commit | Reopen the exact pending key reference. Preserve legacy state; do not overwrite or generate another key. |
| Candidates exist; commit manifest absent | Legacy remains authoritative. Discard/resume only owned transaction staging after source/CAS recheck. |
| Durable commit manifest exists; some members replaced | Startup finishes the existing manifest recovery before reads. Cancellation returns an operation status, not a false rollback result. |
| Target key unavailable after commit | Preserve both committed ciphertext and rollback material; fail secret operations until the named key is restored or explicit compatible rollback is performed. Never regenerate. |
| Verification/source/CAS conflict | Stop before commit and keep the exact original/source inventory. No best-effort partially migrated success. |

Deterministic tests must crash/cancel at every boundary, race first initialization and credential writers, and verify secrets/refs and authority after restart. File-store durability follows its implemented platform contract; native credential-store durability follows that backend. The design does not offer distributed atomicity, protection from external deletion of every recovery key, or recovery of an already lost legacy key.

## Recovery, export/import and rollback

Default exports and configuration diagnostics exclude master keys. An explicit local recovery export seals the required active/retained key generations and directory identity with AES-GCM under an independently provisioned 32-byte recovery wrapping key. Use a versioned, bounded archive with authenticated IDs/format and a fresh nonce; a human password is not a 256-bit wrapping key. Passphrase KDFs and secret-manager integrations are separate work if later required. Recovery input is supplied through a private non-logging input channel, never an argv value or chat/API response. The wrapping key is held separately from both the data directory and archive; exporting it beside the archive defeats the separation.

Import validates/decrypts the entire archive and key IDs before any write, detects existing conflicting generations, and installs through the same explicit source/bootstrap policy. It does not import provider secrets into unrelated directories, overwrite an existing native key, or fall back to a local plaintext key file. Restoration of the original installation, including a moved/restored data directory, preserves IDs and verifies the named records. Independent clone/rebind would require another focused design/Issue for new IDs and full re-encryption; it is rejected by the first import implementation. Tests use synthetic secrets and disposable stores.

Migration retains rollback material and old key sources until verification, restart/recovery acceptance and the retention decision are complete. Do not automatically delete the old file, native generation, backups or quarantine on first startup success. A legacy-key file retained for rollback keeps the old backup exposure alive; label that state truthfully.

Rollback means a compatible, stopped-writer operation that can decrypt **current** data and restore the selected legacy representation; replaying an old snapshot would discard newer credentials. Old binaries do not understand v2 or the new key source, so downgrade is unsupported without explicit reverse migration and compatible metadata. If the new key is missing, restoring an old snapshot recovers only its recorded revision and must require a stated data-loss decision. No automatic downgrade writes.

Retirement is an explicit final action: verify recovery export/import (or the external-source recovery procedure), verify no retained inventory needs the old generation, expire/invalidate old browser receipts, then remove only the listed owned artifacts/keys. Deletion is not a claim of secure erasure of APFS snapshots, external backups or previously leaked keys. No cloud sync or OS-store-wide enumeration/cleanup is part of this design.

## Browser receipt seam and redaction

The focused prerequisite preserves the current fail-closed persistent-key proof while replacing its direct old-file check with a data-directory-bound, verified resolver proof. Do not turn the encryption master key into a new general signing authority. Existing HMAC purpose separation and canonical path binding remain intact; any additional independent browser key lifecycle needs its own design.

Key rotation explicitly invalidates receipts associated with the previous fingerprint generation and requires fresh approval. Never accept a legacy receipt under a new key, broaden its resource, or treat a mismatch as permission. Migration cannot activate until the receipt-generation/invalidation behavior is tested with daemon restart, pending approval replay and missing/locked key sources.

Errors exposed to logs, events, status/doctor and APIs are closed categories such as `key_source_unavailable`, `key_source_locked`, `invalid_key`, `key_not_found`, `ambiguous_key`, `legacy_decryption_failed` and `migration_recovery_required`. Do not serialize native exception bodies, plaintext, raw keys, ciphertext, archive contents, environment values or native entry wrappers. Public diagnostics expose only source category, lifecycle state and operation ID; local metadata IDs stay internal. Use redacted secret containers and bounded lifetimes, wiping owned key/plaintext buffers where feasible; make no claim that third-party/OS allocations are completely wiped.

## Implementation slices and acceptance

Each child uses a fresh isolated branch/worktree, one PR to `dev`, and a 4–8 hour timebox. Dependencies are explicit; platform adapters can proceed in parallel after the shared resolver contract is reviewed. Shared manifests/lockfiles and resolver/transaction files are coordinated before integration. Children remain Triaged until their prerequisites and this design are accepted; no label bypasses Roadmap Ready.

The table below links the concrete child Issues. All code children require affected suites, full locked all-features metadata for dependency changes, exact CI lint/format commands and required PR checks. Production activation requires all of the following: synthetic fault/concurrency tests; native macOS packaged sidecar/CLI and locked/denied tests; Windows restart/profile/persistence tests; Linux Secret Service/locked/no-bus/KDE/headless environment tests; receipt replay rejection after rotation; and verified export/import/rollback with unchanged refs and no secret diagnostics. Compilation or a mock-store pass alone does not satisfy native acceptance.

| Issue | Acceptance slice | Prerequisites (all also depend on #1558) | Estimate |
| --- | --- | --- | --- |
| [#1569](https://github.com/bigduu/Bamboo-agent/issues/1569) | refactor: add a fallible data-directory master-key resolver | #1558 | 4–6 hours |
| [#1570](https://github.com/bigduu/Bamboo-agent/issues/1570) | fix: preserve legacy provider and proxy ciphertext on key-source failure | #1569 | 6–8 hours |
| [#1571](https://github.com/bigduu/Bamboo-agent/issues/1571) | fix: distinguish unavailable keys from invalid credential data | #1569 | 6–8 hours |
| [#1572](https://github.com/bigduu/Bamboo-agent/issues/1572) | feat: add a versioned ciphertext reader without activating new writes | #1569 | 6–8 hours |
| [#1573](https://github.com/bigduu/Bamboo-agent/issues/1573) | feat: add an explicit macOS login Keychain master-key adapter | #1569 | 4–6 hours |
| [#1574](https://github.com/bigduu/Bamboo-agent/issues/1574) | feat: add an explicit Windows Credential Manager master-key adapter | #1569 | 4–6 hours |
| [#1575](https://github.com/bigduu/Bamboo-agent/issues/1575) | feat: add an explicit Linux Secret Service master-key adapter | #1569 | 6–8 hours |
| [#1576](https://github.com/bigduu/Bamboo-agent/issues/1576) | feat: serialize immutable master-key identity and bootstrap | #1569, #1573, #1574, #1575 | 6–8 hours |
| [#1577](https://github.com/bigduu/Bamboo-agent/issues/1577) | feat: inventory and verify legacy secrets before master-key rotation | #1570, #1571, #1572, #1576 | 4–6 hours |
| [#1578](https://github.com/bigduu/Bamboo-agent/issues/1578) | fix: preserve browser fingerprint key proofs across master-key migration | #1569, #1576 | 4–6 hours |
| [#1579](https://github.com/bigduu/Bamboo-agent/issues/1579) | feat: export a bounded sealed master-key recovery archive | #1576, #1572 | 4–6 hours |
| [#1580](https://github.com/bigduu/Bamboo-agent/issues/1580) | feat: restore master keys from a verified recovery archive | #1579, #1576, #1572 | 6–8 hours |
| [#1581](https://github.com/bigduu/Bamboo-agent/issues/1581) | feat: rekey extracted credentials through the existing migration journal | #1577, #1578, #1579, #1580 | 6–8 hours |
| [#1582](https://github.com/bigduu/Bamboo-agent/issues/1582) | feat: reverse master-key migration without discarding newer credentials | #1581, #1580 | 4–6 hours |
| [#1583](https://github.com/bigduu/Bamboo-agent/issues/1583) | test: gate native master-key activation and explicit legacy retirement | #1581, #1582, #1578, #1579, #1580, #1573, #1574, #1575 | 6–8 hours |

The resolver/error/reader children establish the contract before native persistence changes. The three adapters then run in parallel; bootstrap, preflight, recovery and journal integration follow their explicit dependencies. The final activation/retirement slice is the only one allowed to enable production v2 writes and retire legacy material. If any slice needs another independent persistence/recovery protocol or cannot fit eight hours, split it again before implementation.
