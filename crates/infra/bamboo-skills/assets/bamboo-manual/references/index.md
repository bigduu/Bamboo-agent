# Chapter index

All paths are relative to this `bamboo-manual` package. The manifest inventories
every embedded resource and its exact SHA-256. Reading uses the existing Skill
reader and its current permission checks; no network or model call is needed
to retrieve the bytes. Answer generation can still use a configured model.

| Question / search terms | Resource |
| --- | --- |
| Which binary version? Source SHA? Package hash? Offline? | `manifest.json` |
| Configuration key, default, JSON, partition, precedence, environment, CLI | `references/configuration.md` |
| Exact keys, aliases, defaults, typed groups, root coverage and unknown fields | `references/config-keys.json` |
| Memory, Jiandu, Session/Project/Global, lexical, rerank, Dream, 记忆 | `references/memory.md` |
| Hook, lifecycle_hooks, command, script, timeout, portable plugin, 钩子 | `references/hooks.md` |
| Provider, model, MCP, desired/applied revision, live-health | `references/providers-mcp.md` |
| Missing recall, setting did not apply, unavailable skill, diagnosis, 排障 | `references/troubleshooting.md` |

This small chapter index is curated lookup, not a new full-text search engine.
Tool metadata search is not document-content search. Use the key catalog and
selected chapter; do not infer an exhaustive negative from this index.

## Maintenance and identity

Canonical sources live in `crates/infra/bamboo-skills/assets/bamboo-manual` so
Cargo source packages retain them. The build fails for any missing, empty,
non-UTF-8 or malformed JSON declared resource. `build/product_manual.rs` is the
explicit resource allowlist. There is no download or fallback to moving latest.

`content_sha256` hashes the declared resources, in allowlist order, with a
domain prefix and length-framed paths and bytes; the generated manifest is
excluded to avoid self-reference. Individual resource hashes allow checking
the exact text. `package_version` is the compiled bamboo-skills package version.
The normal workspace shares that version with Bamboo; custom mixed-version
embedders must compare their own application version rather than infer it.

Source archives use Cargo's recorded VCS identity when available. Otherwise
source-checkout builds report the baseline Git revision with status
`checkout_unverified`. Cached build metadata cannot attest checkout cleanliness.
Unproven identities remain unknown; a baseline is not an exact release snapshot.
The dependency manifest hash identifies declared inputs; a lock hash identifies
the lockfile available at build time. A null lock hash means exact dependency
resolution is not attested. These are provenance labels, not signatures.

Published websites may consume these same sources in the future. This bundle
does not assert a live documentation website or a publication pipeline.
