# Codex-derived Deferred tool search

This is the Tools search slice of Bamboo's Codex disclosure migration (#1534).
It uses the upstream metadata builder and `bm25` 2.3.2 setup at Codex revision
`7f892275e31002f0422477c6219189284560e689` with Apache-2.0 attribution in
`THIRD_PARTY_NOTICES.md`.

The pinned dependency retains `fxhash` 0.2.1, covered by the
[INFO Unmaintained advisory](https://rustsec.org/advisories/RUSTSEC-2025-0057.html).
No published bm25 release drops fxhash; its hashes define embedding coordinates.
[Pinned Codex also records this exception](https://github.com/openai/codex/blob/7f892275e31002f0422477c6219189284560e689/codex-rs/deny.toml#L76).
Bamboo records the reviewed #1606 decision in both `deny.toml` and
`.cargo/audit.toml`. It excepts this single ID globally; the stated dependency
path is review evidence, not a machine-enforced restriction. Recheck each lock
update, new fxhash path, upstream release/advisory change and before #1537; remove
both entries when an accepted upstream release drops fxhash. Search and locked
dependencies stay unchanged. This preserves existing Bamboo tokenization;
its Unicode segmentation dependency already differs from the pinned Codex lock,
so this is not a claim of exhaustive Unicode parity.

The current session's host-resolved registry is the authority. Search indexes
only eligible Deferred tool names, underscore-expanded names, descriptions,
and recursively nested parameter property names/descriptions (`properties`,
`items`, `anyOf`). Defaults, enum values, examples and extension values do not
enter search text. The index retains metadata and exact execution identities;
selected results receive complete schemas from that same catalog.

Exact registration identity and existing unshadowed aliases come first, followed
by upstream BM25 relevance. Equal scores use execution-name order before the
five-result bound. Queries are limited to 256 Unicode characters; empty queries
and limits outside 1–5 fail. Rebuilding from the current registry prevents
removed, disabled and HostOnly tools from being returned or gaining callable
authority. Historical loaded names still require current registry eligibility.

OpenAI Responses client `tool_search` exposes `query` and optional `limit` and
searches Tools only. New requests with explicit Skill/Workflow kinds fail;
completed historical search transcripts continue to replay. The StickyFallback
gateway also defaults to Tools; explicit Skill/Workflow kinds retain the narrow
legacy command adapter until the subsequent Skill migration. Mixed requests
keep Tools in BM25 order first, then legacy command matches in their own order,
within one shared result bound. The two rank scores are never compared.
Legacy command lookup retains its own five-match bound even when Tools fill
all schema slots. When both searches return the same legacy gateway, the
explicit command match tightens that definition to the advertised IDs/revisions
at its existing Tools position, without using another slot. Additional command
gateways are appended only while schema capacity remains. Tools-only requests
retain complete original schemas.

Anthropic native server search and OpenAI hosted server search retain their
provider-owned ranking and wire protocol. They receive the eligible Deferred
tool catalog using the existing native protocol. This host BM25 implementation
applies to OpenAI client search and the fallback gateway. Unsupported surfaces
retain the existing full catalog behavior.

An adjacent admission gap remains outside this search slice: a changed schema
retaining the same execution name can inherit previous loaded membership.
Same-name definition invalidation requires a separate replay/admission change;
this slice does not create another loaded-state authority.
