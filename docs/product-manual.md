# Version-bound offline product manual

Bamboo embeds a `bamboo-manual` Skill in the binary through `bamboo-skills`.
Discover it with the existing Skill catalog, then read its manifest and chapter
index through the existing paginated Skill reader. Existing skill disablement,
scope and access rules still apply. It needs neither a source checkout nor a
network request to retrieve the manual; using a model to answer is separate.

The canonical, package-safe sources are
[`crates/infra/bamboo-skills/assets/bamboo-manual`](../crates/infra/bamboo-skills/assets/bamboo-manual).
Start with [the chapter index](../crates/infra/bamboo-skills/assets/bamboo-manual/references/index.md).
These are maintained product documentation, not historical proposals. The
manifest records exact content, package and available source/dependency
identities. Missing declared content fails the build instead of silently
shipping an empty manual or fetching latest.

This first slice provides a bounded configuration-key catalog and explanations
of configuration, memory, hooks, providers/MCP and safe troubleshooting. It does
not implement a full configuration schema or website publication. The server
now also offers the bounded read-only observation tool described below. No configuration/memory mutation, hook execution or model
call is part of manual retrieval.

## Maintaining the typed key reference

The public JSON is generated from `crates/infra/bamboo-skills/manual/config_catalog.rs`.
Each curated declaration includes a real typed field accessor, default, example,
alias list and explanation. Selected DTOs are exhaustively destructured, so new
fields (including skipped optional fields) require deliberate coverage updates.
Root ownership is generated from `CONFIG_VALUE_FIELD_OWNERS`; unknown roots are
visible as undocumented rather than silently considered invalid.

Run `cargo run -p bamboo-skills --example generate_manual_catalog -- --write`,
review the asset diff, then run the same command with `--check` and
`cargo test -p bamboo-skills --test product_manual --locked`. The normal tests
compare the embedded artifact byte-for-byte with deterministic generation and
exercise typed defaults, non-default examples, aliases and selected validators.
The generator reads no live configuration or environment. Its no-argument mode
prints JSON; `--check` is read-only and only explicit `--write` changes the asset.
This improves bounded coverage; it does not create a complete JSON Schema or
website.


## Current-session instance observations

The server registers `bamboo_diagnostics` on its existing permission-aware base
surface. It accepts `{}` only and binds to the tool context's current session.
It projects existing in-process provider/MCP health, recorded application
revisions/error codes and cached prompt-memory counts/status. It does not read
configuration/credential files, environment values, transcripts or memory bodies;
it does not probe a provider, reload, repair, load persisted sessions or mutate
state. No new persistence or background authority is introduced.

Read [safe troubleshooting](../crates/infra/bamboo-skills/assets/bamboo-manual/references/troubleshooting.md)
for the evidence limits. Receipt existence is not desired/applied equivalence;
cached memory counts are not proof of model use and have unknown capture time.
Missing/invalid observations remain unknown. The manual is offline product
knowledge; tool output is separately scoped instance evidence. Ordinary tool
results may be sent to the configured model, so only explicit fixed labels,
counts, revisions and timestamps are projected.

Run focused `bamboo-server` diagnostics/receipt tests and the `bamboo-skills`
product-manual integration tests when changing this adapter. Canary tests cover
private source paths/errors, project identifiers, arbitrary status strings and
other-session observations, and compare sources before/after collection.
