# Third-party source notices

## OpenAI Codex tool search

`crates/engine/bamboo-tools/src/tool_search.rs` adapts metadata text construction
and BM25 search setup from OpenAI Codex, copyright 2025 OpenAI, at revision
`7f892275e31002f0422477c6219189284560e689`:

- `codex-rs/tools/src/tool_search.rs`
- `codex-rs/core/src/tools/handlers/tool_search.rs`

These portions retain the [Apache-2.0 license](crates/engine/bamboo-tools/third_party/codex/LICENSE) and
[upstream notices](crates/engine/bamboo-tools/third_party/codex/NOTICE). Bamboo changes adapt JSON Schema
values, current host eligibility, exact-first aliases, deterministic bounds,
and complete schema projection. The remaining project-owned code retains its
existing license.

## OpenAI Codex Skill input

`crates/infra/bamboo-skills/src/store/codex_frontmatter.rs` and its six fixtures
adapt the metadata parser from `codex-rs/skills/src/parser.rs` and
`codex-rs/skills/src/parser_tests.rs` at the same pinned Codex revision above.
`crates/infra/bamboo-skills/src/catalog/codex_metadata.rs` adapts short-description
normalization and optional policy projection from
`codex-rs/ext/skills/src/loader/metadata.rs` and `codex-rs/skills/src/interface.rs`.

These portions are copyright 2025 OpenAI and retain the
[Apache-2.0 license](crates/infra/bamboo-skills/third_party/codex/LICENSE) and
[upstream notices](crates/infra/bamboo-skills/third_party/codex/NOTICE).
Bamboo changes share frontmatter extraction/repair with supported host extras,
retain safe IDs and body bytes, and intersect host invocation denies. Optional
OpenAI descriptive errors do not discard a valid implicit invocation deny.


## OpenAI Codex Skill metadata list

`crates/app/bamboo-server-tools/src/skill_runtime/catalog.rs` adapts
`codex-rs/ext/skills/src/tools/list.rs` at the pinned revision above.
These portions are copyright 2025 OpenAI and retain the complete package-local
[Apache license](crates/app/bamboo-server-tools/third_party/codex/LICENSE) and
[upstream notice](crates/app/bamboo-server-tools/third_party/codex/NOTICE).
Bamboo uses source-validated owned metadata, trusted fresh caller/input intent,
and serialized provider envelopes. Indivisible entries return a budget error.
The package retains Bamboo's original [MIT text](crates/app/bamboo-server-tools/LICENSE).


## OpenAI Codex Skill metadata rendering

`crates/infra/bamboo-skills/src/progressive/render.rs` and `render_tests.rs` adapt
`codex-rs/ext/skills/src/{render.rs,aliases.rs,render_tests.rs}` at the same pinned
revision. These portions are copyright 2025 OpenAI and retain the complete
[Apache license](crates/infra/bamboo-skills/third_party/codex/LICENSE) and
[upstream notice](crates/infra/bamboo-skills/third_party/codex/NOTICE).
Bamboo adapts host locators/source ordering and fresh metadata eligibility;
ordering and unavailable-alias regression fixtures are Bamboo-authored.

### Selected Skill reading and usage guidance (#1623)

The UTF-8 advancing pagination algorithm and handle bounds in
`crates/infra/bamboo-skills/src/progressive/read.rs` and
`crates/app/bamboo-server-tools/src/skill_runtime/catalog.rs` adapt OpenAI Codex
`codex-rs/ext/skills/src/tools/{read.rs,mod.rs}` at revision
`7f892275e31002f0422477c6219189284560e689` (Apache-2.0).
`progressive/render.rs` adapts EOF/reference guidance from `catalog_prompt.rs`.
The upstream executor cache and content-only cursor authority are not imported.
Bamboo supplies the finite charged cache, current source/caller validation and
real provider-envelope accounting. Boundary and behavior fixtures are Bamboo-authored;
the pinned sources contain no dedicated read fixtures to copy.
Complete Bamboo MIT and Codex Apache-2.0/NOTICE copies remain packaged in both
bamboo-skills and bamboo-server-tools. Existing provenance and notices above apply.
