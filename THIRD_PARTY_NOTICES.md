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
