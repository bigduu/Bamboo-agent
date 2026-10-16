# Development-only DuckDB sys ZIP dependency patch

Based on the exact published MIT-licensed libduckdb-sys1.10506.0 archive.
BAMBOO-PATCH-PROVENANCE.json records its SHA256, upstream VCS receipt and every
original member digest. Preserve upstream LICENSE and source attribution.

The only functional source change is Cargo.toml: ZIP6 becomes ZIP9.0.3 and
publish=false prevents accidental republishing under the upstream identity.
Package name/version, DuckDB1.5.6 engine archive, bindings and all Rust/C++
implementation files remain byte-identical. The unused dependency Cargo.lock
is omitted and its digest recorded; the active workspace lock must resolve and
pass the original cargo audit --deny warnings without ignores.

The root crates.io patch is for the dev-only bamboo-analytics graph, outside
the29-crate bamboo-agent publication closure. Verify clean package and registry
consumer behavior; never claim a source-local patch fixes a downstream registry
edge. If any published crate requires this path, the plan must fail closed.

Static preparation only: lock resolution, compilation, Analytics/ZIP regressions,
platform checks and publication acceptance have not run. This is not an accepted
release source or permission to publish.
