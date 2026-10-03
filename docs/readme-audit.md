# README source and release audit

Checked on 2026-10-03 for the Zenith documentation refresh. This is documentation
provenance, not a release announcement or runtime certification.

## Version boundaries

| Surface | Observed identity | Meaning |
|---|---|---|
| Zenith Bamboo pin / README baseline | `025641317c5703226052a4b94a52d1844615c352` | Source checked for the rewritten README |
| Fetched `origin/dev` | `025641317c5703226052a4b94a52d1844615c352` | Same as the pin at audit time |
| Fetched `origin/main` | `28b4f8473b308b26965c3afe3de92821f160a0fd` | Pin has 361 reachable commits not in main |
| Latest non-yanked crates.io index entry | `2026.9.20` | Registry publication; distinct from development source |
| Published crate's `.cargo_vcs_info.json` | `284cfd06ddc34b1ef7f9a6839b0692cc872a8123`, `dirty: true` | Provenance supplied by the package; not proof of byte-identical Git source |
| Latest GitHub release | `v2026.3.3`, tag commit `830a1f8e29c1a108b97c62662d3807a60c7f2682` | Older than the registry release; GitHub releases alone do not establish current crate availability |
| Source frontend lock | `@bigduu/lotus-next@2026.9.22`, source `a480e2bb94f5dd08fe4b01b2f8844a2c9ed03245` | Exact embedded frontend artifact, independently versioned |

The pin has 504 reachable commits not in the package's recorded source commit.
This counts Git history, not individual released features. In particular, the
pin includes later actor continuity and local-child-history work; that source
must not be promoted as functionality already shipped in crate `2026.9.20`.
The separately planned Zenith train version `2026.10.3` does not prove publication
and its configured source revision differs from this pin.

Primary publication evidence:

- [Official Cargo sparse index](https://index.crates.io/ba/mb/bamboo-agent): last
  five entries were `2026.8.27`, `2026.8.28`, `2026.9.15`, `2026.9.19`, `2026.9.20`,
  all non-yanked.
- [Official 2026.9.20 crate archive](https://static.crates.io/crates/bamboo-agent/bamboo-agent-2026.9.20.crate):
  inspected `.cargo_vcs_info.json` and `Cargo.toml.orig`; package manifest declares
  Rust `1.95`. The archive does not contain `scripts/frontend-package-lock.json`.
- [GitHub release](https://github.com/bigduu/Bamboo-agent/releases/tag/v2026.3.3).
  The `/releases/latest` redirect was checked by the coordinating audit.

The crates.io JSON API and GitHub REST API returned HTTP 403 in this environment;
registry index/archive access worked. No credentials or authenticated API access
were used.

## Source checks

| README statement | Checked source |
|---|---|
| CLI flags, echo limitations, permissions/respond flow | [`src/bin/bamboo.rs`](../src/bin/bamboo.rs), [`src/headless.rs`](../src/headless.rs), [`src/admin_cli.rs`](../src/admin_cli.rs) |
| Workspace tiers, Rust minimum, default-member exclusions | [`Cargo.toml`](../Cargo.toml) |
| Embedded Lotus Next identity and explicit staging | [`scripts/frontend-package-lock.json`](../scripts/frontend-package-lock.json), [`scripts/frontend-package.cjs`](../scripts/frontend-package.cjs) |
| Tools are real registrations, not a scale/quality guarantee | [`executor.rs`](../crates/engine/bamboo-tools/src/executor.rs) |
| Provider setup, HTTP/SDK usage | [`GETTING_STARTED.md`](./guides/GETTING_STARTED.md), [`API.md`](./guides/API.md), [`src/setup_cli.rs`](../src/setup_cli.rs) |
| Container restrictions and local-network authentication boundary | [`docker-compose.yml`](../docker/docker-compose.yml) |
| Jiandu canonical memory ownership | [`AGENTS.md`](../AGENTS.md), [`bamboo-memory`](../crates/infra/bamboo-memory) |

## Changes and validation

- Replaced feature-count and infallible-memory promotion with concrete tasks,
  installation, observable runtime entry points, and operating boundaries.
- Corrected Chinese README's stale claim that Lotus Next was experimental and
  not the default. Both languages now explain the same source/package distinction.
- Kept API, SDK, deployment, config, architecture, migration, security, and license
  links. Detailed implementation material is reached through existing guides.
- Removed the old static hero from the introduction; it is not a real recording.
- `node scripts/frontend-package.cjs stage` succeeded and reported the locked
  `lotus-next@2026.9.22` package with source `a480e2bb94f5dd08fe4b01b2f8844a2c9ed03245`.
- Checked README relative links and `git diff --check`. No product code changed,
  so no unrelated Rust regression suite was run by this documentation worker.
- No live provider turn, external MCP session, or native macOS/Windows desktop
  workflow was validated here. `--echo` is explicitly labeled a transport smoke.
  Recording assets, if added by the coordinating audit, carry their own provenance.

No branch for Supervisor #1481 or the #791 owner was changed; no remote write,
submodule pin update, release, deployment, or PR creation was performed.
