# Known Security Issues

This document tracks security advisories that cannot be immediately resolved
due to upstream dependency constraints.

## Audit Status

Last audited: 2026-04-24
Command: `cargo audit`

---

## Fixed

### RUSTSEC-2026-0104 — rustls-webpki (HIGH)
- **Issue**: Reachable panic in certificate revocation list parsing
- **Original version**: 0.103.12
- **Fixed version**: 0.103.13
- **Status**: Resolved via `cargo update -p rustls-webpki`
- **Commit**: See git history for rustls-webpki upgrade

---

## Pending Upstream Fixes

The following warnings are **informational only** (not exploitable vulnerabilities).
They require upstream crate updates and cannot be resolved without breaking API changes.

### RUSTSEC-2024-0384 — instant (unmaintained)
- **Version**: 0.1.13
- **Dependency chain**: `parking_lot 0.11.2` -> `wasm-timer 0.2.5` -> `reqwest-retry 0.7.0`
- **Blocked by**: `reqwest-retry` upgrade requires `reqwest 0.13` + `reqwest-middleware 0.5`
- **Impact**: Low. `instant` is a WASM compatibility shim; unused in production (desktop/server targets only).
- **Tracking**: Upgrade `reqwest-retry` to 0.9+ when `reqwest-middleware` 0.5 migration is feasible.

### RUSTSEC-2024-0436 — paste (unmaintained)
- **Version**: 1.0.15
- **Dependency chain**: `ratatui 0.29.0` -> `bamboo-tui`
- **Blocked by**: `ratatui` 0.30 has breaking API changes (`Widget` trait moved to `ratatui-core`)
- **Impact**: Low. `paste` is a compile-time macro crate; no runtime exposure.
- **Tracking**: Upgrade `ratatui` to 0.30+ and fix `bamboo-tui` widget imports.

### RUSTSEC-2025-0134 — rustls-pemfile (unmaintained)
- **Version**: 2.2.0
- **Dependency chain**: `rustls-native-certs 0.7.3` -> `hyper-http-proxy 1.1.0` -> `launchdarkly-sdk-transport 0.1.1`
- **Blocked by**: `rustls-native-certs` 0.8+ may have API changes
- **Impact**: Low. Only used for GrowthBook/launchdarkly SDK transport.
- **Tracking**: Monitor `hyper-http-proxy` and `launchdarkly-sdk-transport` for updates.

### RUSTSEC-2026-0002 — lru (unsound)
- **Version**: 0.12.5
- **Dependency chain**: `ratatui 0.29.0` -> `bamboo-tui`
- **Blocked by**: Same as `paste` — requires `ratatui` 0.30+
- **Impact**: Low. `IterMut` violation only affects unsafe code paths; ratatui usage is safe.
- **Tracking**: Upgrade `ratatui` to 0.30+ (bundles `lru` 0.16+ which fixes this).

---

## Resolution Plan

| Advisory | Effort | ETA | Owner |
|----------|--------|-----|-------|
| RUSTSEC-2024-0384 (instant) | Medium | Next reqwest upgrade cycle | Backend |
| RUSTSEC-2024-0436 (paste) | Low | With ratatui 0.30 upgrade | TUI |
| RUSTSEC-2025-0134 (rustls-pemfile) | Low | Monitor upstream | Backend |
| RUSTSEC-2026-0002 (lru) | Low | With ratatui 0.30 upgrade | TUI |

---

## CI Configuration

These warnings are allowed in CI via `cargo audit` defaults.
They will be re-evaluated monthly or when major dependency updates occur.
