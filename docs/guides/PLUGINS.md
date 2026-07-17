# How-to: install and trust plugins

A Bamboo **plugin** is a bundle of MCP servers, prompt presets, skills, and/or
workflows, installed as a unit and registered into a running `bamboo serve`
instance. [Nova](https://github.com/bigduu/Nova) (macOS/Windows desktop
control) is the reference first-party plugin.

All `bamboo plugin` verbs are a thin CLI over `/api/v1/plugins` on a **running
server** — start `bamboo serve` first.

## Install

```bash
# From a local directory (development)
bamboo plugin install ./my-plugin

# From a packaged archive
bamboo plugin install ./my-plugin.tar.gz

# The official nova plugin, straight from its trusted GitHub release —
# no flags needed once the release is signed by nova's official key
# (both the host and the key are trusted by default; see `plugin_trust`
# in the config reference)
bamboo plugin install https://github.com/bigduu/Nova/releases/download/v0.2.0/nova-plugin-v0.2.0.tar.gz
```

`install` fails if the plugin id is already installed — use [`update`](#update)
for that. Local sources (`local_dir`/`local_archive`) install unconditionally;
`url` sources go through the trust model below.

## Trust model for `url` sources

A plugin fetched from a URL is checked against **three independent, stacked
layers**, secure by default:

1. **Host allowlist** — the URL's host+path must match an entry in
   `plugin_trust.trusted_hosts` (config.json; defaults to
   `github.com/bigduu/`) unless you pass `--allow-untrusted-host`.
2. **Signature** — the bundle's `<url>.sig` must verify against a key in
   `plugin_trust.trusted_keys` (defaults trust nova's and magpie's official
   signing keys) unless you pass `--allow-unsigned`.
3. **Checksum** — `--sha256 <hex>` pins the downloaded bundle. Without it, a
   `url` install is refused unless you pass `--allow-unverified` — **or** the
   bundle already passed layer 2 (a verified signature is a stronger
   guarantee than a hand-pasted checksum, so it satisfies this layer on its
   own).

Net effect: `bamboo plugin install <official nova release URL>` needs **no
flags at all**. An install from anywhere else needs the matching explicit
opt-out(s):

```bash
# Untrusted host, but you pin the checksum and trust the host itself
bamboo plugin install https://example.com/my-plugin.tar.gz \
  --sha256 3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1 \
  --allow-untrusted-host --allow-unsigned

# Fully untrusted source, accepting the risk explicitly
bamboo plugin install https://example.com/my-plugin.tar.gz \
  --allow-untrusted-host --allow-unsigned --allow-unverified

# Shorthand for all three flags together (dev / self-hosted setups only)
bamboo plugin install https://example.com/my-plugin.tar.gz --insecure
```

`--insecure` only turns OFF checks you didn't opt into — a `--sha256` passed
alongside it is still verified (a mismatch still refuses the install). Every
insecure install is logged with a prominent warning and recorded in
provenance, visible via `bamboo plugin list --json`.

For a private/dev instance that never wants to pass flags at all, there's a
persistent config-level equivalent:

```bash
bamboo config set plugin_trust.enforcement off
```

This makes every `url` install/update behave as if `--insecure` were passed,
with no per-install flag needed. It's an explicit opt-in (`enforcement`
defaults to `"strict"`) and the server logs a startup warning while it's set.

## List, update, remove

```bash
# What's installed — id, version, status, registered capability counts, source
bamboo plugin list
bamboo plugin list --json

# Upgrade an installed plugin to a new version (same source/trust flags as install)
bamboo plugin update nova https://github.com/bigduu/Nova/releases/download/v0.3.0/nova-plugin-v0.3.0.tar.gz

# Uninstall — stops/removes its registered MCP servers, prompt presets, and
# workflow files, then deletes its plugin directory. Confirms unless --yes.
bamboo plugin remove nova
```

`update` drops capabilities the new version no longer declares before
registering the new set — nothing from the old version lingers after an
upgrade that removes a capability.

## After installing

A plugin's registered MCP servers/prompt presets/skills/workflows are live
immediately — no restart needed. Check what a plugin registered with
`bamboo plugin list --json` (the `registered.mcp_server_ids` /
`preset_ids` / `skill_dirs` / `workflow_filenames` fields), or `bamboo mcp
status` for its MCP servers specifically.
