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
   `plugin_trust.trusted_keys` (defaults trust Jiandu's, Nova's and Magpie's official
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

## Optional Jiandu connector

Bamboo/Bodhi's built-in memory remains enabled without installing the standalone
Jiandu plugin. The optional connector lets another MCP client use the same
canonical store. It registers no additional memory MCP inside Bamboo.

`GET /api/v1/plugins/jiandu/setup` is passive. It reports the **server process**
OS/architecture and runtime glibc compatibility, publisher/source trust, accepted
immutable release, installed state, and optional `external_client` configuration.
The producer supports macOS x86_64/aarch64, Windows x86_64, and Linux x86_64 with
glibc 2.35 or newer. Unknown hosts and unavailable artifacts stay unavailable.

`POST /api/v1/plugins/official/jiandu/install` takes exactly `{}`. The server
selects the reviewed source and checksum, requires the Jiandu publisher key,
and uses the existing installer with strict trust, even if generic manual plugin
enforcement was switched off. It does not add a key to a custom trust list or
accept caller URLs, paths, commands or bypass flags.

Only a completed, verified managed installation exposes `external_client`:
an absolute `command` and `args: ["--data-dir", "<active canonical root>"]`.
The root comes from the running MemoryStore, including an explicitly selected
root. The server checks provenance, exact manifest/executable digests and managed
paths before export. Incomplete, tampered, symlinked or untrusted installations
do not provide a command. Do not add an `mcp` subcommand or fabricate Project or
Session identities; those identities are authorized by the external MCP host.

Wrap that object as `{"mcpServers":{"jiandu": <external_client>}}` for clients
using that configuration shape. Removal uses the existing plugin delete action;
it removes the connector without deleting canonical memory.

The active memory root must be outside the managed `plugins/jiandu` directory.
Setup and installation refuse an equal or nested root with
`memory_root_overlaps_plugin`; paths that cannot be resolved safely report
`memory_root_unavailable`. Existing filesystem aliases are resolved without
creating a store. If a later server selects a root inside an installed Jiandu
connector, removal also refuses before changing capabilities, provenance or
files. Select an independent memory root before retrying; this check never moves
memory automatically. Sibling roots and roots containing the connector directory
remain allowed because removing the connector does not remove those roots.

## List, update, remove

```bash
# What's installed — id, version, status, registered capability counts, source
bamboo plugin list
bamboo plugin list --json

# Upgrade an installed plugin to a new version (same source/trust flags as install)
bamboo plugin update nova https://github.com/bigduu/Nova/releases/download/v0.3.0/nova-plugin-v0.3.0.tar.gz

# Uninstall — stops/removes its registered MCP servers and prompt presets,
# then deletes its plugin directory (including in-place skills/workflows).
# Confirms unless --yes.
bamboo plugin remove nova
```

`update` drops capabilities the new version no longer declares before
registering the new set — nothing from the old version lingers after an
upgrade that removes a capability.

## After installing

A plugin's registered MCP servers/prompt presets/skills/workflows are live
immediately — no restart needed. Skills and legacy workflow markdown are
discovered in place under the plugin directory; workflow files are never copied
into `~/.bamboo/workflows`. Check registered shared capabilities with
`bamboo plugin list --json` (the `registered.mcp_server_ids`, `preset_ids`, and
`skill_dirs` fields), or `bamboo mcp status` for MCP servers specifically.
`workflow_filenames` is retained only to clean provenance written by older
installers and is empty for new installs.

## ToolEvent sink example

[`examples/tool-event-recorder`](../../examples/tool-event-recorder/README.md)
is an independently buildable native service/plugin that consumes only the
public ToolEvent protocol. It includes the generated JSON schema, a
metadata-only golden event, cross-platform bundle layout, explicit permission
upgrade guidance, delivery guarantees and limits, and lifecycle diagnostics.
