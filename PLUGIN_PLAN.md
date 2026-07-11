# Plugin framework — Wave 2 plan

**Temporary coordination doc. Delete this file before the final PR merge
(after all Wave-2 branches are assembled onto `feat/plugin-framework`).**

## What Wave 1 (this branch, `feat/plugin-framework`) built

- `crates/infra/bamboo-plugin` — the foundation crate:
  - `manifest.rs` — `PluginManifest` (`plugin.json` schema), `Platform`,
    `McpServerManifestEntry`/`McpTransportManifest` (with the
    `${plugin_dir}`/`${platform_bin}` token-substitution contract — read the
    module doc comment at the top of the file, it's the source of truth),
    `PluginPromptPreset` (inline, not file-referenced — see the "Design
    decision" doc comment), `PluginArtifact` (URL points at an **archive**;
    see its doc comment for the pinned unpack contract). Fully validated
    (`PluginManifest::validate`) and unit-tested.
  - `registry.rs` — `InstalledPlugins` / `InstalledPlugin` /
    `RegisteredCapabilities` / `PluginSource`, persisted to
    `~/.bamboo/plugins/installed.json`. `load`/`save`/`add`(upsert)/`remove`/
    `get`/`list` implemented + tested. **Plus the two helpers Wave-2's
    installer-core MUST call (do not reinvent):**
    - `reconcile_exclusive(declared, existing, owned_previously) ->
      ExclusiveReconciliation { to_register, foreign_conflicts }` — the
      ownership pre-check for REFUSE-on-conflict capabilities (MCP servers,
      workflow files). Closes BLOCKER 1.
    - `RegisteredCapabilities::removed_since(old) -> RegisteredCapabilities`
      — the upgrade drop-diff (old-minus-new). Closes BLOCKER 2.
  - `installer.rs` — the `PluginInstaller` trait (`install`/`uninstall`/
    `list`) + `InstallDisposition` (`FailIfInstalled` vs `Upgrade`), plus a
    reference skeleton `LocalPluginInstaller` that implements everything
    possible WITHOUT `AppState` (validation, platform gating, MCP-entry token
    resolution, the disposition/already-installed decision, the
    `provides.skills`-authoritative check, on-disk skill/workflow existence
    checks, provenance listing) and returns `PluginError::NotImplemented` at
    the exact point capability registration needs `AppState`. Read the
    `// TODO(installer-core agent): ...` block inside `install`/`uninstall` and
    the module-level "Ownership + upgrade contract" doc — they are the spec,
    reproduced below.
  - `examples/hello-plugin/` — a minimal reference plugin (one skill + one
    inline prompt preset, no binary, no MCP server). Exercised end-to-end by
    `tests/hello_plugin_example.rs`.
- `crates/infra/bamboo-config/src/paths.rs` — added `plugins_dir()`,
  `plugin_dir(id)`, `plugins_installed_json_path()`. Use these — don't
  hardcode `~/.bamboo/plugins/...` paths in Wave-2 code.
- `crates/infra/bamboo-skills` — skill discovery extended: every
  `~/.bamboo/plugins/<id>/skills` directory is an additional
  `SkillDiscoveryDir` (`SkillDirectorySource::Plugin`), globbed fresh on
  every `reload()`/`list_skills(refresh=true)`, sorted by plugin dir for
  DETERMINISTIC same-id resolution, with a WARN on an ambiguous
  (same-precedence) collision. Precedence: Project > Global > Plugin (a
  plugin can never silently shadow a user's own global/project skill of the
  same id — see `SkillStore::source_rank`). **Once a plugin's files are
  unpacked at `~/.bamboo/plugins/<id>/skills/...`, its skills are ALREADY
  discoverable — no registration action needed for skills.** The installer
  only sanity-checks that the declared dirs exist AND that no UNDECLARED
  skill dir is present (`provides.skills` is authoritative — see MAJOR 4
  below), then records the dir names into
  `InstalledPlugin.registered.skill_dirs` for provenance.

### Review-driven schema changes already applied (why the shape is what it is)

- **BLOCKER 1 (ownership)** — `reconcile_exclusive` + `PluginError::Conflict`.
  MCP-server/workflow collisions with a NON-plugin entry are REFUSED, never
  clobbered, and never recorded as removable. So `uninstall` iterating
  `registered.*` provably only deletes entries the plugin created.
- **BLOCKER 2 (upgrade)** — `InstallDisposition` + `removed_since`. `install`
  with `Upgrade` loads the old provenance and de-registers capabilities the
  new version dropped before registering the new set. `install` with
  `FailIfInstalled` (the plain `install` verb) errors `AlreadyInstalled`.
- **MAJOR 3 (artifacts vs platforms)** — `validate()` now rejects an
  `artifacts` key outside the `platforms` gate, and (when `${platform_bin}`
  is used AND URL `artifacts` are shipped) requires an artifact for every
  supported platform. `PluginManifest::uses_platform_bin_token()` /
  `effective_platforms()` are exposed.
- **MAJOR 4 (skills authoritative)** — `LocalPluginInstaller::install`
  rejects any on-disk `skills/*` dir not in `provides.skills`. Discovery
  stays a dumb globber.
- **MINOR 5** — deterministic plugin discovery order + WARN on ambiguous
  collision (above).
- **MINOR 6** — `PluginArtifact.url` = archive (`.zip`/`.tar.gz`), unpack
  contract pinned in its doc comment (binary at archive root named
  `<id>`/`<id>.exe`, placed at `bin/<platform>/<id>[.exe]`).
- **MINOR 7** — `is_valid_preset_id` rejects `general_assistant`
  (bamboo-server's reserved `DEFAULT_PRESET_ID`) so it can't pass validation
  then get silently stripped by `sanitize_store`.

Build/test status: `cargo build --workspace --exclude bamboo-analytics`
green; `cargo test -p bamboo-plugin -p bamboo-skills -p bamboo-config` green
(33 + 2 + 60 + 175 tests, incl. the new ownership/upgrade-diff/
artifact-platform/undeclared-skill/reserved-preset/deterministic-order
tests); `cargo clippy` clean on all three crates; `cargo fmt` applied to
touched files only (`bamboo-config/src/config.rs` untouched).

## Wave 2 — three agents, run in parallel, each stacks on this branch

All three depend on `bamboo-plugin`'s types (stable — do not change its
public API without telling the others). Each agent's OWN new files are listed
first; shared files each agent touches are marked **APPEND-ONLY** — add new
items without reordering surrounding code, to minimize cross-branch conflicts.

### 1. Installer-core agent

**Goal:** make `PluginInstaller::install`/`uninstall` actually register
capabilities, and implement the three plugin sources (local dir, `.tar.gz`,
URL).

New files (suggested; adjust to taste):
- `crates/app/bamboo-server/src/plugin_installer.rs` (or a small
  `bamboo-plugin-server` crate) — a new type, e.g. `ServerPluginInstaller {
  state: actix_web::web::Data<AppState> }`, implementing
  `bamboo_plugin::PluginInstaller` (ordinary downstream impl of a foreign
  trait for a local type — no orphan issue). Reuse
  `bamboo_plugin::manifest::McpServerManifestEntry::resolve`,
  `registry::reconcile_exclusive`, `RegisteredCapabilities::removed_since`,
  and `LocalPluginInstaller`'s validation where useful.
- Source handling (local dir copy / `.tar.gz` unpack / URL fetch + sha256
  verify + per-platform artifact selection + binary placement under
  `bin/<platform>/<id>[.exe]` per the `PluginArtifact` archive contract) — a
  new module, e.g. `crates/app/bamboo-server/src/plugin_source.rs`. Produces
  the `plugin_dir` + `PluginSource` passed into `install()`.

**`install()` sequence — implement EXACTLY this order** (it's the numbered
`TODO` block inside `LocalPluginInstaller::install`, and the ordering is
load-bearing for the ownership/upgrade invariants):

0. **Upgrade drop-diff** (only when the id was already installed and
   `disposition == Upgrade`): compute
   `new_registered.removed_since(&old.registered)` and DE-register those
   dropped mcp ids / preset ids / workflow files (same removal ops as
   `uninstall`) BEFORE registering the new set (BLOCKER 2). The skeleton
   already loads the old provenance and, under `FailIfInstalled`, returns
   `AlreadyInstalled`; you inherit that.
1. **MCP** — `let rec = reconcile_exclusive(declared_mcp_ids,
   existing_mcp_ids_from_config, previously_owned_mcp_ids);` If
   `!rec.foreign_conflicts.is_empty()` → return `PluginError::Conflict { kind:
   "mcp server", .. }` (BLOCKER 1 — do NOT clobber). Otherwise resolve each
   `manifest.provides.mcp_servers[i]` via
   `entry.resolve(plugin_dir, &manifest.id, Platform::current())` and merge
   ONLY the `rec.to_register` subset into `Config.mcp.servers` via
   `AppState::update_config`
   (`crates/app/bamboo-server/src/app_state/config_runtime.rs::update_config`,
   ~line 136), reusing the merge-by-id logic in
   `crates/app/bamboo-server/src/handlers/agent/mcp/server_handlers/import.rs`
   (`import_servers`). Then `state.mcp_manager.start_server(cfg)` for each
   enabled one (pattern at `import.rs` ~lines 94-111). Record exactly
   `rec.to_register` as owned.
2. **Prompts** — append `manifest.provides.prompts` into
   `prompt-presets.json`
   (`crates/app/bamboo-server/src/handlers/agent/prompt_presets/storage.rs`:
   `load_store`/`save_store`/`ensure_unique_preset_id`/`sanitize_store`).
   Preset ids are the RENAME-on-collision case (NOT reconcile_exclusive):
   ids were already shape-checked (`is_valid_preset_id`, incl. the reserved
   `general_assistant`) at manifest-validate time, but re-check uniqueness
   against the LIVE store with `ensure_unique_preset_id` and record the
   ACTUAL (possibly renamed) id as owned.
3. **Workflows** — same REFUSE-on-conflict path as MCP: run
   `reconcile_exclusive` on the workflow filenames against the current
   `workflows_dir()` contents; foreign conflicts → `PluginError::Conflict {
   kind: "workflow", .. }`. Then copy `<plugin_dir>/workflows/<name>.md` into
   `bamboo_config::paths::workflows_dir()/<name>.md` (validate each name with
   `bamboo_config::paths::is_safe_workflow_name`). Record the `to_register`
   subset as owned.
4. **Skills** — nothing to register (discovered in place). Record the
   declared+validated dir names as owned.
5. **Commit provenance** — only after 0-4 succeed, build the
   `RegisteredCapabilities` reflecting EXACTLY what was registered (renamed
   preset ids; the `to_register` mcp/workflow subsets — NOT a blind copy of
   `manifest.provides`), then upsert via `InstalledPlugins::load` + `.add(..)`
   + `.save(..)` at `plugins_installed_json_path()`.

`uninstall(id)`: load the provenance entry; using its `registered.*` (which,
by construction, names ONLY plugin-created entries) stop + remove each
`mcp_server_ids` from config (`update_config` + `mcp_manager.stop_server`),
remove each `preset_ids` from `prompt-presets.json`, delete each
`workflow_filenames` file; then `InstalledPlugins::remove(id)` + `.save(..)`;
finally `tokio::fs::remove_dir_all(entry.plugin_dir)`.

**Shared files touched (append-only):**
- None required if `ServerPluginInstaller` borrows `web::Data<AppState>`
  per-call (like handlers do). If you cache it on `AppState`, append ONE
  field at the end of the struct in
  `crates/app/bamboo-server/src/app_state/mod.rs` and init it at the end of
  the builder in `crates/app/bamboo-server/src/app_state/builder.rs` — don't
  reorder existing fields.

### 2. CLI agent

**Goal:** `bamboo plugin install <path|url> / list / remove <id> / update <id>`.

- `install` → call the installer with `InstallDisposition::FailIfInstalled`
  (surfacing `AlreadyInstalled` as "already installed; use `update`").
- `update` → `InstallDisposition::Upgrade`.
- `remove` → `uninstall`. `list` → `list`.

New file:
- `src/plugin_cli.rs` (sibling to `src/admin_cli.rs`/`src/read_cli.rs`),
  following the server-backed verb pattern (grep `McpCommands::List` in
  `src/bin/bamboo.rs` ~line 1600: thin dispatch arm → fn in `read_cli`/
  `admin_cli` that hits the admin HTTP client). Target the HTTP agent's
  `/api/v1/plugins` endpoints (agree the request/response JSON with them, or
  mirror the MCP CLI-verb ↔ MCP HTTP-route pairing).

**Shared files touched (append-only) — `src/bin/bamboo.rs`:**
- Add `Commands::Plugin { command: PluginCommands }` after the existing
  `Mcp { .. }` variant (~line 397).
- Add `enum PluginCommands { Install { .. }, List { .. }, Remove { .. },
  Update { .. } }` near `enum McpCommands` (~line 445), same flag shape.
- Add a `Commands::Plugin { command } => { match command { .. } }` dispatch
  arm near `Commands::Mcp { command } => { .. }` (~line 1597).
- If there's a "requires a running server" gate list (grep
  `Some(Commands::Mcp { .. })` ~line 994) add `Commands::Plugin { .. }` too.

### 3. HTTP agent

**Goal:** `/api/v1/plugins` — install / list / remove / update.

New files:
- `crates/app/bamboo-server/src/handlers/agent/plugin/mod.rs` (+ submodules),
  following the `handlers/agent/mcp/{mod.rs,server_handlers/,api_types/}`
  split. `install` vs `update` handlers pass the corresponding
  `InstallDisposition`. Map `PluginError::Conflict` →
  409 Conflict, `AlreadyInstalled` → 409, `UnsupportedPlatform` →
  400/422, `NotFound` → 404, `InvalidManifest` → 400. Depend on the
  Installer-core agent's `ServerPluginInstaller`; if running truly parallel,
  code against `bamboo_plugin::PluginInstaller` (trait object/generic) and
  wire the concrete type last.

**Shared files touched (append-only) — `crates/app/bamboo-server/src/routes/agent.rs`:**
- Add `fn plugin_scope() -> impl HttpServiceFactory` (mirror `mcp_scope()`
  ~line 5): `GET /plugins`, `POST /plugins/install`, `DELETE /plugins/{id}`,
  `POST /plugins/{id}/update` (exact paths your call; `/api/v1/plugins...` is
  the hard requirement).
- Register it in `agent_routes` (`web::scope("/api/v1")` builder ~line 39)
  with `scope = scope.service(plugin_scope());` after the existing service
  registrations.
- Add `pub mod plugin;` to the `handlers::agent` module list.

## Coordination points (read this before writing code)

- **Ownership pre-check is mandatory for MCP + workflows** — call
  `bamboo_plugin::reconcile_exclusive` and refuse on `foreign_conflicts`
  (`PluginError::Conflict`). NEVER `*existing = server.clone()` a colliding
  id the way raw `import.rs` does — that clobbers a user's own entry AND (via
  provenance) makes uninstall delete it. Only record `to_register` as owned.
- **Prompt presets are the RENAME exception**, not reconcile_exclusive — use
  `ensure_unique_preset_id`, record the renamed id.
- **Upgrade must drop-diff** — `install(Upgrade)` calls
  `RegisteredCapabilities::removed_since(old)` and de-registers the result
  before registering the new set. Never `add()` new provenance without first
  cleaning up what the old version had and the new one dropped.
- **`config.json` mutation always goes through `AppState::update_config`**
  (`crates/app/bamboo-server/src/app_state/config_runtime.rs`) — never write
  `Config` fields directly or call `Config::save_to_dir`. It's the only path
  that keeps in-memory config, the on-disk file, and reload-race-safety
  (`config_io_lock`) consistent. Applies to the MCP merge; prompt-presets.json
  and workflows/*.md are separate files with their own storage helpers.
- **MCP registration reuses the existing merge logic** — the by-id merge in
  `handlers/agent/mcp/server_handlers/import.rs` (`import_servers`). Factor it
  into a shared helper or call it, but apply it only to the reconciled
  `to_register` subset.
- **Skills need no registration call** — discovered in place. Do NOT copy
  skill files into `~/.bamboo/skills/`; that defeats the discovery-dir
  extension and double-counts on reload. `provides.skills` is authoritative;
  the skeleton already rejects undeclared on-disk skill dirs.
- **The foundation types are shared** — if Wave-2 finds a real gap, change
  `bamboo-plugin` directly (it's a shared dep) and flag it to the other two
  agents; don't shadow/duplicate a type locally.
- **Platform gating** — `Platform::current()` returns `None` on an
  unrecognized OS (fails closed). A manifest with a `platforms` restriction
  refuses to install on an unrecognized host rather than guessing (the
  skeleton already does this).
