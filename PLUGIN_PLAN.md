# Plugin framework — Wave 2 plan

**Temporary coordination doc. Delete this file before the final PR merge
(after all Wave-2 branches are assembled onto `feat/plugin-framework`).**

## What Wave 1 (this branch, already merged into `feat/plugin-framework`) built

- `crates/infra/bamboo-plugin` — the foundation crate:
  - `manifest.rs` — `PluginManifest` (`plugin.json` schema), `Platform`,
    `McpServerManifestEntry`/`McpTransportManifest` (with the
    `${plugin_dir}`/`${platform_bin}` token-substitution contract —
    read the module doc comment at the top of the file, it's the source of
    truth), `PluginPromptPreset` (inline, not file-referenced — see the
    "Design decision" doc comment), `PluginArtifact`. Fully validated
    (`PluginManifest::validate`) and unit-tested.
  - `registry.rs` — `InstalledPlugins` / `InstalledPlugin` /
    `RegisteredCapabilities` / `PluginSource`, persisted to
    `~/.bamboo/plugins/installed.json`. `load`/`save`/`add`(upsert)/`remove`/
    `get`/`list` are all implemented and tested (round-trip test included).
  - `installer.rs` — the `PluginInstaller` trait (`install`/`uninstall`/
    `list`) plus a reference skeleton `LocalPluginInstaller` that implements
    everything possible WITHOUT `AppState` (manifest validation, platform
    gating, MCP-entry token resolution, on-disk skill/workflow existence
    checks, provenance listing) and returns `PluginError::NotImplemented` at
    the exact point capability registration needs to happen. Read the
    `// TODO(installer-core agent): ...` block inside `install`/`uninstall`
    — it enumerates the exact steps and file citations, reproduced below.
  - `examples/hello-plugin/` — a minimal reference plugin (one skill + one
    inline prompt preset, no binary, no MCP server) with a real
    `plugin.json` + `SKILL.md`. Exercised end-to-end by
    `tests/hello_plugin_example.rs`.
- `crates/infra/bamboo-config/src/paths.rs` — added `plugins_dir()`,
  `plugin_dir(id)`, `plugins_installed_json_path()` (mirrors the existing
  `workflows_dir()`/`config_json_path()` helpers). Use these — don't
  hardcode `~/.bamboo/plugins/...` paths in Wave-2 code.
- `crates/infra/bamboo-skills` — skill discovery extended: every
  `~/.bamboo/plugins/<id>/skills` directory is now an additional
  `SkillDiscoveryDir` (`SkillDirectorySource::Plugin`), globbed fresh on
  every `reload()`/`list_skills(refresh=true)` call (see
  `store/storage.rs::discover_plugin_skill_dirs` and
  `store/mod.rs::SkillStore::plugins_root_dir` /
  `resolve_skills_maps_for_mode`). Precedence: Project > Global > Plugin (a
  plugin can never silently shadow a user's own global/project skill of the
  same id — see `SkillStore::source_rank`). **This means: once a plugin's
  files are unpacked at `~/.bamboo/plugins/<id>/skills/...`, its skills are
  ALREADY discoverable — no registration action needed for skills.** The
  installer only needs to sanity-check the declared dirs contain `SKILL.md`
  (already done in `LocalPluginInstaller::install`) and record the dir names
  into `InstalledPlugin.registered.skill_dirs` for provenance/uninstall
  bookkeeping.

Build/test status as of this branch: `cargo build --workspace --exclude
bamboo-analytics` green; `cargo test -p bamboo-plugin -p bamboo-skills -p
bamboo-config` green (21 + 2 + 59 + 175 tests); `cargo clippy` clean on all
three touched crates; `cargo fmt` applied to touched files only (did NOT
whole-file-reformat `bamboo-config/src/config.rs` — that file wasn't
touched).

## Wave 2 — three agents, run in parallel, each stacks on this branch

All three depend on `bamboo-plugin`'s types (already stable — do not change
its public API without telling the others). Each agent's OWN new files are
listed first; shared files each agent touches are marked **APPEND-ONLY** —
add new items without reordering/reformatting surrounding code, to keep
diffs small and non-conflicting between the three branches.

### 1. Installer-core agent

**Goal:** make `PluginInstaller::install`/`uninstall` actually register
capabilities, and implement the three plugin sources (local dir, `.tar.gz`,
URL).

New files (suggested; adjust to taste):
- `crates/app/bamboo-server/src/plugin_installer.rs` (or a small
  `bamboo-plugin-server` crate if you'd rather keep `bamboo-server`'s file
  count down) — a new type, e.g. `ServerPluginInstaller { state:
  actix_web::web::Data<AppState> }`, implementing `bamboo_plugin::PluginInstaller`.
  This is an ordinary downstream `impl` of a foreign trait for a local type
  — no orphan-rule issue. It can reuse
  `bamboo_plugin::manifest::McpServerManifestEntry::resolve` and
  `bamboo_plugin::installer::LocalPluginInstaller`'s validation logic
  (either call into `LocalPluginInstaller` for the parts it already does, or
  copy the ~15 lines of validation — your call, they're small and pure).
- Source handling (local dir copy / `.tar.gz` unpack / URL fetch + sha256
  verify + per-platform artifact selection from `PluginManifest.artifacts`)
  — a new module, e.g. `crates/app/bamboo-server/src/plugin_source.rs`. This
  produces the `plugin_dir` + `PluginSource` that gets passed into
  `install()`. Note `bamboo_plugin::manifest::PluginArtifact { url, sha256 }`
  is already the schema for the URL case; you're just filling in the fetch.

Exact wiring, step by step (this is the same list already written as a
`TODO` comment inside `LocalPluginInstaller::install` in
`crates/infra/bamboo-plugin/src/installer.rs` — treat that comment as the
spec):

1. **MCP** — resolve each `manifest.provides.mcp_servers[i]` via
   `entry.resolve(plugin_dir, &manifest.id, Platform::current())` (pure,
   already implemented), then merge the results into `Config.mcp.servers`
   via `AppState::update_config`
   (`crates/app/bamboo-server/src/app_state/config_runtime.rs::update_config`,
   line ~136). Reuse the **merge-by-id** logic already written for the
   existing bulk-import endpoint at
   `crates/app/bamboo-server/src/handlers/agent/mcp/server_handlers/import.rs`
   (`import_servers`) — don't reinvent it, that function already handles
   "insert if absent, replace if id exists" against `root.mcp.servers`. Then
   call `state.mcp_manager.start_server(server_cfg)` for each entry with
   `enabled == true` (same pattern as `import.rs` lines ~94-111).
2. **Prompts** — append `manifest.provides.prompts` into
   `prompt-presets.json`. The storage helpers are in
   `crates/app/bamboo-server/src/handlers/agent/prompt_presets/storage.rs`:
   `load_store`/`save_store`/`validate_preset_id`/`ensure_unique_preset_id`/
   `sanitize_store`. A plugin preset's `id` was already checked against the
   same rule (`bamboo_plugin::manifest::is_valid_preset_id` — kept in sync
   deliberately) at manifest-validate time, but re-check uniqueness against
   the LIVE store with `ensure_unique_preset_id` before appending (a
   colliding id must not clobber a user's own preset — rename the plugin's,
   don't overwrite the user's).
3. **Workflows** — copy `<plugin_dir>/workflows/<name>.md` into
   `bamboo_config::paths::workflows_dir()/<name>.md` for each declared name
   (existence on the plugin side is already checked in
   `LocalPluginInstaller::install`). Validate the destination name with
   `bamboo_config::paths::is_safe_workflow_name` before writing (belt and
   suspenders — manifest validation already rejects non-`.md`/traversal
   names, but this is the one capability that isn't a discovery-dir, it's a
   real file copy into a shared directory, so re-validate at the point of
   write).
4. **Skills** — nothing to register (see the discovery-in-place note
   above). Just record the validated dir names into
   `RegisteredCapabilities.skill_dirs`.
5. **Commit provenance** — only after 1-3 succeed, build the
   `RegisteredCapabilities` reflecting exactly what was registered (not a
   copy of `manifest.provides` — if step 2 renamed a colliding preset id,
   record the RENAMED id, not the manifest's original one), then
   `InstalledPlugins::load(&bamboo_config::paths::plugins_installed_json_path())`
   + `.add(InstalledPlugin { .. })` + `.save(..)`.

`uninstall(id)`: load the provenance entry, reverse 1-3 using
`registered.{mcp_server_ids,preset_ids,workflow_filenames}` (stop +
`mcp.servers.retain(id != ..)` via `update_config`; remove matching entries
from `prompt-presets.json`; delete the workflow files), then
`InstalledPlugins::remove(id)` + `.save(..)`, then
`tokio::fs::remove_dir_all(entry.plugin_dir)`.

**Shared files touched (append-only):**
- None required — `AppState` doesn't need a new field if
  `ServerPluginInstaller` just borrows `web::Data<AppState>` per-call (like
  handlers already do). If you DO want it cached on `AppState`, add one new
  field at the end of the struct in
  `crates/app/bamboo-server/src/app_state/mod.rs` and initialize it at the
  end of the builder in `crates/app/bamboo-server/src/app_state/builder.rs`
  — append, don't reorder existing fields (both structs are already large;
  reordering causes needless conflicts with the other two agents' reads of
  the same files, if any).

### 2. CLI agent

**Goal:** `bamboo plugin install <path|url> / list / remove <id> / update <id>`.

New file:
- `src/plugin_cli.rs` (sibling to `src/admin_cli.rs`/`src/read_cli.rs`) —
  the verb implementations, following the existing pattern for other
  server-backed verbs (see `mcp_list`/`mcp_add` etc. in `src/read_cli.rs` —
  git-blame/grep `McpCommands::List` in `src/bin/bamboo.rs` line ~1600 for
  the calling convention: thin dispatch arm → a function in `read_cli`/
  `admin_cli` that hits the admin HTTP client). Since Wave-2's HTTP agent is
  adding `/api/v1/plugins` routes in parallel, target those endpoints (agree
  on the request/response JSON shape with the HTTP agent, or just mirror
  the existing MCP CLI-verb ↔ MCP HTTP-route pairing).

**Shared files touched (append-only):**
- `src/bin/bamboo.rs`:
  - Add one new `Commands::Plugin { command: PluginCommands }` variant to
    the `Commands` enum (append after the existing `Mcp { .. }` variant,
    currently ~line 397, so the diff is a clean insertion, not a reorder).
  - Add a new `enum PluginCommands { Install { .. }, List { .. }, Remove {
    .. }, Update { .. } }` near (not necessarily adjacent to, just don't
    disturb) the existing `enum McpCommands` (~line 445) — follow its exact
    shape (each variant takes whatever `--data-dir`/`--conn`-style flags the
    other verbs take for consistency).
  - Add one dispatch arm `Commands::Plugin { command } => { match command {
    ... } }` near the existing `Commands::Mcp { command } => { ... }` arm
    (~line 1597) — append, mirror its structure.
  - If there's a "requires a running server" gate list (grep
    `Some(Commands::Mcp { .. })` — ~line 994) add `Commands::Plugin { .. }`
    to it too, same append pattern.

### 3. HTTP agent

**Goal:** `/api/v1/plugins` — install / list / remove / update.

New files:
- `crates/app/bamboo-server/src/handlers/agent/plugin/mod.rs` (+
  submodules as needed, following the existing
  `handlers/agent/mcp/{mod.rs,server_handlers/,api_types/}` split — that's
  the precedent to copy: request/response DTOs in `api_types`, handler fns
  in one or more files, `mod.rs` re-exporting).
  Handlers should depend on the Installer-core agent's
  `ServerPluginInstaller` (or whatever it's named) — if that agent's PR
  lands first, import it directly; if run truly in parallel, code against
  `bamboo_plugin::PluginInstaller` as a trait object /
  generic so you're not blocked on the concrete type, and wire the concrete
  type in last.

**Shared files touched (append-only):**
- `crates/app/bamboo-server/src/routes/agent.rs`:
  - Add a new `fn plugin_scope() -> impl HttpServiceFactory` (mirror
    `mcp_scope()` at the top of the file, ~line 5) with routes like
    `.route("/plugins", web::get().to(agent::plugin::list_plugins))`,
    `.route("/plugins/install", web::post().to(agent::plugin::install_plugin))`,
    `.route("/plugins/{id}", web::delete().to(agent::plugin::remove_plugin))`,
    `.route("/plugins/{id}/update", web::post().to(agent::plugin::update_plugin))`
    — exact paths are your call, `/api/v1/plugins...` is the only hard
    requirement (task spec).
  - Register it in `agent_routes` (the `web::scope("/api/v1")` builder,
    ~line 39) with `scope = scope.service(plugin_scope());` — append after
    the existing `.service(mcp_scope())`/similar line, don't reorder.
  - Add `pub mod plugin;` to the `handlers::agent` module list (wherever
    `pub mod mcp;` etc. currently lives) — append.

## Coordination points (read this before writing code)

- **`config.json` mutation always goes through `AppState::update_config`**
  (`crates/app/bamboo-server/src/app_state/config_runtime.rs`) — never
  write `Config` fields directly and never call `Config::save_to_dir`
  yourselves. `update_config` is the ONLY path that keeps the in-memory
  config, the on-disk file, and reload-race-safety (`config_io_lock`, see
  the doc comment on `update_config`) consistent. This applies to the MCP
  merge AND nothing else the plugin system touches (prompt-presets.json and
  workflows/*.md are separate files with their own storage helpers, not part
  of `Config`).
- **MCP registration reuses the existing import/merge logic** — see
  `crates/app/bamboo-server/src/handlers/agent/mcp/server_handlers/import.rs`
  (`import_servers`, the `POST /api/v1/mcp/servers/import` handler). Don't
  write a second "insert-or-replace by id into `root.mcp.servers`"
  implementation; factor the loop out of `import.rs` into a shared helper
  if it's easiest, or call it directly if the shapes already line up.
- **Prompt preset id collisions are a MUST-NOT-CLOBBER case.** A plugin
  should never silently overwrite a user's own preset (or another plugin's)
  just because ids match — rename (via
  `ensure_unique_preset_id`, already written) and record the ACTUAL
  registered id in provenance, not the manifest's nominal one.
  `sanitize_store` also runs after any mutation — call it (see how the
  existing prompt-preset handlers do it).
- **Skills need no registration call at all** — see the "discovery
  in-place" note above. Resist the urge to copy skill files into
  `~/.bamboo/skills/` "to be safe"; that defeats the whole point of the
  discovery-dir extension (and would double-count on reload).
- **`PluginManifest`/`InstalledPlugins`/`PluginInstaller` are the frozen
  foundation types for this wave.** If Wave-2 work reveals a real gap in
  them (a field that's missing, a validation rule that's wrong), that's a
  legitimate finding — but change them in `bamboo-plugin` directly (it's a
  shared dependency, not owned by any one Wave-2 branch) and flag the change
  to the other two agents rather than shadowing/duplicating the type
  locally.
- **Platform gating**: `bamboo_plugin::manifest::Platform::current()`
  returns `None` on an OS Bamboo doesn't recognize (fails closed — see its
  doc comment). Any Wave-2 code gating on platform should treat `None` the
  same way `LocalPluginInstaller::install` already does: a manifest with a
  `platforms` restriction refuses to install on an unrecognized host rather
  than guessing.
