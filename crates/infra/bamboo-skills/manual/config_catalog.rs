//! Development-time manual catalog. Never loads instance state or environment.
//! One declaration supplies the typed accessor, default, example and explanation.
use bamboo_config::{
    Config, MemoryConfig, SectionId, ServerConfig, SkillsConfig, StreamTimeoutConfig, TlsConfig,
    ToolsConfig, WorktreeSettings, CONFIG_VALUE_FIELD_OWNERS,
};
use serde_json::{json, Value};

pub struct Entry {
    pub key: &'static str,
    pub kind: &'static str,
    pub example: Value,
    pub meaning: &'static str,
    pub aliases: &'static [&'static str],
    pub read: fn(&Config) -> Value,
    pub required_when: Option<&'static str>,
}

fn memory(c: &Config) -> MemoryConfig {
    c.memory().clone().unwrap_or_default()
}
fn server(c: &Config) -> ServerConfig {
    c.server.clone()
}
fn tls(c: &Config) -> TlsConfig {
    c.server
        .tls
        .clone()
        .expect("TLS children require a configured TLS object")
}
fn timeouts(c: &Config) -> StreamTimeoutConfig {
    c.stream_timeout
}
fn tools(c: &Config) -> ToolsConfig {
    c.tools.clone()
}
fn skills(c: &Config) -> SkillsConfig {
    c.skills.clone()
}
fn worktrees(c: &Config) -> WorktreeSettings {
    c.worktrees.clone()
}

// No `..`: adding ANY field (even a skipped Option) to a covered DTO requires
// updating its documentation or declaring an explicit dynamic-map exclusion.
macro_rules! group {
    ($out:ident, $ty:ident, $root:literal, $getter:ident,
     [$($field:ident => ($kind:literal, $example:expr, $meaning:literal, [$($alias:literal),*])),* $(,)?],
     [$($excluded:ident),*]) => {{
        let _coverage_check = |value: $ty| {
            let $ty { $($field: _,)* $($excluded: _,)* } = value;
        };
        $($out.push(Entry {
            key: concat!($root, ".", stringify!($field)), kind: $kind,
            example: json!($example), meaning: $meaning, aliases: &[$($alias),*],
            read: |c| json!($getter(c).$field),
            required_when: if $root == "server.tls" { Some("server.tls is present") } else { None },
        });)*
    }};
}

pub fn entries() -> Vec<Entry> {
    let mut entries = Vec::new();
    group!(entries, MemoryConfig, "memory", memory, [
        background_model => ("option<string>", "example-background-model", "Optional background-model selector; null falls back to the provider fast model.", []),
        summary_target_ratio => ("f64", 0.3, "Desired summary size as a fraction of source tokens.", []),
        summary_safe_window_percent => ("u8", 60, "Rendered summarization request safety budget as a percent of model context.", []),
        auto_dream_enabled => ("bool", false, "Automatic Dream ticks; configured models, eligible work and runtime gates still matter.", []),
        auto_dream_interval_secs => ("u64", 3600, "Seconds between automatic Dream ticks; does not force work or a model call.", []),
        project_prompt_injection => ("bool", false, "Inject the project durable-memory index into the main prompt.", ["memory.memory_project_prompt_injection"]),
        relevant_recall => ("bool", false, "Enable automatic relevant durable-memory recall.", ["memory.memory_relevant_recall"]),
        relevant_recall_rerank => ("bool", true, "Optional model reranking of lexical candidates; fallback remains lexical.", ["memory.memory_relevant_recall_rerank"]),
        project_first_dream => ("bool", false, "Prefer project Dream over the global fallback.", ["memory.memory_project_first_dream"]),
        ledger_agenda_injection => ("bool", false, "Inject the applicable ledger agenda; an empty agenda adds no section.", ["memory.memory_ledger_agenda_injection"]),
        ledger_gardener_enabled => ("bool", false, "Enable background ledger maintenance; expiry and reconciliation are deterministic.", []),
        ledger_gardener_interval_secs => ("u64", 43200, "Seconds between ledger gardener runs.", []),
        ledger_distillation_enabled => ("bool", false, "Allow model-backed distillation of completed ledger records when a model is available.", []),
        dream_refine_mode => ("bool", true, "Deprecated compatibility input, ignored: it does not enable a Dream rewrite mode.", ["memory.memory_dream_refine_mode"]),
        gardener_enabled => ("bool", false, "Allow model-backed splitting of candidate blob memories subject to runtime gates and caps.", ["memory.memory_gardener_enabled"]),
        gardener_interval_secs => ("u64", 172800, "Seconds between time-triggered gardener passes.", []),
        gardener_volume_trigger => ("usize", 0, "New-memory threshold for early gardening; zero disables the volume trigger, not timed passes.", []),
        gardener_max_splits_per_run => ("usize", 2, "Maximum model-backed splits in one gardener run.", []),
        gardener_min_sections => ("usize", 8, "Minimum separator-delimited sections for a split candidate.", []),
        dedup_gardener_enabled => ("bool", false, "Allow bounded model-backed consolidation of near-duplicate memories.", ["memory.memory_dedup_gardener_enabled"]),
        dedup_gardener_min_score => ("f64", 0.9, "Content-keyword Jaccard threshold for dedup candidates; not embedding similarity.", []),
        dedup_gardener_max_merges_per_run => ("usize", 2, "Maximum model-backed consolidations in one dedup pass.", []),
        memory_active_capacity => ("usize", 1000, "Recallable-memory capacity per scope; zero disables capacity archiving, exempt types remain protected.", []),
        capacity_max_archivals_per_run => ("usize", 2, "Maximum reversible capacity archivals in one pass; not deletion.", []),
        granularity_freshness_gardener_enabled => ("bool", false, "Allow deterministic granularity freshness demotion from Active to Stale; no model call.", []),
    ], []);
    group!(entries, ServerConfig, "server", server, [
        port => ("u16", 9652, "HTTP listener port; a serve CLI override can change runtime state.", []),
        bind => ("string", "0.0.0.0", "Listener bind address; changing it can expose the service and needs separate authorization.", []),
        static_dir => ("option<path>", "example-static", "Optional static-files directory, not an observed user path.", []),
        workers => ("usize", 2, "Actix HTTP worker count.", []),
        tls => ("option<object>", json!({"cert_file":"example-cert.pem","key_file":"example-key.pem"}), "Optional TLS configuration. Paths identify files, never embedded private-key contents. Missing or invalid files fail startup.", []),
    ], [extra]);
    group!(entries, TlsConfig, "server.tls", tls, [
        cert_file => ("path", "example-cert.pem", "PEM certificate-chain file path, applicable only when server.tls is present.", []),
        key_file => ("path", "example-key.pem", "PEM private-key file path, applicable only when server.tls is present; never include key contents.", []),
    ], []);
    group!(entries, StreamTimeoutConfig, "stream_timeout", timeouts, [
        transport_idle_timeout_secs => ("u64", 300, "Maximum gap between valid transport frames, including keepalives; accepted range 1..86400 seconds.", []),
        first_semantic_timeout_secs => ("u64", 900, "Deadline to the first semantic chunk after request dispatch; accepted range 1..86400 seconds.", []),
        semantic_idle_timeout_secs => ("u64", 900, "Maximum gap between semantic progress after the first chunk; accepted range 1..86400 seconds.", []),
    ], []);
    group!(entries, ToolsConfig, "tools", tools, [
        disabled => ("array<string>", vec!["WebFetch"], "Globally omitted tool names; does not grant permissions.", []),
    ], [extra]);
    group!(entries, SkillsConfig, "skills", skills, [
        disabled => ("array<string>", vec!["bamboo-manual"], "Globally unavailable skill IDs, including the manual if listed.", []),
    ], [extra]);
    group!(entries, WorktreeSettings, "worktrees", worktrees, [
        default_root => ("option<string>", "example-worktrees", "Optional default root for user-created worktrees, not a runtime path observation.", []),
        project_roots => ("map<string,string>", json!({"example-project":"example-worktrees"}), "Per-project worktree root overrides; dynamic map keys are project identifiers, not config-field names.", []),
    ], []);
    entries.push(Entry { key: "lifecycle_hooks.enabled", kind: "bool", example: json!(true),
        meaning: "Native lifecycle-hook switch; handler configuration and authorization remain separate.",
        aliases: &[], read: |c| json!(c.lifecycle_hooks.enabled), required_when: None });
    entries.push(Entry { key: "context_management.strategy", kind: "enum", example: json!("retrieval_window"),
        meaning: "Context strategy; retrieval-window validation and history-tool requirements still apply.",
        aliases: &[], read: |c| json!(c.context_management.strategy), required_when: None });
    entries.sort_by_key(|entry| entry.key);
    entries
}

pub fn owner(key: &str) -> SectionId {
    if key.split('.').next() == Some("memory") {
        return SectionId::Memory;
    }
    CONFIG_VALUE_FIELD_OWNERS
        .iter()
        .find(|(root, _)| key.split('.').next() == Some(*root))
        .expect("typed key must have an owner")
        .1
}

pub fn catalog() -> Value {
    let defaults = Config::default();
    let entries = entries();
    let mut roots: Vec<Value> = CONFIG_VALUE_FIELD_OWNERS.iter().map(|(root, section)| {
        let count = entries.iter().filter(|entry| entry.key.split('.').next() == Some(*root)).count();
        json!({"root":root, "section":section.descriptor().name,
            "documented_entries":count, "coverage":if count == 0 {"not_documented"} else {"selected_fields"},
            "kind": if *root == "extra" {"flattened_dynamic_map_not_literal_key"} else {"compatibility_dto_field"}})
    }).collect();
    roots.push(
        json!({"root":"memory", "section":"memory", "coverage":"typed_fields",
        "documented_entries": entries.iter().filter(|e| e.key.starts_with("memory.")).count()}),
    );
    roots.sort_by(|a, b| a["root"].as_str().cmp(&b["root"].as_str()));
    json!({
        "format_version": 2,
        "coverage": "bounded_typed_catalog_not_full_schema",
        "representation": "Config compatibility view; not section-file envelopes",
        "default_scope": "typed input defaults, not effective instance values; required TLS children have no documented default",
        "unknown_key_meaning": "not covered here; absence does not mean unsupported",
        "generator": "manual/config_catalog.rs; cargo run -p bamboo-skills --example generate_manual_catalog -- --check",
        "source": "crates/infra/bamboo-config/src/config.rs",
        "root_coverage_scope": "ConfigValues ownership inventory plus memory; not an exhaustive list of sidecars, environment variables or sections",
        "root_coverage": roots,
        "exhaustive_typed_groups": ["memory", "server", "server.tls", "stream_timeout", "tools", "skills", "worktrees"],
        "dynamic_maps": ["server.<unknown>", "tools.<unknown>", "skills.<unknown>", "worktrees.project_roots.<project>"],
        "entries": entries.iter().map(|entry| {
            let mut value = json!({"key":entry.key, "section":owner(entry.key).descriptor().name,
                "type":entry.kind, "example":entry.example,
                "meaning":entry.meaning, "aliases":entry.aliases});
            if let Some(condition) = entry.required_when {
                value["required_when"] = json!(condition);
            } else {
                value["default"] = (entry.read)(&defaults);
            }
            if entry.key == "context_management.strategy" {
                value["values"] = json!(["summary", "message_summary", "retrieval_window"]);
            }
            if entry.key == "memory.dream_refine_mode" { value["status"] = json!("deprecated_ignored"); }
            value
        }).collect::<Vec<_>>()
    })
}

pub fn render() -> String {
    format!(
        "{}\n",
        serde_json::to_string_pretty(&catalog()).expect("static catalog serializes")
    )
}
