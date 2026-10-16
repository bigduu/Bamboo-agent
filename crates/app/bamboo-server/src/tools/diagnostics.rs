//! Allowlisted process observations, never a configuration dump or an active probe.
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::PromptMemoryObservability;
use serde_json::{json, Value};

use crate::app_state::{ConfigApplicationState, ConfigLiveHealth};

const MEMORY_KEY: &str = "runtime_prompt_memory_observability";
const MAX_MEMORY_BYTES: usize = 16 * 1024;

pub(crate) struct ReadSources {
    pub providers: Arc<RwLock<ConfigLiveHealth>>,
    pub mcp: Arc<RwLock<ConfigLiveHealth>>,
    pub application: Arc<RwLock<ConfigApplicationState>>,
    pub sessions: bamboo_engine::SessionCache,
    pub config_io_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Late binding shares the existing owners; no duplicate snapshots or background work.
#[derive(Default)]
pub(crate) struct DiagnosticsSource(OnceLock<ReadSources>);

impl DiagnosticsSource {
    pub(crate) fn bind(&self, sources: ReadSources) {
        assert!(self.0.set(sources).is_ok(), "diagnostics bound twice");
    }
}

pub(crate) struct BambooDiagnosticsTool {
    source: Arc<DiagnosticsSource>,
}

impl BambooDiagnosticsTool {
    pub(crate) fn new(source: Arc<DiagnosticsSource>) -> Self {
        Self { source }
    }

    async fn snapshot(&self, session_id: &str) -> Value {
        let Some(sources) = self.source.0.get() else {
            return json!({"schema_version":1,"availability":"unknown","reason":"runtime_unbound"});
        };
        // Same publication boundary as live-health. No reload, file reads,
        // credential hydration or desired-input digest computation.
        let config = {
            let _guard = sources.config_io_lock.lock().await;
            json!({
                "providers": safe_health(&sources.providers),
                "mcp": safe_health(&sources.mcp),
                "application": sources.application.read().map(|state| state.safe_observed_receipts())
                    .unwrap_or_else(|_| json!({"availability":"unknown","reason":"state_unavailable"})),
            })
        };
        let memory = sources
            .sessions
            .get(session_id)
            .map(|entry| {
                let session = entry.value().read();
                safe_memory(session.metadata.get(MEMORY_KEY).map(String::as_str))
            })
            .unwrap_or_else(|| json!({"availability":"unknown","reason":"session_not_cached"}));
        json!({
            "schema_version":1,
            "availability":"observed",
            "server_version":env!("CARGO_PKG_VERSION"),
            "embedded_manual":manual_identity(),
            "observed_at":chrono::Utc::now(),
            "config":config,
            "memory":memory,
            "limits":{
                "active_probes":"not_performed",
                "cross_subsystem_snapshot":"not_atomic",
                "desired_config_comparison":"not_performed",
                "memory_evidence":"cached_prompt_assembly_only",
                "model_received_or_used_memory":"unknown",
                "changes_performed":false,
            },
            "manual_resource":"bamboo-manual/references/troubleshooting.md",
        })
    }
}

// Static build artifact only, not the user-overridable skill catalog.
fn manual_identity() -> Value {
    let manifest = bamboo_skills::store::builtin::BUILTIN_SKILL_FILES
        .iter()
        .find(|(path, _)| *path == "bamboo-manual/manifest.json")
        .and_then(|(_, bytes)| serde_json::from_slice::<Value>(bytes).ok());
    match manifest {
        Some(manifest) => json!({
            "package_version":manifest["package_version"],
            "source_revision":manifest["source"]["revision"],
            "source_status":manifest["source"]["status"],
            "source_dirty":manifest["source"]["dirty"],
            "content_sha256":manifest["content_sha256"],
            "version_matches_server":manifest["package_version"] == env!("CARGO_PKG_VERSION"),
        }),
        None => json!({"availability":"unknown"}),
    }
}

fn safe_health(health: &RwLock<ConfigLiveHealth>) -> Value {
    match health.read() {
        Ok(health) => {
            json!({"status":health.status,"revision":health.revision,"loaded_at":health.loaded_at})
        }
        Err(_) => json!({"status":"unknown"}),
    }
}

// Never copy a runtime status string, even when future producers add variants.
fn status(value: &str) -> &'static str {
    match value {
        "empty" => "empty",
        "loaded" => "loaded",
        "loaded_truncated" => "loaded_truncated",
        "disabled" => "disabled",
        "missing" => "missing",
        "no_project_key" => "no_project_key",
        "no_query" => "no_query",
        "no_match" => "no_match",
        "lexical" => "lexical",
        "reranked" => "reranked",
        "rerank_fallback" => "rerank_fallback",
        "forced_loaded" => "forced_loaded",
        "forced_loaded_truncated" => "forced_loaded_truncated",
        "forced_missing" => "forced_missing",
        "skipped_project_memory_or_dream_present" => "skipped_project_memory_or_dream_present",
        "fallback_loaded" => "fallback_loaded",
        "fallback_loaded_truncated" => "fallback_loaded_truncated",
        "fallback_missing" => "fallback_missing",
        "project" => "project",
        "global_fallback" => "global_fallback",
        "none" => "none",
        _ => "unknown",
    }
}

fn safe_memory(raw: Option<&str>) -> Value {
    let parsed = raw
        .filter(|raw| raw.len() <= MAX_MEMORY_BYTES)
        .and_then(|raw| serde_json::from_str::<PromptMemoryObservability>(raw).ok());
    let Some(m) = parsed else {
        return json!({"availability":"unknown","reason":"capture_missing_or_invalid"});
    };
    json!({
        "availability":"cached",
        "captured_at":null,
        "freshness":"cached_capture_time_unknown",
        "session_notes_status":status(&m.session_notes_status),
        "project_memory_index_status":status(&m.project_memory_index_status),
        "relevant_memory_status":status(&m.relevant_memory_status),
        "project_dream_status":status(&m.project_dream_status),
        "global_dream_fallback_status":status(&m.global_dream_fallback_status),
        "dream_source":status(&m.dream_source),
        "session_topic_count":m.session_topic_count,
        "truncated_session_topic_count":m.truncated_session_topic_count,
        "relevant_memory_count":m.relevant_memory_count,
        "session_note_section_chars":m.session_note_section_chars,
        "project_memory_index_section_chars":m.project_memory_index_section_chars,
        "relevant_memory_section_chars":m.relevant_memory_section_chars,
        "project_dream_section_chars":m.project_dream_section_chars,
        "global_dream_fallback_section_chars":m.global_dream_fallback_section_chars,
        "context_pressure_warning_chars":m.context_pressure_warning_chars,
        "external_memory_section_chars":m.external_memory_section_chars,
    })
}

#[async_trait]
impl Tool for BambooDiagnosticsTool {
    fn name(&self) -> &str {
        "bamboo_diagnostics"
    }
    fn description(&self) -> &str {
        "Read allowlisted local Bamboo configuration health/revisions and the current session's cached memory-injection status/counts. No active probes, file/config/credential reads, repairs or reloads. Missing evidence is unknown; cached counts do not prove a model received or used memory. Consult the version-bound bamboo-manual for interpretation. Takes no arguments."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }
    fn classify(&self, _args: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if !args.as_object().is_some_and(|args| args.is_empty()) {
            return Err(ToolError::InvalidArguments(
                "bamboo_diagnostics takes no arguments".into(),
            ));
        }
        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("bamboo_diagnostics requires a current session".into())
        })?;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            self.snapshot(session_id).await.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;
    use bamboo_config::{SectionSourceKind, SectionStatus};
    use bamboo_engine::SessionSnapshot;

    const CANARY: &str = "PRIVATE_CANARY_path_token_memory";

    fn health() -> Arc<RwLock<ConfigLiveHealth>> {
        Arc::new(RwLock::new(ConfigLiveHealth {
            revision: 7,
            loaded_at: chrono::Utc::now(),
            source_path: CANARY.into(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Degraded,
            last_error: Some(CANARY.into()),
        }))
    }

    fn memory() -> PromptMemoryObservability {
        PromptMemoryObservability {
            resolved_project_key: Some(CANARY.into()),
            session_notes_status: "loaded".into(),
            project_memory_index_status: CANARY.into(),
            relevant_memory_status: CANARY.into(),
            project_dream_status: CANARY.into(),
            global_dream_fallback_status: CANARY.into(),
            dream_source: CANARY.into(),
            session_topic_count: 3,
            ..Default::default()
        }
    }

    #[test]
    fn memory_projection_never_echoes_free_text_or_unknown_fields() {
        let mut value = serde_json::to_value(memory()).unwrap();
        value["future_secret"] = json!(CANARY);
        let output = safe_memory(Some(&value.to_string()));
        assert!(!output.to_string().contains(CANARY));
        assert_eq!(output["session_topic_count"], 3);
        assert_eq!(output["session_notes_status"], "loaded");
        assert_eq!(output["dream_source"], "unknown");
        assert!(output["captured_at"].is_null());
        assert!(output.get("resolved_project_key").is_none());
        for raw in [
            None,
            Some("malformed"),
            Some("{}"),
            Some(&"x".repeat(MAX_MEMORY_BYTES + 1)),
        ] {
            assert_eq!(safe_memory(raw)["availability"], "unknown");
        }
    }

    #[tokio::test]
    async fn diagnostics_is_current_session_only_and_does_not_change_sources() {
        let sessions: bamboo_engine::SessionCache = Default::default();
        let mut current = Session::new("current", "model");
        current.title = CANARY.into();
        current
            .metadata
            .insert(MEMORY_KEY.into(), serde_json::to_string(&memory()).unwrap());
        current.metadata.insert(CANARY.into(), CANARY.into());
        let current = Arc::new(SessionSnapshot::new(current));
        sessions.insert("current".into(), current.clone());
        let mut other = Session::new("other", "model");
        let mut other_memory = memory();
        other_memory.session_topic_count = 999;
        other.metadata.insert(
            MEMORY_KEY.into(),
            serde_json::to_string(&other_memory).unwrap(),
        );
        sessions.insert("other".into(), Arc::new(SessionSnapshot::new(other)));
        let providers = health();
        let application = Arc::new(RwLock::new(ConfigApplicationState::default()));
        let before_health = serde_json::to_value(&*providers.read().unwrap()).unwrap();
        let before_session = serde_json::to_value(&*current.read()).unwrap();
        let before_receipts = application.read().unwrap().safe_observed_receipts();
        let source = Arc::new(DiagnosticsSource::default());
        source.bind(ReadSources {
            providers: providers.clone(),
            mcp: health(),
            application: application.clone(),
            sessions: sessions.clone(),
            config_io_lock: Arc::new(tokio::sync::Mutex::new(())),
        });
        let tool = BambooDiagnosticsTool::new(source);
        let mut ctx = ToolCtx::none("diagnostics-test");
        ctx.session_id = Some(Arc::from("current"));
        assert!(matches!(
            tool.invoke(json!({}), ctx).await.unwrap(),
            ToolOutcome::Completed(_)
        ));
        let output = tool.snapshot("current").await;
        assert!(!output.to_string().contains(CANARY));
        assert!(!output.to_string().contains("999"));
        assert_eq!(output["embedded_manual"]["version_matches_server"], true);
        assert!(output["embedded_manual"]["content_sha256"]
            .as_str()
            .is_some_and(|hash| hash.len() == 64));
        assert_eq!(output["memory"]["session_topic_count"], 3);
        assert_eq!(output["config"]["providers"]["revision"], 7);
        assert_eq!(
            output["config"]["application"]["desired_vs_applied"],
            "not_evaluated"
        );
        assert_eq!(output["limits"]["changes_performed"], false);
        assert!(output.to_string().len() < 16 * 1024);
        assert_eq!(
            tool.snapshot("not_cached").await["memory"]["reason"],
            "session_not_cached"
        );
        assert_eq!(sessions.len(), 2);
        assert_eq!(
            serde_json::to_value(&*current.read()).unwrap(),
            before_session
        );
        assert_eq!(
            serde_json::to_value(&*providers.read().unwrap()).unwrap(),
            before_health
        );
        assert_eq!(
            application.read().unwrap().safe_observed_receipts(),
            before_receipts
        );
        assert_eq!(tool.classify(&json!({})), ToolClass::READONLY_PARALLEL);
        for args in [
            json!({"session_id":"other"}),
            json!({"repair":true}),
            json!(null),
        ] {
            assert!(tool
                .invoke(args, ToolCtx::none("diagnostics-test"))
                .await
                .is_err());
        }
        assert!(tool
            .invoke(json!({}), ToolCtx::none("diagnostics-test"))
            .await
            .is_err());
    }

    #[test]
    fn poisoned_health_is_unknown_without_repair() {
        let health = health();
        let worker = health.clone();
        assert!(std::thread::spawn(move || {
            let _guard = worker.write().unwrap();
            panic!("synthetic lock failure");
        })
        .join()
        .is_err());
        assert_eq!(safe_health(&health), json!({"status":"unknown"}));
        assert!(health.is_poisoned());
    }

    #[tokio::test]
    async fn unbound_source_is_unknown_without_fallback() {
        let tool = BambooDiagnosticsTool::new(Arc::new(DiagnosticsSource::default()));
        assert_eq!(tool.snapshot("current").await["reason"], "runtime_unbound");
    }
}
