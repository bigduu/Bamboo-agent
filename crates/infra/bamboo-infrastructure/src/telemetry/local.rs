//! A bounded, process-local projection of Bamboo's own finished OTel spans.
//! Only explicitly allowlisted attributes are copied out of the SDK span.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bamboo_domain::ModelTokenUsage;
use chrono::{DateTime, Utc};
use opentelemetry::trace::SpanId;
use opentelemetry::{Context, Value};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use serde::Serialize;

use super::{
    session_reference, valid_identifier, valid_session_reference, INSTRUMENTATION_NAME,
    LOCAL_SESSION_REFERENCE,
};

pub const MAX_LOCAL_SPANS: usize = 2048;
const MAX_EVICTION_SCOPES: usize = 2048;

#[derive(Clone, Debug, Serialize)]
pub struct LocalSpan {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub kind: &'static str,
    pub name: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub status: &'static str,
    pub error_type: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub run_id: Option<String>,
    pub call_id: Option<String>,
    pub usage: Option<ModelTokenUsage>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub status: &'static str,
    pub span_count: usize,
    pub partial: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct TraceDetail {
    pub trace_id: String,
    pub partial: bool,
    pub spans: Vec<LocalSpan>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Retention {
    pub max_spans: usize,
    /// `None` means the bounded eviction counter for this lifetime was retired.
    pub evicted_spans: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TracePage {
    pub schema_version: u8,
    pub session_id: String,
    pub collection_state: &'static str,
    /// Local inspection is sampled/best-effort and is never a billing ledger.
    pub coverage: &'static str,
    pub traces: Vec<TraceSummary>,
    pub next_cursor: Option<String>,
    pub retention: Retention,
}

#[derive(Clone, Debug)]
struct Record {
    scope: Option<String>,
    session_id: String,
    birth: DateTime<Utc>,
    trace_id: String,
    trace_sequence: u64,
    span: LocalSpan,
}

#[derive(Default, Debug)]
struct Inner {
    records: VecDeque<Record>,
    next_sequence: u64,
    evictions: HashMap<(Option<String>, String, DateTime<Utc>), u64>,
    retired_eviction_scopes: bool,
}

#[derive(Default, Debug)]
pub struct LocalTraceStore {
    enabled: AtomicBool,
    dropped: AtomicBool,
    inner: Mutex<Inner>,
}

static STORE: OnceLock<Arc<LocalTraceStore>> = OnceLock::new();
static SCOPE_HASHERS: OnceLock<(
    std::collections::hash_map::RandomState,
    std::collections::hash_map::RandomState,
)> = OnceLock::new();

/// Opaque process-local data-authority key. The path itself is not retained or
/// serialized; two process-seeded hashers produce the key.
pub fn scope_for_path(path: &Path) -> String {
    let (first, second) = SCOPE_HASHERS.get_or_init(|| (Default::default(), Default::default()));
    format!(
        "{:016x}{:016x}",
        first.hash_one(path),
        second.hash_one(path)
    )
}

pub fn store() -> Arc<LocalTraceStore> {
    STORE
        .get_or_init(|| Arc::new(LocalTraceStore::default()))
        .clone()
}

impl LocalTraceStore {
    pub(super) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
        if !enabled {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *inner = Inner::default();
            self.dropped.store(false, Ordering::Release);
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    fn insert(&self, data: SpanData) {
        if !self.enabled() || data.instrumentation_scope.name() != INSTRUMENTATION_NAME {
            return;
        }
        let Some(mut record) = project(data) else {
            return;
        };
        // SDK calls on_end synchronously. Never wait behind an API read.
        let Ok(mut inner) = self.inner.try_lock() else {
            self.dropped.store(true, Ordering::Release);
            return;
        };
        if !self.enabled() {
            return;
        }
        let prior_sequence = inner
            .records
            .iter()
            .find(|existing| {
                existing.scope == record.scope
                    && existing.session_id == record.session_id
                    && existing.birth == record.birth
                    && existing.trace_id == record.trace_id
            })
            .map(|existing| existing.trace_sequence);
        record.trace_sequence = if let Some(sequence) = prior_sequence {
            sequence
        } else {
            inner.next_sequence = inner.next_sequence.saturating_add(1);
            inner.next_sequence
        };
        // A span processor is allowed to receive the same completed span twice.
        if inner.records.iter().any(|existing| {
            existing.scope == record.scope
                && existing.session_id == record.session_id
                && existing.birth == record.birth
                && existing.trace_id == record.trace_id
                && existing.span.span_id == record.span.span_id
        }) {
            return;
        }
        if inner.records.len() == MAX_LOCAL_SPANS {
            if let Some(evicted) = inner.records.pop_front() {
                let key = (evicted.scope, evicted.session_id, evicted.birth);
                if !inner.evictions.contains_key(&key)
                    && inner.evictions.len() >= MAX_EVICTION_SCOPES
                {
                    if let Some(old) = inner.evictions.keys().next().cloned() {
                        inner.evictions.remove(&old);
                        inner.retired_eviction_scopes = true;
                    }
                }
                let count = inner.evictions.entry(key).or_default();
                *count = count.saturating_add(1);
            }
        }
        inner.records.push_back(record);
    }

    pub fn page(
        &self,
        session_id: &str,
        birth: DateTime<Utc>,
        limit: usize,
        cursor: Option<u64>,
    ) -> TracePage {
        self.page_scoped(None, session_id, birth, limit, cursor)
    }

    pub fn page_scoped(
        &self,
        scope: Option<&str>,
        session_id: &str,
        birth: DateTime<Utc>,
        limit: usize,
        cursor: Option<u64>,
    ) -> TracePage {
        let limit = limit.clamp(1, MAX_LOCAL_SPANS);
        let reference = session_reference(session_id).unwrap_or_default();
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut groups: HashMap<&str, Vec<&Record>> = HashMap::new();
        for record in inner.records.iter().filter(|r| {
            r.scope.as_deref() == scope && r.session_id == reference && r.birth == birth
        }) {
            groups.entry(&record.trace_id).or_default().push(record);
        }
        let mut groups: Vec<_> = groups.into_iter().collect();
        groups.sort_by_key(|(_, records)| std::cmp::Reverse(records[0].trace_sequence));
        if let Some(cursor) = cursor {
            groups.retain(|(_, records)| records[0].trace_sequence < cursor);
        }
        let has_more = groups.len() > limit;
        groups.truncate(limit);
        let next_cursor = has_more.then(|| groups.last().unwrap().1[0].trace_sequence.to_string());
        let key = (scope.map(str::to_owned), reference, birth);
        let evicted_spans = inner
            .evictions
            .get(&key)
            .copied()
            .or_else(|| (!inner.retired_eviction_scopes).then_some(0));
        let incomplete = self.dropped.load(Ordering::Acquire) || evicted_spans != Some(0);
        let traces = groups
            .into_iter()
            .map(|(trace_id, records)| {
                let mut value = summary(trace_id, &records);
                value.partial |= incomplete;
                value
            })
            .collect();
        TracePage {
            schema_version: 1,
            session_id: session_id.to_owned(),
            collection_state: if self.enabled() {
                "enabled"
            } else {
                "disabled"
            },
            coverage: "best_effort",
            traces,
            next_cursor,
            retention: Retention {
                max_spans: MAX_LOCAL_SPANS,
                evicted_spans,
            },
        }
    }

    pub fn detail(
        &self,
        session_id: &str,
        birth: DateTime<Utc>,
        trace_id: &str,
    ) -> Option<TraceDetail> {
        self.detail_scoped(None, session_id, birth, trace_id)
    }

    pub fn detail_scoped(
        &self,
        scope: Option<&str>,
        session_id: &str,
        birth: DateTime<Utc>,
        trace_id: &str,
    ) -> Option<TraceDetail> {
        let reference = session_reference(session_id)?;
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let records: Vec<_> = inner
            .records
            .iter()
            .filter(|r| {
                r.scope.as_deref() == scope
                    && r.session_id == reference
                    && r.birth == birth
                    && r.trace_id == trace_id
            })
            .collect();
        if records.is_empty() {
            return None;
        }
        let key = (scope.map(str::to_owned), reference, birth);
        let evicted = inner
            .evictions
            .get(&key)
            .copied()
            .or_else(|| (!inner.retired_eviction_scopes).then_some(0));
        let partial =
            is_partial(&records) || self.dropped.load(Ordering::Acquire) || evicted != Some(0);
        let by_id: HashMap<&str, &LocalSpan> = records
            .iter()
            .map(|record| (record.span.span_id.as_str(), &record.span))
            .collect();
        let mut spans: Vec<_> = records
            .iter()
            .map(|r| {
                let mut span = r.span.clone();
                let mut parent = span.parent_span_id.as_deref();
                let mut visited = HashSet::new();
                while span.run_id.is_none() {
                    let Some(id) = parent else { break };
                    if !visited.insert(id) {
                        break;
                    }
                    let Some(ancestor) = by_id.get(id) else { break };
                    span.run_id.clone_from(&ancestor.run_id);
                    parent = ancestor.parent_span_id.as_deref();
                }
                span
            })
            .collect();
        spans.sort_by_key(|span| span.started_at);
        Some(TraceDetail {
            trace_id: trace_id.to_owned(),
            partial,
            spans,
        })
    }
}

fn is_partial(records: &[&Record]) -> bool {
    let ids: HashSet<&str> = records
        .iter()
        .map(|record| record.span.span_id.as_str())
        .collect();
    records.iter().any(|record| {
        record
            .span
            .parent_span_id
            .as_deref()
            .is_some_and(|parent| !ids.contains(parent))
    })
}

fn summary(trace_id: &str, records: &[&Record]) -> TraceSummary {
    let started_at = records.iter().map(|r| r.span.started_at).min().unwrap();
    let ended_at = records.iter().map(|r| r.span.ended_at).max().unwrap();
    let status = if records.iter().any(|r| r.span.status == "error") {
        "error"
    } else if records.iter().any(|r| r.span.status == "cancelled") {
        "cancelled"
    } else {
        "success"
    };
    TraceSummary {
        trace_id: trace_id.to_owned(),
        started_at,
        ended_at,
        status,
        span_count: records.len(),
        partial: is_partial(records),
    }
}

fn string_attr<'a>(data: &'a SpanData, key: &str) -> Option<&'a str> {
    data.attributes
        .iter()
        .find(|entry| entry.key.as_str() == key)
        .and_then(|entry| match &entry.value {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        })
}
fn identifier(data: &SpanData, key: &str) -> Option<String> {
    string_attr(data, key)
        .filter(|value| valid_identifier(value))
        .map(str::to_owned)
}

fn session_identifier(data: &SpanData) -> Option<String> {
    identifier(data, "gen_ai.conversation.id").or_else(|| {
        string_attr(data, LOCAL_SESSION_REFERENCE)
            .filter(|value| value.starts_with('~') && valid_session_reference(value))
            .map(str::to_owned)
    })
}
fn count(data: &SpanData, key: &str) -> Option<u64> {
    data.attributes
        .iter()
        .find(|entry| entry.key.as_str() == key)
        .and_then(|entry| match entry.value {
            Value::I64(value) => u64::try_from(value).ok(),
            _ => None,
        })
}

fn project(data: SpanData) -> Option<Record> {
    let session_id = session_identifier(&data)?;
    let birth = DateTime::parse_from_rfc3339(string_attr(&data, "bamboo.session.birth")?)
        .ok()?
        .with_timezone(&Utc);
    let kind = match string_attr(&data, "gen_ai.operation.name")? {
        "invoke_agent" => "agent",
        "chat" => "llm",
        "execute_tool" => "tool",
        _ => return None,
    };
    let name = match kind {
        "agent" => "invoke_agent".to_owned(),
        "llm" => "chat".to_owned(),
        "tool" => {
            identifier(&data, "gen_ai.tool.name").unwrap_or_else(|| "execute_tool".to_owned())
        }
        _ => unreachable!(),
    };
    let status = match string_attr(&data, "bamboo.outcome") {
        Some("success") => "success",
        Some("cancelled") => "cancelled",
        _ => "error",
    };
    let error_type = string_attr(&data, "error.type")
        .filter(|value| {
            matches!(
                *value,
                "cancelled"
                    | "provider_error"
                    | "tool_error"
                    | "timeout"
                    | "context_overflow"
                    | "empty_response"
                    | "execution_error"
                    | "panic"
            )
        })
        .map(str::to_owned);
    let usage = ModelTokenUsage {
        input_tokens: count(&data, "gen_ai.usage.input_tokens"),
        output_tokens: count(&data, "gen_ai.usage.output_tokens"),
        reasoning_tokens: count(&data, "bamboo.usage.reasoning_tokens"),
        cache_read_input_tokens: count(&data, "bamboo.usage.cache_read_input_tokens"),
        cache_creation_input_tokens: count(&data, "bamboo.usage.cache_creation_input_tokens"),
        cache_write_input_tokens: count(&data, "bamboo.usage.cache_write_input_tokens"),
    };
    let usage = (usage != ModelTokenUsage::default()).then_some(usage);
    let provider = string_attr(&data, "gen_ai.provider.name")
        .filter(|value| {
            matches!(
                *value,
                "openai" | "anthropic" | "gcp.gen_ai" | "github.copilot"
            )
        })
        .map(str::to_owned);
    Some(Record {
        scope: identifier(&data, "bamboo.local.scope"),
        session_id,
        birth,
        trace_id: data.span_context.trace_id().to_string(),
        trace_sequence: 0,
        span: LocalSpan {
            span_id: data.span_context.span_id().to_string(),
            parent_span_id: (data.parent_span_id != SpanId::INVALID)
                .then(|| data.parent_span_id.to_string()),
            kind,
            name,
            started_at: data.start_time.into(),
            ended_at: data.end_time.into(),
            status,
            error_type,
            model: identifier(&data, "gen_ai.request.model"),
            provider,
            run_id: identifier(&data, "bamboo.run.id"),
            call_id: identifier(&data, "bamboo.model_call.id"),
            usage,
        },
    })
}

#[derive(Debug)]
pub(super) struct LocalProcessor(pub Arc<LocalTraceStore>);
impl SpanProcessor for LocalProcessor {
    fn on_start(&self, _span: &mut Span, _context: &Context) {}
    fn on_end(&self, span: SpanData) {
        self.0.insert(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{test_support::LocalTraceTest, Observation, Outcome};

    #[tokio::test]
    async fn real_spans_preserve_presence_parentage_and_exact_session_lifetime() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        trace
            .scope(async {
                let root = Observation::agent("local-session", "local-session")
                    .bind_session_lifetime("local-session", birth);
                root.scope(async {
                    crate::telemetry::record_execution_identity("execution-1", "run-1");
                    tracing::warn!(
                        prompt = "PRIVATE_PROMPT_SENTINEL",
                        "ordinary log is not a span event"
                    );
                    let llm = Observation::llm(Some("openai"), "synthetic-model", "local-session");
                    llm.model_call_id("call-1");
                    llm.model_usage(&ModelTokenUsage {
                        input_tokens: Some(100),
                        output_tokens: Some(0),
                        reasoning_tokens: None,
                        cache_read_input_tokens: Some(0),
                        cache_creation_input_tokens: None,
                        cache_write_input_tokens: Some(12),
                    });
                    llm.finish(Outcome::Success);
                })
                .await;
                root.finish(Outcome::Success);
            })
            .await;
        let page = trace.store.page("local-session", birth, 50, None);
        assert_eq!(page.traces.len(), 1);
        assert!(!page.traces[0].partial);
        assert_eq!(page.traces[0].span_count, 2);
        assert!(trace
            .store
            .page("other-session", birth, 50, None)
            .traces
            .is_empty());
        assert!(trace
            .store
            .page(
                "local-session",
                birth + chrono::Duration::seconds(1),
                50,
                None
            )
            .traces
            .is_empty());
        let detail = trace
            .store
            .detail("local-session", birth, &page.traces[0].trace_id)
            .unwrap();
        let root = detail
            .spans
            .iter()
            .find(|span| span.kind == "agent")
            .unwrap();
        let llm = detail.spans.iter().find(|span| span.kind == "llm").unwrap();
        assert_eq!(llm.parent_span_id.as_deref(), Some(root.span_id.as_str()));
        assert_eq!(llm.run_id.as_deref(), Some("run-1"));
        assert_eq!(llm.call_id.as_deref(), Some("call-1"));
        let usage = llm.usage.as_ref().unwrap();
        assert_eq!(usage.output_tokens, Some(0));
        assert_eq!(usage.cache_read_input_tokens, Some(0));
        assert_eq!(usage.cache_creation_input_tokens, None);
        assert_eq!(usage.cache_write_input_tokens, Some(12));
        assert!(!serde_json::to_string(&detail)
            .unwrap()
            .contains("PRIVATE_PROMPT_SENTINEL"));
    }

    #[tokio::test]
    async fn non_ascii_and_spaced_session_ids_keep_scoped_trace_isolation() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        let scope_path = std::path::Path::new("/synthetic-trace-scope");
        let scope = scope_for_path(scope_path);
        let other_scope = scope_for_path(std::path::Path::new("/synthetic-other-scope"));
        let ids = ["用户 会话", "用户 会话二"];
        trace
            .scope(async {
                for id in ids {
                    let root = Observation::agent(id, id)
                        .bind_session_lifetime(id, birth)
                        .bind_local_scope(scope_path);
                    root.scope(async {
                        crate::telemetry::record_execution_identity("execution-1", "run-1");
                        Observation::llm(Some("openai"), "synthetic-model", id)
                            .finish(Outcome::Success);
                    })
                    .await;
                    root.finish(Outcome::Success);
                }
            })
            .await;
        let first = trace
            .store
            .page_scoped(Some(&scope), ids[0], birth, 10, None);
        let second = trace
            .store
            .page_scoped(Some(&scope), ids[1], birth, 10, None);
        assert_eq!(first.session_id, ids[0]);
        assert_eq!(first.traces.len(), 1);
        assert_eq!(second.traces.len(), 1);
        assert_ne!(first.traces[0].trace_id, second.traces[0].trace_id);
        assert!(trace
            .store
            .detail_scoped(Some(&scope), ids[1], birth, &first.traces[0].trace_id)
            .is_none());
        assert!(trace
            .store
            .page_scoped(Some(&other_scope), ids[0], birth, 10, None)
            .traces
            .is_empty());
        let detail = trace
            .store
            .detail_scoped(Some(&scope), ids[0], birth, &first.traces[0].trace_id)
            .unwrap();
        assert_eq!(detail.spans.len(), 2);
        assert!(detail
            .spans
            .iter()
            .any(|span| span.run_id.as_deref() == Some("run-1")));
        assert!(!serde_json::to_string(&detail).unwrap().contains(ids[0]));
    }

    #[tokio::test]
    async fn pagination_and_retention_are_bounded_to_the_requested_lifetime() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        trace
            .scope(async {
                for _ in 0..(MAX_LOCAL_SPANS + 2) {
                    Observation::agent("retention-session", "retention-session")
                        .bind_session_lifetime("retention-session", birth)
                        .finish(Outcome::Success);
                }
            })
            .await;
        let first = trace.store.page("retention-session", birth, 1, None);
        assert_eq!(first.traces.len(), 1);
        assert_eq!(first.retention.evicted_spans, Some(2));
        let cursor = first.next_cursor.unwrap().parse().unwrap();
        let second = trace
            .store
            .page("retention-session", birth, 1, Some(cursor));
        assert_ne!(first.traces[0].trace_id, second.traces[0].trace_id);
        assert_eq!(
            trace
                .store
                .page("another-session", birth, 1, None)
                .retention
                .evicted_spans,
            Some(0)
        );
    }

    #[tokio::test]
    async fn run_id_is_inherited_only_from_the_actual_ancestor() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        trace
            .scope(async {
                let outer = Observation::agent("nested-session", "nested-session")
                    .bind_session_lifetime("nested-session", birth);
                outer
                    .scope(async {
                        crate::telemetry::record_execution_identity("execution-outer", "run-outer");
                        let inner = Observation::agent("nested-session", "nested-session")
                            .bind_session_lifetime("nested-session", birth);
                        inner
                            .scope(async {
                                crate::telemetry::record_execution_identity(
                                    "execution-inner",
                                    "run-inner",
                                );
                                Observation::llm(Some("openai"), "inner-model", "nested-session")
                                    .finish(Outcome::Success);
                            })
                            .await;
                        inner.finish(Outcome::Success);
                        Observation::llm(Some("openai"), "outer-model", "nested-session")
                            .finish(Outcome::Success);
                    })
                    .await;
                outer.finish(Outcome::Success);
            })
            .await;
        let page = trace.store.page("nested-session", birth, 10, None);
        let detail = trace
            .store
            .detail("nested-session", birth, &page.traces[0].trace_id)
            .unwrap();
        for (model, expected) in [("inner-model", "run-inner"), ("outer-model", "run-outer")] {
            let span = detail
                .spans
                .iter()
                .find(|span| span.model.as_deref() == Some(model))
                .unwrap();
            assert_eq!(span.run_id.as_deref(), Some(expected));
        }
    }

    #[tokio::test]
    async fn equal_session_lifetimes_in_distinct_data_scopes_do_not_mix() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        let first = Path::new("/private-scope-one");
        let second = Path::new("/private-scope-two");
        trace
            .scope(async {
                for path in [first, second] {
                    Observation::agent("same-session", "same-session")
                        .bind_session_lifetime("same-session", birth)
                        .bind_local_scope(path)
                        .finish(Outcome::Success);
                }
            })
            .await;
        let first_key = scope_for_path(first);
        let second_key = scope_for_path(second);
        let first_page = trace
            .store
            .page_scoped(Some(&first_key), "same-session", birth, 10, None);
        let second_page =
            trace
                .store
                .page_scoped(Some(&second_key), "same-session", birth, 10, None);
        assert_eq!(first_page.traces.len(), 1);
        assert_eq!(second_page.traces.len(), 1);
        assert_ne!(
            first_page.traces[0].trace_id,
            second_page.traces[0].trace_id
        );
        assert!(trace
            .store
            .detail_scoped(
                Some(&second_key),
                "same-session",
                birth,
                &first_page.traces[0].trace_id
            )
            .is_none());
        assert!(!serde_json::to_string(&first_page)
            .unwrap()
            .contains("private-scope"));
    }

    #[tokio::test]
    async fn nested_agent_without_its_own_scope_cannot_inherit_parent_authority() {
        let trace = LocalTraceTest::isolated();
        let birth = Utc::now();
        let path = Path::new("/parent-authority");
        trace
            .scope(async {
                let outer = Observation::agent("same-session", "same-session")
                    .bind_session_lifetime("same-session", birth)
                    .bind_local_scope(path);
                outer
                    .scope(async {
                        Observation::agent("same-session", "same-session")
                            .bind_session_lifetime("same-session", birth)
                            .finish(Outcome::Success);
                    })
                    .await;
                outer.finish(Outcome::Success);
            })
            .await;
        let scoped =
            trace
                .store
                .page_scoped(Some(&scope_for_path(path)), "same-session", birth, 10, None);
        assert_eq!(scoped.traces.len(), 1);
        assert_eq!(scoped.traces[0].span_count, 1);
        let unscoped = trace.store.page("same-session", birth, 10, None);
        assert_eq!(unscoped.traces.len(), 1);
        assert_eq!(unscoped.traces[0].span_count, 1);
        assert!(unscoped.traces[0].partial);
    }
}
