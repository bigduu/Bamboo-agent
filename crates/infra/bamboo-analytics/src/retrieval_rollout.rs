use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::HistoryToolMetrics;

pub const RETRIEVAL_ROLLOUT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum RetrievalRolloutError {
    #[error("failed to read rollout evidence: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid rollout evidence JSON on line {line}: {source}")]
    Json {
        line: usize,
        source: serde_json::Error,
    },
    #[error("invalid rollout evidence: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStrategy {
    Summary,
    RetrievalWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    SyntheticAggregate,
    SanitizedRealModelAggregate,
}

impl EvidenceSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::SyntheticAggregate => "synthetic aggregate",
            Self::SanitizedRealModelAggregate => "sanitized real-model aggregate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    HistoricalFact,
    Identifier,
    PriorDecision,
    ToolArgument,
    ToolResult,
}

impl EvidenceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::HistoricalFact => "historical_fact",
            Self::Identifier => "identifier",
            Self::PriorDecision => "prior_decision",
            Self::ToolArgument => "tool_argument",
            Self::ToolResult => "tool_result",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryClass {
    CjkTwoCharacter,
    CjkLonger,
    Latin,
    Mixed,
    PathPunctuation,
    Negative,
}

impl QueryClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::CjkTwoCharacter => "cjk_two_character",
            Self::CjkLonger => "cjk_longer",
            Self::Latin => "latin",
            Self::Mixed => "mixed",
            Self::PathPunctuation => "path_punctuation",
            Self::Negative => "negative",
        }
    }
}

/// One privacy-safe aggregate outcome from a paired offline run.
///
/// The schema intentionally has no fields for prompts, answers, queries,
/// messages, tool arguments/results, memory, provider payloads, or file paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalRolloutSample {
    pub schema_version: u32,
    pub corpus_id: String,
    pub pair_id: String,
    pub evidence_source: EvidenceSource,
    pub model: String,
    pub provider: String,
    pub config_id: String,
    pub strategy: RolloutStrategy,
    pub evidence_kind: EvidenceKind,
    pub query_class: QueryClass,
    pub expected_evidence_id: Option<String>,
    pub observed_evidence_id: Option<String>,
    pub task_completed: bool,
    pub provider_calls: u64,
    pub tool_rounds: u64,
    pub retrieval_calls: u64,
    pub retrieval_hits: u64,
    pub read_around_calls: u64,
    pub retrieval_truncations: u64,
    pub overflow_count: u64,
    pub user_restatements: u64,
    pub prompt_input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    pub cache_write_input_tokens: Option<u64>,
    pub latency_ms: u64,
    pub retrieval_latency_ms: Option<u64>,
    pub context_epochs: u64,
    pub restart_count: u64,
    pub provider_cache_boundary_count: u64,
}

impl RetrievalRolloutSample {
    fn exact_recovery(&self) -> bool {
        self.expected_evidence_id == self.observed_evidence_id
    }
}

pub struct RetrievalRolloutReport {
    samples: Vec<RetrievalRolloutSample>,
}

impl RetrievalRolloutReport {
    pub fn from_jsonl(path: &Path) -> Result<Self, RetrievalRolloutError> {
        let file = File::open(path)?;
        let mut samples = Vec::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line_number = index + 1;
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let sample =
                serde_json::from_str(&line).map_err(|source| RetrievalRolloutError::Json {
                    line: line_number,
                    source,
                })?;
            samples.push(sample);
        }
        validate_samples(&samples)?;
        Ok(Self { samples })
    }

    pub fn samples(&self) -> &[RetrievalRolloutSample] {
        &self.samples
    }

    pub fn render_markdown(
        &self,
        live_history_tool_metrics: Option<&HistoryToolMetrics>,
    ) -> String {
        let corpus_ids = values(&self.samples, |sample| sample.corpus_id.as_str());
        let models = values(&self.samples, |sample| sample.model.as_str());
        let providers = values(&self.samples, |sample| sample.provider.as_str());
        let configs = values(&self.samples, |sample| sample.config_id.as_str());
        let sources = self
            .samples
            .iter()
            .map(|sample| sample.evidence_source.as_str())
            .collect::<BTreeSet<_>>();
        let query_classes = self
            .samples
            .iter()
            .map(|sample| sample.query_class.as_str())
            .collect::<BTreeSet<_>>();
        let evidence_kinds = self
            .samples
            .iter()
            .map(|sample| sample.evidence_kind.as_str())
            .collect::<BTreeSet<_>>();
        let pair_count = self
            .samples
            .iter()
            .map(|sample| sample.pair_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let summary = aggregate_strategy(&self.samples, RolloutStrategy::Summary);
        let retrieval = aggregate_strategy(&self.samples, RolloutStrategy::RetrievalWindow);
        let representative_real_model = self
            .samples
            .iter()
            .all(|sample| sample.evidence_source == EvidenceSource::SanitizedRealModelAggregate);
        let promote = representative_real_model
            && retrieval.task_completed == retrieval.samples
            && retrieval.exact_recovery == retrieval.samples
            && retrieval.task_completed >= summary.task_completed
            && retrieval.exact_recovery >= summary.exact_recovery
            && retrieval.overflow_count <= summary.overflow_count
            && retrieval.user_restatements <= summary.user_restatements;
        let decision = if promote {
            "Promote `retrieval_window` to the default only after separately confirming these sanitized real-model aggregates are representative of the parent gate."
        } else {
            "Keep `summary` as the default and keep `retrieval_window` opt-in."
        };
        let rationale = if representative_real_model {
            "The checked evidence is real-model aggregate data, but the conservative task-quality and reliability gates were not all met."
        } else {
            "The checked evidence is synthetic aggregate coverage, not representative real-model task-quality evidence; it cannot justify a default promotion."
        };

        let mut report = String::new();
        report.push_str("# Retrieval-window rollout evidence v1\n\n");
        report.push_str("This report is generated deterministically from privacy-safe aggregate JSONL. It contains no prompts, answers, queries, messages, memory, tool arguments/results, file paths, or provider payloads.\n\n");
        report.push_str("## Evidence identity\n\n");
        report.push_str(&format!("- Schema: `{RETRIEVAL_ROLLOUT_SCHEMA_VERSION}`\n"));
        report.push_str(&format!("- Corpus: {}\n", code_list(&corpus_ids)));
        report.push_str(&format!("- Models: {}\n", code_list(&models)));
        report.push_str(&format!("- Providers: {}\n", code_list(&providers)));
        report.push_str(&format!("- Configs: {}\n", code_list(&configs)));
        report.push_str(&format!("- Sources: {}\n", plain_list(&sources)));
        report.push_str(&format!("- Paired scenario count: `{pair_count}`\n"));
        report.push_str(&format!(
            "- Aggregate sample count: `{}`\n",
            self.samples.len()
        ));
        report.push_str(&format!("- Query classes: {}\n", code_list(&query_classes)));
        report.push_str(&format!(
            "- Evidence kinds: {}\n\n",
            code_list(&evidence_kinds)
        ));

        report.push_str("## Deterministic search/index evidence\n\n");
        report.push_str("This is production-shaped index evidence from [Bamboo #1156](https://github.com/bigduu/Bamboo-agent/issues/1156) / PR #1157, not model/task-quality evidence.\n\n");
        report.push_str("- Migrated messages: `5,001`; schema-v5 rebuild: `42,936 us`.\n");
        report.push_str(
            "- Live database bytes: `1,511,424 -> 2,772,992`; sampled peak: `4,779,272`.\n",
        );
        report.push_str(
            "- 5,000-message Latin infix warm latency p50/p95/p99: `1,094 / 1,440 / 1,653 us`.\n",
        );
        report.push_str(
            "- Bounded two-character literal fallback p50/p95/p99: `4,342 / 4,699 / 4,972 us`.\n\n",
        );

        report.push_str("## Paired aggregate outcomes\n\n");
        report.push_str("| Metric | summary | retrieval_window |\n");
        report.push_str("|---|---:|---:|\n");
        append_strategy_rows(&mut report, &summary, &retrieval);
        report.push('\n');
        report.push_str(&format!(
            "Paired delta (`retrieval_window - summary`): provider/model calls `{:+}`, tool rounds `{:+}`.\n\n",
            signed_delta(retrieval.provider_calls, summary.provider_calls),
            signed_delta(retrieval.tool_rounds, summary.tool_rounds),
        ));

        report.push_str("## Existing `session_history_current` tool metrics\n\n");
        if let Some(metrics) = live_history_tool_metrics {
            report.push_str(&format!("- Calls: `{}`\n", metrics.calls));
            report.push_str(&format!(
                "- Succeeded / failed / incomplete: `{}` / `{}` / `{}`\n",
                metrics.succeeded, metrics.failed, metrics.incomplete
            ));
            report.push_str(&format!(
                "- Success rate: {}\n",
                percent(metrics.success_rate)
            ));
            report.push_str(&format!(
                "- Latency p50/p95/p99: `{}` / `{}` / `{}` ms\n\n",
                optional_u64(metrics.latency_ms_p50),
                optional_u64(metrics.latency_ms_p95),
                optional_u64(metrics.latency_ms_p99)
            ));
        } else {
            report.push_str("Live `metrics.db` observations: unavailable. This report does not treat unavailable calls, success, or latency as zero. The runtime continues to use the existing `tool_call_metrics` writer; no parallel token-log writer was added.\n\n");
        }

        report.push_str("## Default-strategy decision\n\n");
        report.push_str(&format!("**Decision: {decision}**\n\n"));
        report.push_str(rationale);
        report.push('\n');
        report
    }
}

pub fn render_retrieval_rollout_report(
    evidence_jsonl: &Path,
    live_history_tool_metrics: Option<&HistoryToolMetrics>,
) -> Result<String, RetrievalRolloutError> {
    Ok(RetrievalRolloutReport::from_jsonl(evidence_jsonl)?
        .render_markdown(live_history_tool_metrics))
}

fn validate_samples(samples: &[RetrievalRolloutSample]) -> Result<(), RetrievalRolloutError> {
    if samples.is_empty() {
        return Err(RetrievalRolloutError::Validation(
            "at least one paired sample is required".to_string(),
        ));
    }
    let mut pairs: BTreeMap<&str, Vec<&RetrievalRolloutSample>> = BTreeMap::new();
    for sample in samples {
        if sample.schema_version != RETRIEVAL_ROLLOUT_SCHEMA_VERSION {
            return Err(RetrievalRolloutError::Validation(format!(
                "unsupported schema version {}",
                sample.schema_version
            )));
        }
        for (name, value) in [
            ("corpus_id", sample.corpus_id.as_str()),
            ("pair_id", sample.pair_id.as_str()),
            ("model", sample.model.as_str()),
            ("provider", sample.provider.as_str()),
            ("config_id", sample.config_id.as_str()),
        ] {
            validate_public_identifier(name, value)?;
        }
        if let Some(value) = sample.expected_evidence_id.as_deref() {
            validate_public_identifier("expected_evidence_id", value)?;
        }
        if let Some(value) = sample.observed_evidence_id.as_deref() {
            validate_public_identifier("observed_evidence_id", value)?;
        }
        if sample.provider_calls == 0 || sample.context_epochs == 0 {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {} must record at least one provider call and context epoch",
                sample.pair_id
            )));
        }
        if sample.retrieval_hits > sample.retrieval_calls
            || sample.read_around_calls > sample.retrieval_calls
        {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {} has retrieval outcomes larger than retrieval calls",
                sample.pair_id
            )));
        }
        if sample.retrieval_calls == 0 && sample.retrieval_latency_ms.is_some() {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {} records retrieval latency without a retrieval call",
                sample.pair_id
            )));
        }
        if sample.retrieval_calls > 0 && sample.retrieval_latency_ms.is_none() {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {} must record retrieval latency when retrieval calls exist",
                sample.pair_id
            )));
        }
        pairs.entry(&sample.pair_id).or_default().push(sample);
    }

    for (pair_id, pair) in &pairs {
        if pair.len() != 2 {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {pair_id} must contain exactly two strategies"
            )));
        }
        let strategies = pair
            .iter()
            .map(|sample| sample.strategy)
            .collect::<BTreeSet<_>>();
        if strategies
            != BTreeSet::from([RolloutStrategy::Summary, RolloutStrategy::RetrievalWindow])
        {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {pair_id} must contain summary and retrieval_window"
            )));
        }
        let first = pair[0];
        let second = pair[1];
        if first.corpus_id != second.corpus_id
            || first.model != second.model
            || first.provider != second.provider
            || first.evidence_source != second.evidence_source
            || first.evidence_kind != second.evidence_kind
            || first.query_class != second.query_class
            || first.expected_evidence_id != second.expected_evidence_id
        {
            return Err(RetrievalRolloutError::Validation(format!(
                "pair {pair_id} changes a paired identity or expected outcome"
            )));
        }
    }

    require_coverage(
        "query class",
        samples.iter().map(|sample| sample.query_class).collect(),
        BTreeSet::from([
            QueryClass::CjkTwoCharacter,
            QueryClass::CjkLonger,
            QueryClass::Latin,
            QueryClass::Mixed,
            QueryClass::PathPunctuation,
            QueryClass::Negative,
        ]),
    )?;
    require_coverage(
        "evidence kind",
        samples.iter().map(|sample| sample.evidence_kind).collect(),
        BTreeSet::from([
            EvidenceKind::HistoricalFact,
            EvidenceKind::Identifier,
            EvidenceKind::PriorDecision,
            EvidenceKind::ToolArgument,
            EvidenceKind::ToolResult,
        ]),
    )?;
    if !samples.iter().any(|sample| sample.context_epochs > 1)
        || !samples.iter().any(|sample| sample.restart_count > 0)
        || !samples
            .iter()
            .any(|sample| sample.provider_cache_boundary_count > 0)
    {
        return Err(RetrievalRolloutError::Validation(
            "fixtures must cover repeated epochs, restart, and provider-cache boundaries"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_public_identifier(name: &str, value: &str) -> Result<(), RetrievalRolloutError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/' | b'@' | b'+')
        });
    if valid {
        Ok(())
    } else {
        Err(RetrievalRolloutError::Validation(format!(
            "{name} must be a bounded public identifier"
        )))
    }
}

fn require_coverage<T: Ord + std::fmt::Debug>(
    name: &str,
    actual: BTreeSet<T>,
    required: BTreeSet<T>,
) -> Result<(), RetrievalRolloutError> {
    if actual == required {
        Ok(())
    } else {
        Err(RetrievalRolloutError::Validation(format!(
            "incomplete {name} coverage: {actual:?}"
        )))
    }
}

#[derive(Default)]
struct StrategyAggregate {
    samples: u64,
    task_completed: u64,
    exact_recovery: u64,
    provider_calls: u64,
    tool_rounds: u64,
    retrieval_calls: u64,
    retrieval_hits: u64,
    read_around_calls: u64,
    retrieval_truncations: u64,
    overflow_count: u64,
    user_restatements: u64,
    prompt_input_tokens: u64,
    output_tokens: u64,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_write_input_tokens: Option<u64>,
    latency_ms_p50: Option<u64>,
    latency_ms_p95: Option<u64>,
    latency_ms_p99: Option<u64>,
    retrieval_latency_ms_p50: Option<u64>,
    retrieval_latency_ms_p95: Option<u64>,
    retrieval_latency_ms_p99: Option<u64>,
    context_epochs: u64,
    restart_count: u64,
    provider_cache_boundary_count: u64,
}

fn aggregate_strategy(
    samples: &[RetrievalRolloutSample],
    strategy: RolloutStrategy,
) -> StrategyAggregate {
    let selected = samples
        .iter()
        .filter(|sample| sample.strategy == strategy)
        .collect::<Vec<_>>();
    let mut latencies = selected
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    let mut retrieval_latencies = selected
        .iter()
        .filter_map(|sample| sample.retrieval_latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    retrieval_latencies.sort_unstable();
    let all_cache_available = selected.iter().all(|sample| {
        sample.cache_read_input_tokens.is_some() && sample.cache_creation_input_tokens.is_some()
    });
    let all_cache_write_available = selected
        .iter()
        .all(|sample| sample.cache_write_input_tokens.is_some());
    StrategyAggregate {
        samples: selected.len() as u64,
        task_completed: selected
            .iter()
            .filter(|sample| sample.task_completed)
            .count() as u64,
        exact_recovery: selected
            .iter()
            .filter(|sample| sample.exact_recovery())
            .count() as u64,
        provider_calls: selected.iter().map(|sample| sample.provider_calls).sum(),
        tool_rounds: selected.iter().map(|sample| sample.tool_rounds).sum(),
        retrieval_calls: selected.iter().map(|sample| sample.retrieval_calls).sum(),
        retrieval_hits: selected.iter().map(|sample| sample.retrieval_hits).sum(),
        read_around_calls: selected.iter().map(|sample| sample.read_around_calls).sum(),
        retrieval_truncations: selected
            .iter()
            .map(|sample| sample.retrieval_truncations)
            .sum(),
        overflow_count: selected.iter().map(|sample| sample.overflow_count).sum(),
        user_restatements: selected.iter().map(|sample| sample.user_restatements).sum(),
        prompt_input_tokens: selected
            .iter()
            .map(|sample| sample.prompt_input_tokens)
            .sum(),
        output_tokens: selected.iter().map(|sample| sample.output_tokens).sum(),
        cache_read_input_tokens: all_cache_available.then(|| {
            selected
                .iter()
                .filter_map(|sample| sample.cache_read_input_tokens)
                .sum()
        }),
        cache_creation_input_tokens: all_cache_available.then(|| {
            selected
                .iter()
                .filter_map(|sample| sample.cache_creation_input_tokens)
                .sum()
        }),
        cache_write_input_tokens: all_cache_write_available.then(|| {
            selected
                .iter()
                .filter_map(|sample| sample.cache_write_input_tokens)
                .sum()
        }),
        latency_ms_p50: nearest_rank(&latencies, 0.50),
        latency_ms_p95: nearest_rank(&latencies, 0.95),
        latency_ms_p99: nearest_rank(&latencies, 0.99),
        retrieval_latency_ms_p50: nearest_rank(&retrieval_latencies, 0.50),
        retrieval_latency_ms_p95: nearest_rank(&retrieval_latencies, 0.95),
        retrieval_latency_ms_p99: nearest_rank(&retrieval_latencies, 0.99),
        context_epochs: selected.iter().map(|sample| sample.context_epochs).sum(),
        restart_count: selected.iter().map(|sample| sample.restart_count).sum(),
        provider_cache_boundary_count: selected
            .iter()
            .map(|sample| sample.provider_cache_boundary_count)
            .sum(),
    }
}

fn append_strategy_rows(
    report: &mut String,
    summary: &StrategyAggregate,
    retrieval: &StrategyAggregate,
) {
    let rows = [
        (
            "Samples",
            summary.samples.to_string(),
            retrieval.samples.to_string(),
        ),
        (
            "Task completion",
            ratio(summary.task_completed, summary.samples),
            ratio(retrieval.task_completed, retrieval.samples),
        ),
        (
            "Exact recovery",
            ratio(summary.exact_recovery, summary.samples),
            ratio(retrieval.exact_recovery, retrieval.samples),
        ),
        (
            "Provider/model calls",
            summary.provider_calls.to_string(),
            retrieval.provider_calls.to_string(),
        ),
        (
            "Tool rounds",
            summary.tool_rounds.to_string(),
            retrieval.tool_rounds.to_string(),
        ),
        (
            "Retrieval calls / hits",
            format!("{} / {}", summary.retrieval_calls, summary.retrieval_hits),
            format!(
                "{} / {}",
                retrieval.retrieval_calls, retrieval.retrieval_hits
            ),
        ),
        (
            "Retrieval hit rate",
            percent_rate(summary.retrieval_hits, summary.retrieval_calls),
            percent_rate(retrieval.retrieval_hits, retrieval.retrieval_calls),
        ),
        (
            "Read-around calls",
            summary.read_around_calls.to_string(),
            retrieval.read_around_calls.to_string(),
        ),
        (
            "Retrieval truncations",
            summary.retrieval_truncations.to_string(),
            retrieval.retrieval_truncations.to_string(),
        ),
        (
            "Overflow signals",
            summary.overflow_count.to_string(),
            retrieval.overflow_count.to_string(),
        ),
        (
            "User restatements",
            summary.user_restatements.to_string(),
            retrieval.user_restatements.to_string(),
        ),
        (
            "Prompt input tokens",
            summary.prompt_input_tokens.to_string(),
            retrieval.prompt_input_tokens.to_string(),
        ),
        (
            "Output tokens",
            summary.output_tokens.to_string(),
            retrieval.output_tokens.to_string(),
        ),
        (
            "Cache read tokens",
            optional_u64(summary.cache_read_input_tokens),
            optional_u64(retrieval.cache_read_input_tokens),
        ),
        (
            "Cache creation tokens",
            optional_u64(summary.cache_creation_input_tokens),
            optional_u64(retrieval.cache_creation_input_tokens),
        ),
        (
            "Cache write tokens",
            optional_u64(summary.cache_write_input_tokens),
            optional_u64(retrieval.cache_write_input_tokens),
        ),
        (
            "Cached fraction",
            cache_fraction(summary),
            cache_fraction(retrieval),
        ),
        (
            "Task latency p50/p95/p99 ms",
            percentile_triplet(
                summary.latency_ms_p50,
                summary.latency_ms_p95,
                summary.latency_ms_p99,
            ),
            percentile_triplet(
                retrieval.latency_ms_p50,
                retrieval.latency_ms_p95,
                retrieval.latency_ms_p99,
            ),
        ),
        (
            "Retrieval latency p50/p95/p99 ms",
            percentile_triplet(
                summary.retrieval_latency_ms_p50,
                summary.retrieval_latency_ms_p95,
                summary.retrieval_latency_ms_p99,
            ),
            percentile_triplet(
                retrieval.retrieval_latency_ms_p50,
                retrieval.retrieval_latency_ms_p95,
                retrieval.retrieval_latency_ms_p99,
            ),
        ),
        (
            "Context epochs / restarts / cache boundaries",
            format!(
                "{} / {} / {}",
                summary.context_epochs,
                summary.restart_count,
                summary.provider_cache_boundary_count
            ),
            format!(
                "{} / {} / {}",
                retrieval.context_epochs,
                retrieval.restart_count,
                retrieval.provider_cache_boundary_count
            ),
        ),
    ];
    for (metric, summary_value, retrieval_value) in rows {
        report.push_str(&format!(
            "| {metric} | {summary_value} | {retrieval_value} |\n"
        ));
    }
}

fn values<'a>(
    samples: &'a [RetrievalRolloutSample],
    value: impl Fn(&'a RetrievalRolloutSample) -> &'a str,
) -> BTreeSet<&'a str> {
    samples.iter().map(value).collect()
}

fn code_list<T: AsRef<str> + Ord>(values: &BTreeSet<T>) -> String {
    values
        .iter()
        .map(|value| format!("`{}`", value.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn plain_list<T: AsRef<str> + Ord>(values: &BTreeSet<T>) -> String {
    values
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

fn ratio(value: u64, total: u64) -> String {
    if total == 0 {
        "unavailable".to_string()
    } else {
        format!(
            "{value}/{total} ({:.1}%)",
            value as f64 / total as f64 * 100.0
        )
    }
}

fn percent_rate(value: u64, total: u64) -> String {
    if total == 0 {
        "unavailable".to_string()
    } else {
        format!("{:.1}%", value as f64 / total as f64 * 100.0)
    }
}

fn percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "unavailable".to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn cache_fraction(aggregate: &StrategyAggregate) -> String {
    match aggregate.cache_read_input_tokens {
        Some(read) => {
            if aggregate.prompt_input_tokens == 0 {
                "unavailable".to_string()
            } else {
                format!(
                    "{:.1}%",
                    read as f64 / aggregate.prompt_input_tokens as f64 * 100.0
                )
            }
        }
        _ => "unavailable".to_string(),
    }
}

fn percentile_triplet(p50: Option<u64>, p95: Option<u64>, p99: Option<u64>) -> String {
    match (p50, p95, p99) {
        (Some(p50), Some(p95), Some(p99)) => format!("{p50} / {p95} / {p99}"),
        _ => "unavailable".to_string(),
    }
}

fn nearest_rank(sorted: &[u64], percentile: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.saturating_sub(1)).copied()
}

fn signed_delta(value: u64, baseline: u64) -> i128 {
    i128::from(value) - i128::from(baseline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_deterministic() {
        let values = [10, 20, 30, 40, 50];
        assert_eq!(nearest_rank(&values, 0.50), Some(30));
        assert_eq!(nearest_rank(&values, 0.95), Some(50));
        assert_eq!(nearest_rank(&values, 0.99), Some(50));
        assert_eq!(nearest_rank(&[], 0.50), None);
    }

    #[test]
    fn checked_fixture_is_valid_and_report_is_reproducible() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let fixture = manifest.join("fixtures/retrieval-rollout-v1.jsonl");
        let expected = std::fs::read_to_string(
            manifest.join("../../../docs/evaluations/retrieval-window-rollout-v1.md"),
        )
        .expect("checked report");
        let actual = render_retrieval_rollout_report(&fixture, None).expect("render report");
        assert_eq!(actual, expected);
    }

    #[test]
    fn aggregate_fixture_covers_exact_recovery_and_missing_live_metrics() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let report = RetrievalRolloutReport::from_jsonl(
            &manifest.join("fixtures/retrieval-rollout-v1.jsonl"),
        )
        .expect("valid aggregate fixture");
        assert_eq!(report.samples().len(), 12);
        let markdown = report.render_markdown(None);
        assert!(markdown.contains("Live `metrics.db` observations: unavailable"));
        assert!(markdown.contains("Keep `summary` as the default"));
        assert!(!markdown.contains("expected_evidence_id"));
        assert!(!markdown.contains("observed_evidence_id"));
    }
}
