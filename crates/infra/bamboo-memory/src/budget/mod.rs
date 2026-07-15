//! Token budget management — re-exported from bamboo-compression, plus
//! granularity-aware prefix/suffix segmentation for durable-memory recall
//! (issue #61 phase 2, "Prefix Cache Friendly" constraint).

pub use bamboo_compression::{
    active_messages_for_budget, apply_compression_plan, build_compression_plan_with_summary,
    build_forced_compression_plan_with_summary, build_summary_prompt, compression_summary_message,
    context_window_usage_percent, estimate_context_compression_exposure,
    normalized_trigger_percent, summary_source_messages, CompressionPlan, CompressionPlanError,
    ContextCompressionExposure,
};
pub use bamboo_compression::{
    create_budget_for_model, prepare_hybrid_context, BudgetError, BudgetStrategy,
    HeuristicSummarizer, HeuristicTokenCounter, MessageSegmenter, ModelLimitsRegistry,
    PreparedContext, Summarizer, SummaryManager, SummaryTrigger, TiktokenTokenCounter, TokenBudget,
    TokenCounter, TokenUsageBreakdown,
};

use crate::memory_store::TemporalGranularity;

/// One memory's already-rendered contribution to a recalled-memory prompt section,
/// tagged with the temporal granularity that decides which side of the prefix/
/// suffix split it lands on (issue #61). `rendered` is whatever markdown/text form
/// the caller uses for a single recalled memory (title, summary, freshness note,
/// ...) — this module only cares about the granularity tag and the resulting char
/// cost, not the rendering format.
///
/// Callers are expected to pass items in priority order (e.g. recall's relevance
/// ranking — see `recall::sort_recall_candidates`); this module preserves that
/// relative order within each of the two output segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GranularityBudgetItem {
    pub id: String,
    pub granularity: Option<TemporalGranularity>,
    pub rendered: String,
}

impl GranularityBudgetItem {
    pub fn new(
        id: impl Into<String>,
        granularity: Option<TemporalGranularity>,
        rendered: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            granularity,
            rendered: rendered.into(),
        }
    }
}

/// Result of [`segment_by_granularity_budget`]: the PREFIX (coarse, cache-stable)
/// and SUFFIX (fine, volatile) text blocks, plus which candidate ids were dropped
/// from each side when the budget ran out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GranularityBudgetSegments {
    pub prefix: String,
    pub suffix: String,
    pub prefix_dropped_ids: Vec<String>,
    pub suffix_dropped_ids: Vec<String>,
}

impl GranularityBudgetSegments {
    /// Prefix followed by suffix — coarse-before-fine render order, per the
    /// issue's Prefix Cache Friendly constraint.
    pub fn combined(&self) -> String {
        format!("{}{}", self.prefix, self.suffix)
    }
}

/// Split already-selected, already-ordered recall candidates into a cache-stable
/// PREFIX segment (coarse granularity: `None`/month/quarter/year) and a volatile
/// SUFFIX segment (fine granularity: week/day), so the prefix can sit at a stable
/// position in the LLM prompt and benefit from provider-side prompt-prefix caching
/// even while the suffix churns turn to turn.
///
/// # Why the prefix gets first claim on the budget, not a shared running total
///
/// The prefix region only stays prompt-cache stable if its rendered bytes never
/// change in response to something happening in the suffix region. If this
/// function spent one shared running budget over the combined, priority-ordered
/// list (the way a single flat render loop naturally would), adding or growing a
/// `day`-granularity memory earlier in that combined order could push a later
/// `year`-granularity memory out of the budget — silently changing the prefix in
/// response to fine-grained churn, exactly what the issue's constraint forbids.
///
/// Instead, the two groups get INDEPENDENT budgets: the coarse (prefix) group is
/// filled first, using only its own items' sizes and the total budget; whatever
/// budget remains afterward goes to the fine (suffix) group. Coarse inclusion is
/// therefore a pure function of the coarse items and the total budget — it can
/// never be perturbed by suffix content, which is exactly the "day-level churn
/// cannot invalidate the prefix region" property this function guarantees (see
/// `changing_or_adding_a_day_memory_does_not_change_the_rendered_prefix_segment`).
///
/// # Interaction with the recall ordering tie-break (Phase 1, `recall::sort_recall_candidates`)
///
/// Recall selection ranks candidates by RELEVANCE first and only breaks exact ties
/// by granularity (see recall.rs's
/// `higher_score_still_wins_over_cache_stable_granularity` test) — a highly
/// relevant `day` memory legitimately outranks a barely relevant `year` one when
/// deciding what makes the (small) recalled shortlist. That relevance contract
/// governs *selection* — which memories are worth showing at all — and this
/// function does not revisit it; every item passed in here already cleared that
/// bar.
///
/// This function governs a different, later decision: once the (typically tiny)
/// selected shortlist doesn't fit in the render budget, WHICH side gives way. By
/// design, that decision favors cache stability: a coarse item is never dropped in
/// favor of a fine item, even a more-relevant one, because the whole point of the
/// segmentation is a prefix whose presence and byte content the fine/volatile side
/// can never influence. In practice this almost never bites — recall shortlists
/// are tiny (3 items in the engine caller) and comfortably fit typical section
/// budgets — but when it does, cache stability wins over relevance-at-the-margin
/// by explicit design; see
/// `budget_exhaustion_drops_suffix_before_prefix_even_if_suffix_item_was_more_relevant`.
pub fn segment_by_granularity_budget(
    items: &[GranularityBudgetItem],
    total_budget_chars: usize,
) -> GranularityBudgetSegments {
    let mut prefix_items = Vec::new();
    let mut suffix_items = Vec::new();
    for item in items {
        if TemporalGranularity::is_high_churn(item.granularity) {
            suffix_items.push(item);
        } else {
            prefix_items.push(item);
        }
    }

    let (prefix, prefix_chars, prefix_dropped_ids) =
        fill_budget_segment(&prefix_items, total_budget_chars);
    // Fine-grained content only spends what the coarse segment didn't need, so
    // growing/adding a day-level memory can shrink or empty the SUFFIX segment but
    // can never reach back and shrink, reorder, or drop anything from the PREFIX
    // segment.
    let remaining_budget = total_budget_chars.saturating_sub(prefix_chars);
    let (suffix, _suffix_chars, suffix_dropped_ids) =
        fill_budget_segment(&suffix_items, remaining_budget);

    GranularityBudgetSegments {
        prefix,
        suffix,
        prefix_dropped_ids,
        suffix_dropped_ids,
    }
}

/// Greedily concatenate `items` (already in priority order) up to `budget_chars`.
/// Always includes at least the first item even if it alone exceeds the budget —
/// so one oversized memory doesn't silently produce an empty segment — mirroring
/// the truncation convention `bamboo-engine`'s `load_relevant_memory_snippets`
/// already uses for the flat (pre-segmentation) render loop. Stops at the first
/// item that doesn't fit rather than skipping ahead to a smaller later one, so
/// segment order always matches input order and every dropped id is contiguous
/// from the cutoff to the end.
///
/// A `budget_chars` of `0` renders nothing and drops every item — this is how a
/// fully-exhausted-by-the-prefix budget correctly starves the suffix segment
/// entirely rather than force-including one oversized item into a segment that
/// was allotted no room at all.
fn fill_budget_segment(
    items: &[&GranularityBudgetItem],
    budget_chars: usize,
) -> (String, usize, Vec<String>) {
    if budget_chars == 0 {
        return (
            String::new(),
            0,
            items.iter().map(|item| item.id.clone()).collect(),
        );
    }

    let mut rendered = String::new();
    let mut used_chars = 0usize;
    let mut dropped_ids = Vec::new();
    let mut truncated = false;

    for item in items {
        if truncated {
            dropped_ids.push(item.id.clone());
            continue;
        }
        let cost = item.rendered.chars().count();
        if used_chars > 0 && used_chars + cost > budget_chars {
            truncated = true;
            dropped_ids.push(item.id.clone());
            continue;
        }
        used_chars += cost;
        rendered.push_str(&item.rendered);
    }

    (rendered, used_chars, dropped_ids)
}

#[cfg(test)]
mod granularity_budget_tests {
    use super::*;

    fn item(
        id: &str,
        granularity: Option<TemporalGranularity>,
        rendered: &str,
    ) -> GranularityBudgetItem {
        GranularityBudgetItem::new(id, granularity, rendered)
    }

    #[test]
    fn coarse_memories_render_before_fine_ones() {
        let items = vec![
            // Deliberately interleaved input order — output must still group
            // coarse-before-fine regardless of input order.
            item("day-1", Some(TemporalGranularity::Day), "DAY."),
            item("year-1", Some(TemporalGranularity::Year), "YEAR."),
            item("week-1", Some(TemporalGranularity::Week), "WEEK."),
            item("quarter-1", Some(TemporalGranularity::Quarter), "QUARTER."),
        ];

        let segments = segment_by_granularity_budget(&items, 1_000);
        assert_eq!(segments.prefix, "YEAR.QUARTER.");
        assert_eq!(segments.suffix, "DAY.WEEK.");
        let combined = segments.combined();
        assert!(combined.find("YEAR").unwrap() < combined.find("DAY").unwrap());
        assert!(segments.prefix_dropped_ids.is_empty());
        assert!(segments.suffix_dropped_ids.is_empty());
    }

    #[test]
    fn none_granularity_is_treated_as_prefix_eligible() {
        // `None` mirrors Phase 1's "most stable" treatment (cache_stability_rank
        // rank 0) — untagged memories belong in the prefix, not the suffix.
        let items = vec![
            item("untagged", None, "UNTAGGED."),
            item("day-1", Some(TemporalGranularity::Day), "DAY."),
        ];
        let segments = segment_by_granularity_budget(&items, 1_000);
        assert_eq!(segments.prefix, "UNTAGGED.");
        assert_eq!(segments.suffix, "DAY.");
    }

    #[test]
    fn changing_or_adding_a_day_memory_does_not_change_the_rendered_prefix_segment() {
        let coarse = [
            item("year-1", Some(TemporalGranularity::Year), "YEAR-ONE."),
            item(
                "quarter-1",
                Some(TemporalGranularity::Quarter),
                "QUARTER-ONE.",
            ),
        ];

        let before = vec![
            coarse[0].clone(),
            coarse[1].clone(),
            item("day-1", Some(TemporalGranularity::Day), "DAY-ONE."),
        ];
        let segments_before = segment_by_granularity_budget(&before, 500);

        // Mutate the day memory's content AND add a brand new day memory — both
        // are pure suffix-side churn.
        let after = vec![
            coarse[0].clone(),
            coarse[1].clone(),
            item(
                "day-1",
                Some(TemporalGranularity::Day),
                "DAY-ONE-REWRITTEN-WITH-MORE-DETAIL.",
            ),
            item(
                "day-2",
                Some(TemporalGranularity::Day),
                "DAY-TWO-BRAND-NEW.",
            ),
        ];
        let segments_after = segment_by_granularity_budget(&after, 500);

        assert_eq!(
            segments_before.prefix, segments_after.prefix,
            "prefix segment must be byte-identical regardless of suffix churn"
        );
        assert_ne!(
            segments_before.suffix, segments_after.suffix,
            "suffix segment is expected to change"
        );
    }

    #[test]
    fn budget_exhaustion_drops_fine_before_coarse() {
        let items = vec![
            item("year-1", Some(TemporalGranularity::Year), &"Y".repeat(40)),
            item("day-1", Some(TemporalGranularity::Day), &"D".repeat(40)),
        ];
        // Budget covers the coarse item comfortably but leaves nothing for the
        // fine one.
        let segments = segment_by_granularity_budget(&items, 40);
        assert_eq!(segments.prefix.chars().count(), 40);
        assert!(segments.prefix_dropped_ids.is_empty());
        assert!(
            segments.suffix.is_empty(),
            "suffix must be fully starved once the prefix consumes the whole budget"
        );
        assert_eq!(segments.suffix_dropped_ids, vec!["day-1".to_string()]);
    }

    #[test]
    fn budget_exhaustion_drops_suffix_before_prefix_even_if_suffix_item_was_more_relevant() {
        // By the time items reach this function they already cleared recall's
        // relevance bar (see the module doc comment on `segment_by_granularity_budget`).
        // Put the "more relevant" fine item FIRST in priority order — recall would
        // have ranked it there — and confirm the granularity split still wins over
        // that ordering once the shared budget can't fit both: the coarse item is
        // exactly wide enough to consume all remaining room.
        let items = vec![
            item(
                "day-most-relevant",
                Some(TemporalGranularity::Day),
                &"R".repeat(30),
            ),
            item(
                "year-least-relevant",
                Some(TemporalGranularity::Year),
                &"C".repeat(30),
            ),
        ];
        let segments = segment_by_granularity_budget(&items, 30);
        assert_eq!(
            segments.prefix.chars().count(),
            30,
            "coarse item still renders"
        );
        assert!(
            segments.suffix.is_empty(),
            "fine item is dropped by design, despite higher relevance"
        );
        assert_eq!(
            segments.suffix_dropped_ids,
            vec!["day-most-relevant".to_string()]
        );
    }

    #[test]
    fn leftover_prefix_budget_is_available_to_the_suffix() {
        let items = vec![
            item("year-1", Some(TemporalGranularity::Year), &"Y".repeat(10)),
            item("day-1", Some(TemporalGranularity::Day), &"D".repeat(10)),
        ];
        let segments = segment_by_granularity_budget(&items, 25);
        assert_eq!(segments.prefix.chars().count(), 10);
        assert_eq!(segments.suffix.chars().count(), 10);
        assert!(segments.prefix_dropped_ids.is_empty());
        assert!(segments.suffix_dropped_ids.is_empty());
    }

    #[test]
    fn always_includes_at_least_one_oversized_item_per_segment() {
        let items = vec![item(
            "year-huge",
            Some(TemporalGranularity::Year),
            &"Y".repeat(500),
        )];
        let segments = segment_by_granularity_budget(&items, 10);
        assert_eq!(segments.prefix.chars().count(), 500);
        assert!(segments.prefix_dropped_ids.is_empty());
    }

    #[test]
    fn zero_budget_drops_everything_and_starves_both_segments() {
        let items = vec![
            item("year-1", Some(TemporalGranularity::Year), "Y"),
            item("day-1", Some(TemporalGranularity::Day), "D"),
        ];
        let segments = segment_by_granularity_budget(&items, 0);
        assert!(segments.prefix.is_empty());
        assert!(segments.suffix.is_empty());
        assert_eq!(segments.prefix_dropped_ids, vec!["year-1".to_string()]);
        assert_eq!(segments.suffix_dropped_ids, vec!["day-1".to_string()]);
    }

    #[test]
    fn empty_input_yields_empty_segments() {
        let segments = segment_by_granularity_budget(&[], 1_000);
        assert_eq!(segments, GranularityBudgetSegments::default());
    }
}
