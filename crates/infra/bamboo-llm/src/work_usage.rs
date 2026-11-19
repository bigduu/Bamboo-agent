//! Authoritative provider stream accounting for a reserved Work request.

use std::time::Duration;

use bamboo_domain::TokenUsage;

use crate::LLMChunk;

/// Complete the accounting trailer within a bounded interval after Done.
pub const POST_DONE_USAGE_TIMEOUT: Duration = Duration::from_secs(1);

/// Usage bounds observed directly from the provider, including partial reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedWorkUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// Project normalized provider frames into durable accounting evidence.
/// Callers require a clean terminal and EOF before settling exact usage.
#[derive(Default)]
pub struct WorkUsage {
    input: Option<u64>,
    output: Option<u64>,
    // Distinct positive counts remain lower bounds even when the native
    // parser loses optional-field presence. Keep each across split events.
    fresh_input: Option<u64>,
    cache_creation: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    output_floor: Option<u64>,
    replacement_input_floor: Option<u64>,
    reported_total: Option<u64>,
    latest_replacement: Option<(Option<u64>, Option<u64>)>,
    done: bool,
    inconsistent: bool,
}
impl WorkUsage {
    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn observe(&mut self, chunk: &LLMChunk) {
        match chunk {
            LLMChunk::Done => self.done = true,
            LLMChunk::ProviderUsageReplace(usage) => {
                // Retain observed maxima for immediate over-cap checks, but
                // never refund from stale data after malformed evidence.
                self.inconsistent |= usage.invalid_fields.any();
                self.latest_replacement = Some((usage.input_tokens, usage.output_tokens));
                if let (Some(input), Some(output), Some(total)) =
                    (usage.input_tokens, usage.output_tokens, usage.total_tokens)
                {
                    self.inconsistent |= input.checked_add(output) != Some(total);
                }
                self.input = self.input.max(usage.input_tokens);
                self.output = self.output.max(usage.output_tokens);
                self.reported_total = self.reported_total.max(usage.total_tokens);
                self.cache_creation = self.cache_creation.max(usage.cache_creation_input_tokens);
                self.cache_read = self.cache_read.max(usage.cache_read_input_tokens);
                self.cache_write = self.cache_write.max(usage.cache_write_input_tokens);
                self.output_floor = self.output_floor.max(usage.reasoning_tokens);
                self.replacement_input_floor = self.replacement_input_floor.max(usage.input_floor);
                self.output_floor = self.output_floor.max(usage.output_floor);
            }
            LLMChunk::ProviderUsage {
                input_tokens,
                output_tokens,
                total_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens,
            } => {
                if let (Some(input), Some(output), Some(total)) =
                    (input_tokens, output_tokens, total_tokens)
                {
                    self.inconsistent |= input.checked_add(*output) != Some(*total);
                }
                self.input = self.input.max(*input_tokens);
                self.output = self.output.max(*output_tokens);
                self.reported_total = self.reported_total.max(*total_tokens);
                self.cache_creation = self.cache_creation.max(*cache_creation_input_tokens);
                self.cache_read = self.cache_read.max(*cache_read_input_tokens);
                self.cache_write = self.cache_write.max(*cache_write_input_tokens);
                self.output_floor = self.output_floor.max(*reasoning_tokens);
            }
            LLMChunk::CacheUsage {
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
            } => {
                // Normalized fresh/read/creation dimensions are disjoint. A
                // later event may omit fresh input, so max the dimensions
                // individually before summing rather than maxing event sums.
                self.fresh_input = self.fresh_input.max(Some(*input_tokens));
                self.cache_creation = self.cache_creation.max(Some(*cache_creation_input_tokens));
                self.cache_read = self.cache_read.max(Some(*cache_read_input_tokens));
            }
            LLMChunk::UsageSummary {
                output_tokens,
                thinking_tokens,
            } => {
                // Thinking can be a subset of output. Either positive count
                // proves a lower bound; adding them would invent a charge.
                self.output_floor = self
                    .output_floor
                    .max(Some((*output_tokens).max(*thinking_tokens)));
            }
            _ => {}
        }
    }
    fn input_floor(&self) -> Option<u64> {
        let disjoint = (self.fresh_input.is_some()
            || self.cache_creation.is_some()
            || self.cache_read.is_some())
        .then(|| {
            self.fresh_input
                .unwrap_or(0)
                .saturating_add(self.cache_creation.unwrap_or(0))
                .saturating_add(self.cache_read.unwrap_or(0))
        });
        // Canonical input already includes cache. Cache-write has no disjoint
        // relationship to the legacy counters, so compare it independently.
        self.input
            .max(disjoint)
            .max(self.cache_write)
            .max(self.replacement_input_floor)
    }
    pub fn over_cap(&self, input_cap: u32, output_cap: u32) -> Option<ObservedWorkUsage> {
        let input_tokens = self.input_floor();
        let output_tokens = self.output.max(self.output_floor);
        (input_tokens.is_some_and(|v| v > u64::from(input_cap))
            || output_tokens.is_some_and(|v| v > u64::from(output_cap))
            || self
                .reported_total
                .is_some_and(|v| v > u64::from(input_cap) + u64::from(output_cap)))
        .then_some(ObservedWorkUsage {
            input_tokens,
            output_tokens,
            total_tokens: self.reported_total,
        })
    }
    pub fn exact(&self) -> Option<TokenUsage> {
        let (input_tokens, output_tokens) = (self.input?, self.output?);
        if let Some((latest_input, latest_output)) = self.latest_replacement {
            // Previous maxima remain safe over-cap lower bounds but are not
            // exact settlement evidence when the latest snapshot is incomplete
            // or lower. A later complete, consistent snapshot can recover.
            if latest_input != Some(input_tokens) || latest_output != Some(output_tokens) {
                return None;
            }
        }
        if self.inconsistent
            || self.input_floor().is_some_and(|floor| floor > input_tokens)
            || self.output_floor.is_some_and(|floor| floor > output_tokens)
            // Total may arrive separately, including after Done. Incomplete
            // or inconsistent canonical evidence never authorizes a refund.
            || self.reported_total.is_some_and(|total| input_tokens.checked_add(output_tokens) != Some(total))
        {
            return None;
        }
        Some(TokenUsage {
            prompt_tokens: input_tokens,
            completion_tokens: output_tokens,
            total_tokens: input_tokens.checked_add(output_tokens)?,
        })
    }
}
