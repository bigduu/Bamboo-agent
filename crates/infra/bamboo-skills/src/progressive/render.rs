// Copyright 2025 OpenAI. Licensed under Apache-2.0.
// Adapted from Codex ext/skills/src/{render.rs,aliases.rs,catalog_prompt.rs} at
// 7f892275e31002f0422477c6219189284560e689; see third_party/codex notices.
use super::{source_rank, SkillCatalogMetadata};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::num::NonZeroUsize;

/// Codex's explicit main-prompt limit, measured in UTF-8 bytes, not characters.
pub const EXPLICIT_SKILL_PROMPT_BYTES: usize = 8_000;

/// Adapted from pinned Codex `truncate_utf8_to_bytes` and its character-boundary
/// utility. This only truncates the supplied string; it establishes no access.
pub fn truncate_skill_utf8_bytes(contents: &str, max_bytes: usize) -> (&str, bool) {
    let mut end = contents.len().min(max_bytes);
    while !contents.is_char_boundary(end) {
        end -= 1;
    }
    (&contents[..end], end < contents.len())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SkillMetadataBudget {
    Tokens(usize),
    Characters(usize),
}

pub fn skill_metadata_budget(
    context_window: Option<i64>,
    metadata_tokens: Option<NonZeroUsize>,
) -> SkillMetadataBudget {
    if let Some(tokens) = metadata_tokens {
        return SkillMetadataBudget::Tokens(tokens.get().min(10_000));
    }
    context_window
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(|value| {
            SkillMetadataBudget::Tokens(value.saturating_mul(2).saturating_div(100).max(1))
        })
        .unwrap_or(SkillMetadataBudget::Characters(8_000))
}

impl SkillMetadataBudget {
    pub fn limit(self) -> usize {
        match self {
            Self::Tokens(limit) | Self::Characters(limit) => limit,
        }
    }
    fn with_limit(self, limit: usize) -> Self {
        match self {
            Self::Tokens(_) => Self::Tokens(limit),
            Self::Characters(_) => Self::Characters(limit),
        }
    }
    fn cost_from_counts(self, chars: usize, bytes: usize) -> usize {
        match self {
            Self::Tokens(_) => bytes.saturating_add(3) / 4,
            Self::Characters(_) => chars,
        }
    }
    pub fn cost(self, text: &str) -> usize {
        self.cost_from_counts(text.chars().count(), text.len())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SkillCatalogRenderPolicy {
    CoreCompatible,
    ExtensionCompatible,
}

impl SkillCatalogRenderPolicy {
    fn description(self, entry: &SkillCatalogMetadata) -> &str {
        match self {
            Self::CoreCompatible => &entry.description,
            Self::ExtensionCompatible => entry
                .short_description
                .as_deref()
                .unwrap_or(&entry.description),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SkillCatalogRender {
    pub text: String,
    pub included_count: usize,
    pub omitted_count: usize,
    pub truncated_description_chars: usize,
}

pub(crate) fn truncate_catalog_skill_description(description: &str) -> Cow<'_, str> {
    if description.char_indices().nth(1_024).is_none() {
        return Cow::Borrowed(description);
    }
    let end = description
        .char_indices()
        .nth(1_021)
        .map_or(description.len(), |(index, _)| index);
    Cow::Owned(format!("{}...", &description[..end]))
}

struct SkillLine<'a> {
    name: &'a str,
    description: Cow<'a, str>,
    locator: String,
}
impl<'a> SkillLine<'a> {
    fn new(
        entry: &'a SkillCatalogMetadata,
        policy: SkillCatalogRenderPolicy,
        locator: String,
    ) -> Self {
        Self {
            name: &entry.name,
            description: truncate_catalog_skill_description(policy.description(entry)),
            locator,
        }
    }
    fn render(&self, chars: usize) -> String {
        let end = self
            .description
            .char_indices()
            .nth(chars)
            .map_or(self.description.len(), |(index, _)| index);
        let description = &self.description[..end];
        if description.is_empty() {
            format!("- {}: (file: {})", self.name, self.locator)
        } else {
            format!("- {}: {} (file: {})", self.name, description, self.locator)
        }
    }
    fn cost(&self, budget: SkillMetadataBudget, chars: usize) -> usize {
        budget.cost(&format!("{}\n", self.render(chars)))
    }
    fn chars(&self) -> usize {
        self.description.chars().count()
    }
}

struct DescriptionBudgetLine {
    description_char_count: usize,
    extra_costs: Vec<usize>,
}
impl DescriptionBudgetLine {
    fn new(line: &SkillLine<'_>, budget: SkillMetadataBudget) -> Self {
        let minimum_line = line.render(0);
        let minimum_chars = minimum_line.chars().count().saturating_add(1);
        let minimum_bytes = minimum_line.len().saturating_add(1);
        let minimum_cost = budget.cost_from_counts(minimum_chars, minimum_bytes);
        let description_char_count = line.chars();
        let mut extra_costs = vec![0];
        let mut prefix_chars = 0usize;
        let mut prefix_bytes = 0usize;
        for ch in line.description.chars() {
            prefix_chars = prefix_chars.saturating_add(1);
            prefix_bytes = prefix_bytes.saturating_add(ch.len_utf8());
            let cost = budget
                .cost_from_counts(
                    minimum_chars.saturating_add(prefix_chars).saturating_add(1),
                    minimum_bytes.saturating_add(prefix_bytes).saturating_add(1),
                )
                .saturating_sub(minimum_cost);
            extra_costs.push(cost);
        }
        Self {
            description_char_count,
            extra_costs,
        }
    }
}

fn allocate_description_chars(
    budget: SkillMetadataBudget,
    lines: &[SkillLine<'_>],
    limit: usize,
) -> Vec<usize> {
    let budget_lines = lines
        .iter()
        .map(|line| DescriptionBudgetLine::new(line, budget))
        .collect::<Vec<_>>();
    let mut allocations = vec![0usize; budget_lines.len()];
    let mut current_costs = vec![0usize; budget_lines.len()];
    let mut remaining = limit;
    loop {
        let mut changed = false;
        for (index, line) in budget_lines.iter().enumerate() {
            if allocations[index] >= line.description_char_count {
                continue;
            }
            let next = allocations[index].saturating_add(1);
            let cost = line.extra_costs[next];
            let delta = cost.saturating_sub(current_costs[index]);
            if delta <= remaining {
                allocations[index] = next;
                current_costs[index] = cost;
                remaining = remaining.saturating_sub(delta);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    allocations
}

fn allocate_skill_lines(
    lines: &[SkillLine<'_>],
    budget: SkillMetadataBudget,
) -> Vec<Option<usize>> {
    let full = lines.iter().fold(0usize, |used, line| {
        used.saturating_add(line.cost(budget, line.chars()))
    });
    if full <= budget.limit() {
        return lines.iter().map(|line| Some(line.chars())).collect();
    }
    let minimum = lines.iter().fold(0usize, |used, line| {
        used.saturating_add(line.cost(budget, 0))
    });
    if minimum <= budget.limit() {
        return allocate_description_chars(budget, lines, budget.limit().saturating_sub(minimum))
            .into_iter()
            .map(Some)
            .collect();
    }
    let mut used = 0usize;
    lines
        .iter()
        .map(|line| {
            let next = used.saturating_add(line.cost(budget, 0));
            if next <= budget.limit() {
                used = next;
                Some(0)
            } else {
                None
            }
        })
        .collect()
}

struct AliasPlan {
    roots: Vec<String>,
}
impl AliasPlan {
    fn build(entries: &[&SkillCatalogMetadata]) -> Self {
        let mut seen = HashSet::new();
        Self {
            roots: entries
                .iter()
                .filter(|entry| seen.insert(&entry.root))
                .map(|entry| entry.root.clone())
                .collect(),
        }
    }
    fn shorten(&self, locator: &str) -> String {
        self.roots
            .iter()
            .enumerate()
            .filter_map(|(index, root)| {
                let suffix = locator
                    .strip_prefix(root.trim_end_matches(['/', '\\']))?
                    .strip_prefix(['/', '\\'])?;
                Some((root, format!("r{index}/{suffix}")))
            })
            .max_by_key(|(root, _)| root.len())
            .map(|(_, shortened)| shortened)
            .unwrap_or_else(|| locator.to_string())
    }
    fn table(&self) -> String {
        let lines = self
            .roots
            .iter()
            .enumerate()
            .map(|(index, root)| format!("- `r{index}` = `{root}`"))
            .collect::<Vec<_>>();
        format!("### Skill roots\n{}\n", lines.join("\n"))
    }
}

fn render_lines(
    entries: &[&SkillCatalogMetadata],
    policy: SkillCatalogRenderPolicy,
    budget: SkillMetadataBudget,
    aliases: Option<&AliasPlan>,
) -> Option<SkillCatalogRender> {
    let prefix = aliases.map_or_else(String::new, AliasPlan::table);
    let overhead = budget.cost(&prefix);
    if aliases.is_some_and(|plan| plan.roots.is_empty())
        || (overhead >= budget.limit() && !prefix.is_empty())
    {
        return None;
    }
    let lines = entries
        .iter()
        .map(|entry| {
            SkillLine::new(
                entry,
                policy,
                aliases.map_or_else(
                    || entry.main_resource.clone(),
                    |plan| plan.shorten(&entry.main_resource),
                ),
            )
        })
        .collect::<Vec<_>>();
    let adjusted = budget.with_limit(budget.limit().saturating_sub(overhead));
    let allocations = allocate_skill_lines(&lines, adjusted);
    let mut rendered = Vec::new();
    let mut omitted = 0usize;
    let mut truncated = 0usize;
    for (line, allocation) in lines.iter().zip(allocations) {
        if let Some(chars) = allocation {
            rendered.push(line.render(chars));
            truncated += line.chars().saturating_sub(chars);
        } else {
            omitted += 1;
            truncated += line.chars();
        }
    }
    if omitted > 0 && matches!(policy, SkillCatalogRenderPolicy::ExtensionCompatible) {
        loop {
            let word = if omitted == 1 { "skill" } else { "skills" };
            let marker =
                format!("- {omitted} additional {word} omitted from this bounded skills list.");
            let cost = rendered
                .iter()
                .map(|line| adjusted.cost(&format!("{line}\n")))
                .sum::<usize>();
            if cost.saturating_add(adjusted.cost(&format!("{marker}\n"))) <= adjusted.limit() {
                rendered.push(marker);
                break;
            }
            if rendered.pop().is_none() {
                break;
            }
            omitted += 1;
        }
    }
    let body = rendered
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    Some(SkillCatalogRender {
        text: format!("{prefix}{body}"),
        included_count: entries.len().saturating_sub(omitted),
        omitted_count: omitted,
        truncated_description_chars: truncated,
    })
}

/// Render metadata only. Description allocation, root aliases and omission
/// notices all consume the declared metadata budget; usage guidance is separate.
pub fn render_skill_catalog(
    entries: &[SkillCatalogMetadata],
    policy: SkillCatalogRenderPolicy,
    budget: SkillMetadataBudget,
) -> SkillCatalogRender {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    if matches!(policy, SkillCatalogRenderPolicy::CoreCompatible) {
        ordered.sort_by(|a, b| {
            source_rank(a.source)
                .cmp(&source_rank(b.source))
                .then(a.name.cmp(&b.name))
                .then(a.main_resource.cmp(&b.main_resource))
        });
    }
    let plain = render_lines(&ordered, policy, budget, None)
        .expect("unaliased metadata has no root-table overhead");
    let aliases = AliasPlan::build(&ordered);
    let Some(aliased) = render_lines(&ordered, policy, budget, Some(&aliases)) else {
        return plain;
    };
    if (
        aliased.included_count,
        usize::MAX - aliased.truncated_description_chars,
    ) > (
        plain.included_count,
        usize::MAX - plain.truncated_description_chars,
    ) || (aliased.included_count == plain.included_count
        && aliased.truncated_description_chars == plain.truncated_description_chars
        && budget.cost(&aliased.text) < budget.cost(&plain.text))
    {
        aliased
    } else {
        plain
    }
}

// Stable packages and source membership replace Codex executor/cloud aliases.
const SKILL_USAGE: &str = r#"### Using Skills
- Discovery: use `skills_list` to find each Skill's stable package and main resource. Display root aliases alone do not authorize file access.
- Trigger rules: if the user names a Skill or the task clearly matches its description, use it for the current turn. Multiple mentions mean use them all; do not automatically carry Skills into later turns.
- Announce the selected Skills and their order in one short sentence.
- Open the selected package with `skills_read` (omitting resource selects `SKILL.md`). Follow every `next_cursor` until EOF before any task action.
- When instructions name relative files such as `references/guide.md`, use that resource with the same selected package. Read every required reference completely to EOF before applying it.
- The main agent must read the instructions itself. Subagents may perform task work when the selected Skill allows it; delegated summaries do not replace reading.
- Read the files required for the task and preserve the Skill's own routing. Reuse supported scripts/assets/templates where appropriate.
- If a required Skill or resource is unavailable, state the limitation and use the best available fallback. Never treat a partial page as complete instructions.
"#;

/// Complete guidance or None when indivisible guidance does not fit.
/// The runtime must deliberately install the read Tool before exposing this.
pub fn render_skill_usage_instructions(budget: SkillMetadataBudget) -> Option<&'static str> {
    (budget.cost(SKILL_USAGE) <= budget.limit()).then_some(SKILL_USAGE)
}
