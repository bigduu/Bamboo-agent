//! Small, allocation-bounded fuzzy search shared by the session, model, and
//! command pickers. The TUI deliberately keeps this local instead of pulling
//! in another palette dependency: catalogs are small (hundreds of rows), and
//! a deterministic subsequence score is easier to keep selection-stable.

/// Return ranked item indices for `query`.
///
/// Every whitespace-separated query token must be a subsequence of the item's
/// searchable text. Contiguous characters, word boundaries, and early matches
/// rank higher. Empty queries preserve the caller's source order.
pub fn ranked_indices<T>(
    items: &[T],
    query: &str,
    mut searchable_text: impl FnMut(&T) -> String,
) -> Vec<usize> {
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(|token| token.to_lowercase())
        .filter(|token| !token.is_empty())
        .collect();

    if tokens.is_empty() {
        return (0..items.len()).collect();
    }

    let mut ranked = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            let haystack = searchable_text(item).to_lowercase();
            let score = tokens.iter().try_fold(0_i64, |total, token| {
                fuzzy_subsequence_score(&haystack, token).map(|score| total + score)
            })?;
            Some((index, score))
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    ranked.into_iter().map(|(index, _)| index).collect()
}

fn fuzzy_subsequence_score(haystack: &str, needle: &str) -> Option<i64> {
    if needle.is_empty() {
        return Some(0);
    }

    let haystack_chars = haystack.chars().collect::<Vec<_>>();
    let mut cursor = 0_usize;
    let mut previous_match = None;
    let mut score = 0_i64;

    for wanted in needle.chars() {
        let relative = haystack_chars[cursor..]
            .iter()
            .position(|candidate| *candidate == wanted)?;
        let matched = cursor + relative;

        // Prefer compact runs and token starts. The constants are deliberately
        // small; stable source ordering remains the final tie-breaker.
        score += 10;
        if previous_match.is_some_and(|previous| previous + 1 == matched) {
            score += 8;
        }
        if matched == 0
            || haystack_chars
                .get(matched.saturating_sub(1))
                .is_some_and(|ch| !ch.is_alphanumeric())
        {
            score += 6;
        }
        score -= matched.min(64) as i64;

        previous_match = Some(matched);
        cursor = matched + 1;
    }

    Some(score)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_query_preserves_source_order() {
        let items = ["beta", "alpha"];
        assert_eq!(
            ranked_indices(&items, "", |item| (*item).to_string()),
            vec![0, 1]
        );
    }

    #[test]
    fn tokens_match_as_ranked_subsequences() {
        let items = [
            "OpenAI GPT 5.6",
            "Anthropic Claude Sonnet",
            "OpenAI GPT 4.1",
        ];
        assert_eq!(
            ranked_indices(&items, "oa 56", |item| (*item).to_string()),
            vec![0]
        );
        assert_eq!(
            ranked_indices(&items, "gpt", |item| (*item).to_string()),
            vec![0, 2]
        );
    }

    #[test]
    fn matching_is_unicode_and_case_insensitive() {
        let items = ["会话 Alpha", "Session Beta"];
        assert_eq!(
            ranked_indices(&items, "会a", |item| (*item).to_string()),
            vec![0]
        );
    }
}
