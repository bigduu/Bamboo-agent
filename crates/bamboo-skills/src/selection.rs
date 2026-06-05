use std::collections::HashSet;

/// Parse selected skill IDs from session metadata.
///
/// Supports JSON array format (`["skill-a","skill-b"]`) and
/// legacy comma-separated format (`skill-a,skill-b`).
pub fn parse_selected_skill_ids_metadata(raw: &str) -> Option<Vec<String>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let parsed_json = serde_json::from_str::<Vec<String>>(raw).ok();
    let values = match parsed_json {
        Some(list) => list,
        None => raw
            .split(',')
            .map(|part| part.trim().to_string())
            .collect::<Vec<_>>(),
    };

    normalize_selected_skill_ids(values)
}

/// Normalize selected skill IDs:
/// - trim whitespace
/// - drop empty values
/// - deduplicate
/// - stable-sort for deterministic persistence
pub fn normalize_selected_skill_ids<I>(ids: I) -> Option<Vec<String>>
where
    I: IntoIterator<Item = String>,
{
    let mut deduped: HashSet<String> = HashSet::new();
    for id in ids {
        let trimmed = id.trim();
        if trimmed.is_empty() {
            continue;
        }
        deduped.insert(trimmed.to_string());
    }

    if deduped.is_empty() {
        return None;
    }

    let mut normalized = deduped.into_iter().collect::<Vec<_>>();
    normalized.sort();
    Some(normalized)
}

#[cfg(test)]
mod tests {
    use super::{normalize_selected_skill_ids, parse_selected_skill_ids_metadata};

    #[test]
    fn parse_selected_skill_ids_metadata_supports_json_array() {
        let parsed = parse_selected_skill_ids_metadata(r#"["skill-b","skill-a","skill-a"]"#)
            .expect("should parse");
        assert_eq!(parsed, vec!["skill-a".to_string(), "skill-b".to_string()]);
    }

    #[test]
    fn parse_selected_skill_ids_metadata_supports_csv_fallback() {
        let parsed = parse_selected_skill_ids_metadata(" skill-b, skill-a , skill-a ")
            .expect("should parse");
        assert_eq!(parsed, vec!["skill-a".to_string(), "skill-b".to_string()]);
    }

    #[test]
    fn parse_selected_skill_ids_metadata_returns_none_for_empty_input() {
        assert!(parse_selected_skill_ids_metadata("   ").is_none());
        assert!(parse_selected_skill_ids_metadata("[]").is_none());
    }

    #[test]
    fn normalize_selected_skill_ids_deduplicates_and_sorts() {
        let normalized = normalize_selected_skill_ids(vec![
            " skill-z ".to_string(),
            "skill-a".to_string(),
            "".to_string(),
            "skill-z".to_string(),
        ])
        .expect("should normalize");

        assert_eq!(
            normalized,
            vec!["skill-a".to_string(), "skill-z".to_string()]
        );
    }
}
