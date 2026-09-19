use std::collections::{BTreeSet, HashMap, HashSet};

use actix_web::{web, HttpResponse, Responder};
use chrono::NaiveDate;

use super::super::{
    internal_error, McpServerUsageItem, McpToolUsageItem, MetricsUsageBreakdownResponse,
    MetricsUsageQuery, SkillUsageItem, UsageCountItem,
};
use crate::app_state::AppState;
use bamboo_tools::exposure::canonical_tool_name;

use super::filters::normalize_model_filter;

#[derive(Debug, Clone)]
struct ParsedMcpAlias {
    server_id: String,
    tool_name: String,
}

fn is_v1_alias_tag(value: &str) -> bool {
    value.len() == 26
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || matches!(byte, b'2'..=b'7'))
}

fn parse_mcp_tool_alias(value: &str) -> Option<ParsedMcpAlias> {
    let trimmed = value.trim();
    if !trimmed.starts_with("mcp__") {
        return None;
    }

    let rest = &trimmed["mcp__".len()..];
    if let Some((server_tag, owner_tag)) = rest
        .strip_prefix("v1_")
        .and_then(|tags| tags.split_once('_'))
    {
        if is_v1_alias_tag(server_tag) && is_v1_alias_tag(owner_tag) {
            // V1 exposes only stable pseudonyms. Preserve real per-server
            // grouping without putting raw MCP labels into metrics.
            return Some(ParsedMcpAlias {
                server_id: format!("v1_{server_tag}"),
                tool_name: owner_tag.to_string(),
            });
        }
    }

    let sep = rest.find("__")?;
    if sep == 0 {
        return None;
    }

    let server_id = rest[..sep].trim();
    let tool_name = rest[sep + 2..].trim();
    if server_id.is_empty() || tool_name.is_empty() {
        return None;
    }

    Some(ParsedMcpAlias {
        server_id: server_id.to_string(),
        tool_name: tool_name.to_string(),
    })
}

fn session_matches_range(
    started_at: chrono::DateTime<chrono::Utc>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> bool {
    let started_date = started_at.date_naive();
    if let Some(start) = start_date {
        if started_date < start {
            return false;
        }
    }
    if let Some(end) = end_date {
        if started_date > end {
            return false;
        }
    }
    true
}

fn parse_skill_id(arguments: &str) -> Option<String> {
    let parsed = serde_json::from_str::<serde_json::Value>(arguments).ok()?;
    parsed
        .get("skill_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Gets usage breakdown across core tools, skills, and MCP tools.
///
/// # HTTP Route
/// `GET /metrics/usage-breakdown`
pub async fn usage_breakdown(
    state: web::Data<AppState>,
    query: web::Query<MetricsUsageQuery>,
) -> impl Responder {
    let session_index_entries = state.session_store.list_index_entries().await;
    let model_filter = normalize_model_filter(query.model.as_deref());

    let mut matched_session_ids: HashSet<String> = HashSet::new();
    let mut total_tool_calls = 0_u64;
    let mut core_tool_calls = 0_u64;
    let mut skill_load_calls = 0_u64;
    let mut mcp_calls = 0_u64;

    let mut core_tools: HashMap<String, u64> = HashMap::new();
    let mut skills: HashMap<String, u64> = HashMap::new();
    let mut mcp_servers: HashMap<String, u64> = HashMap::new();
    let mut mcp_server_tool_sets: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut mcp_tools: HashMap<(String, String, String), u64> = HashMap::new();
    let mut sessions_with_skill_loads: HashSet<String> = HashSet::new();
    let mut sessions_with_mcp_calls: HashSet<String> = HashSet::new();

    for entry in session_index_entries {
        if let Some(model) = model_filter.as_ref() {
            if entry.model != *model {
                continue;
            }
        }
        if !session_matches_range(entry.created_at, query.start_date, query.end_date) {
            continue;
        }

        matched_session_ids.insert(entry.id.clone());

        let session = match state.storage.load_session(&entry.id).await {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(error) => return internal_error(error),
        };

        let mut session_has_skill_load = false;
        let mut session_has_mcp_call = false;

        for message in session.messages {
            let Some(tool_calls) = message.tool_calls else {
                continue;
            };

            for tool_call in tool_calls {
                let tool_name = tool_call.function.name;
                total_tool_calls += 1;

                if let Some(parsed_mcp) = parse_mcp_tool_alias(&tool_name) {
                    mcp_calls += 1;
                    session_has_mcp_call = true;
                    *mcp_servers.entry(parsed_mcp.server_id.clone()).or_insert(0) += 1;
                    mcp_server_tool_sets
                        .entry(parsed_mcp.server_id.clone())
                        .or_default()
                        .insert(parsed_mcp.tool_name.clone());
                    *mcp_tools
                        .entry((
                            tool_name.clone(),
                            parsed_mcp.server_id.clone(),
                            parsed_mcp.tool_name.clone(),
                        ))
                        .or_insert(0) += 1;
                    continue;
                }

                let canonical_name = canonical_tool_name(&tool_name);
                if canonical_name == "load_skill" {
                    skill_load_calls += 1;
                    session_has_skill_load = true;
                    if let Some(skill_id) = parse_skill_id(&tool_call.function.arguments) {
                        *skills.entry(skill_id).or_insert(0) += 1;
                    }
                    continue;
                }

                core_tool_calls += 1;
                *core_tools.entry(canonical_name).or_insert(0) += 1;
            }
        }

        if session_has_skill_load {
            sessions_with_skill_loads.insert(entry.id.clone());
        }
        if session_has_mcp_call {
            sessions_with_mcp_calls.insert(entry.id.clone());
        }
    }

    let unique_skills = skills.len() as u64;
    let unique_mcp_servers = mcp_servers.len() as u64;
    let unique_mcp_tools = mcp_tools.len() as u64;

    let mut top_core_tools: Vec<UsageCountItem> = core_tools
        .into_iter()
        .map(|(name, count)| UsageCountItem { name, count })
        .collect();
    top_core_tools.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.name.cmp(&right.name))
    });
    top_core_tools.truncate(10);

    let mut top_skills: Vec<SkillUsageItem> = skills
        .into_iter()
        .map(|(skill_id, count)| SkillUsageItem { skill_id, count })
        .collect();
    top_skills.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.skill_id.cmp(&right.skill_id))
    });
    top_skills.truncate(10);

    let mut top_mcp_servers: Vec<McpServerUsageItem> = mcp_servers
        .into_iter()
        .map(|(server_id, count)| McpServerUsageItem {
            unique_tools: mcp_server_tool_sets
                .get(&server_id)
                .map(|items| items.len() as u64)
                .unwrap_or(0),
            server_id,
            count,
        })
        .collect();
    top_mcp_servers.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.server_id.cmp(&right.server_id))
    });
    top_mcp_servers.truncate(10);

    let mut top_mcp_tools: Vec<McpToolUsageItem> = mcp_tools
        .into_iter()
        .map(|((alias, server_id, tool_name), count)| McpToolUsageItem {
            alias,
            server_id,
            tool_name,
            count,
        })
        .collect();
    top_mcp_tools.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.server_id.cmp(&right.server_id))
            .then_with(|| left.tool_name.cmp(&right.tool_name))
    });
    top_mcp_tools.truncate(12);

    HttpResponse::Ok().json(MetricsUsageBreakdownResponse {
        total_sessions: matched_session_ids.len() as u64,
        total_tool_calls,
        core_tool_calls,
        skill_load_calls,
        mcp_calls,
        unique_skills,
        unique_mcp_servers,
        unique_mcp_tools,
        sessions_with_skill_loads: sessions_with_skill_loads.len() as u64,
        sessions_with_mcp_calls: sessions_with_mcp_calls.len() as u64,
        top_core_tools,
        top_skills,
        top_mcp_servers,
        top_mcp_tools,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_mcp_tool_alias;

    #[test]
    fn parses_v1_canonical_alias_as_secret_safe_mcp_pseudonyms() {
        let alias =
            bamboo_mcp::ToolIndex::new().generate_alias("secret-server-label", "secret-tool-label");
        let (server_tag, owner_tag) = alias
            .strip_prefix("mcp__v1_")
            .and_then(|tags| tags.split_once('_'))
            .unwrap();
        let parsed =
            parse_mcp_tool_alias(&alias).expect("canonical v1 aliases remain in MCP metrics");

        assert_eq!(parsed.server_id, format!("v1_{server_tag}"));
        assert_eq!(parsed.tool_name, owner_tag);
        assert!(!parsed.server_id.contains("secret"));
        assert!(!parsed.tool_name.contains("secret"));
    }

    #[test]
    fn v1_metrics_group_same_server_and_separate_distinct_servers() {
        let index = bamboo_mcp::ToolIndex::new();
        let first = parse_mcp_tool_alias(&index.generate_alias("server-a", "tool-one")).unwrap();
        let sibling = parse_mcp_tool_alias(&index.generate_alias("server-a", "tool-two")).unwrap();
        let other = parse_mcp_tool_alias(&index.generate_alias("server-b", "tool-one")).unwrap();

        assert_eq!(first.server_id, sibling.server_id);
        assert_ne!(first.tool_name, sibling.tool_name);
        assert_ne!(first.server_id, other.server_id);
        assert_ne!(first.tool_name, other.tool_name);
    }

    #[test]
    fn preserves_legacy_alias_parsing_and_rejects_malformed_v1_names() {
        let legacy = parse_mcp_tool_alias("mcp__filesystem__read_file").unwrap();
        assert_eq!(legacy.server_id, "filesystem");
        assert_eq!(legacy.tool_name, "read_file");

        let legacy_v1_prefix = parse_mcp_tool_alias("mcp__v1_server__tool").unwrap();
        assert_eq!(legacy_v1_prefix.server_id, "v1_server");
        assert_eq!(legacy_v1_prefix.tool_name, "tool");

        assert!(parse_mcp_tool_alias("mcp__v1_too_short").is_none());
        assert!(
            parse_mcp_tool_alias(&format!("mcp__v1_{}_{}", "a".repeat(26), "0".repeat(26)))
                .is_none()
        );
    }
}
