//! Client tool ID normalization from LiteLLM c3cafa3782642685ef10777e4c5931400c0a7176.
//! Bamboo assigns colliding client IDs in message order to preserve the
//! cacheable history prefix and updates calls and their results together.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

pub(crate) fn normalize_claude_tool_ids(body: &mut Value, official_anthropic: bool) {
    let claude_model = body["model"].as_str().is_some_and(|model| {
        let model = model.strip_prefix("anthropic/").unwrap_or(model);
        model.starts_with("claude-")
    });
    if !official_anthropic && !claude_model {
        return;
    }
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };

    let blocks = || {
        messages
            .iter()
            .filter_map(|message| message.get("content").and_then(Value::as_array))
            .flatten()
    };
    // Provider-owned server tool IDs and their references stay intact.
    let mut occupied: HashSet<_> = blocks()
        .filter(|block| block["type"] == "server_tool_use")
        .filter_map(|block| block["id"].as_str().map(str::to_owned))
        .collect();
    let mut replacements = HashMap::new();
    let mut next_suffix = HashMap::new();
    for block in blocks() {
        let Some(id) = client_id_field(block).and_then(|field| block[field].as_str()) else {
            continue;
        };
        if replacements.contains_key(id) {
            continue;
        }
        let base = normalize_id(id);
        let replacement = if occupied.insert(base.clone()) {
            base
        } else {
            let suffix = next_suffix.entry(base.clone()).or_insert(1_usize);
            loop {
                let candidate = format!("{base}-{suffix}");
                *suffix += 1;
                if occupied.insert(candidate.clone()) {
                    break candidate;
                }
            }
        };
        replacements.insert(id.to_owned(), replacement);
    }

    for block in messages
        .iter_mut()
        .filter_map(|message| message.get_mut("content").and_then(Value::as_array_mut))
        .flatten()
    {
        let Some(id) = client_id_field(block).and_then(|field| block.get_mut(field)) else {
            continue;
        };
        if let Some(replacement) = id.as_str().and_then(|id| replacements.get(id)) {
            *id = Value::String(replacement.clone());
        }
    }
}

fn client_id_field(block: &Value) -> Option<&'static str> {
    match block["type"].as_str() {
        Some("tool_use") => Some("id"),
        Some("tool_result") => Some("tool_use_id"),
        _ => None,
    }
}

fn normalize_id(id: &str) -> String {
    let base = id.split_once("__thought__").map_or(id, |(base, _)| base);
    if base.is_empty() {
        return "tool_use_id".into();
    }
    base.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{normalize_claude_tool_ids, normalize_id};
    use serde_json::json;

    #[test]
    fn client_ids_follow_pinned_character_and_suffix_rules() {
        for (id, expected) in [
            ("functions.lookup:0", "functions_lookup_0"),
            ("call_1__thought__a__thought__b", "call_1"),
            ("toolu_01-A_b", "toolu_01-A_b"),
            ("café", "caf_"),
            ("::", "__"),
            ("", "tool_use_id"),
            ("__thought__CiIB", "tool_use_id"),
        ] {
            assert_eq!(normalize_id(id), expected);
        }
    }

    #[test]
    fn appended_collisions_preserve_already_normalized_history() {
        let pair = |id| {
            [
                json!({"role":"assistant","content":[{"type":"tool_use","id":id}]}),
                json!({"role":"user","content":[{"type":"tool_result","tool_use_id":id}]}),
            ]
        };
        let mut original = json!({"model":"claude-opus-4-6","messages":pair("history:lookup")});
        let mut extended = original.clone();
        let messages = extended["messages"].as_array_mut().unwrap();
        messages.extend(pair("history.lookup"));
        messages.extend(pair("history_lookup"));

        normalize_claude_tool_ids(&mut original, true);
        normalize_claude_tool_ids(&mut extended, true);
        let messages = extended["messages"].as_array().unwrap();
        assert_eq!(&messages[..2], original["messages"].as_array().unwrap());
        for (call, result, id) in [
            (0, 1, "history_lookup"),
            (2, 3, "history_lookup-1"),
            (4, 5, "history_lookup-2"),
        ] {
            assert_eq!(messages[call]["content"][0]["id"], id);
            assert_eq!(messages[result]["content"][0]["tool_use_id"], id);
        }
    }
}
