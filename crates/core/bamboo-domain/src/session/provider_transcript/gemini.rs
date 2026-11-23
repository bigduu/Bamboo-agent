use std::collections::HashSet;

use serde_json::{Map, Value};

use super::{
    is_sha256_hex, reject_unknown_fields, ProviderTranscriptAuthor, ProviderTranscriptError,
    ProviderTranscriptItemKind, ProviderTranscriptOrigin,
};

pub(super) fn validate_model_content(
    item_type: &str,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    object: &Map<String, Value>,
) -> Result<ProviderTranscriptItemKind, ProviderTranscriptError> {
    if item_type != "gemini_model_content" {
        return Err(ProviderTranscriptError::UnsupportedItemType);
    }
    if (origin, author)
        != (
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
        )
    {
        return Err(ProviderTranscriptError::InvalidItem(
            "Gemini model authority",
        ));
    }
    reject_unknown_fields(
        object,
        &["type", "scope_sha256", "parts", "tool_call_ids"],
        "Gemini model content fields",
    )?;
    if !object
        .get("scope_sha256")
        .and_then(Value::as_str)
        .is_some_and(is_sha256_hex)
    {
        return Err(ProviderTranscriptError::InvalidItem("Gemini signing scope"));
    }
    let parts = object
        .get("parts")
        .and_then(Value::as_array)
        .ok_or(ProviderTranscriptError::InvalidItem("Gemini parts"))?;
    let ids = object
        .get("tool_call_ids")
        .and_then(Value::as_array)
        .ok_or(ProviderTranscriptError::InvalidItem("Gemini tool ids"))?;
    let mut signed = false;
    let mut calls = 0;
    let mut wire_ids = HashSet::new();
    for part in parts {
        let part = part
            .as_object()
            .ok_or(ProviderTranscriptError::InvalidItem("Gemini part"))?;
        reject_unknown_fields(
            part,
            &["text", "thought", "thoughtSignature", "functionCall"],
            "Gemini part fields",
        )?;
        if let Some(signature) = part.get("thoughtSignature") {
            if !nonempty_string(signature) {
                return Err(ProviderTranscriptError::InvalidItem(
                    "Gemini thought signature",
                ));
            }
            signed = true;
        }
        if part.get("thought").is_some_and(|value| !value.is_boolean()) {
            return Err(ProviderTranscriptError::InvalidItem("Gemini thought flag"));
        }
        match (part.get("text"), part.get("functionCall")) {
            (Some(text), None) if text.is_string() => {}
            (None, Some(call)) if part.get("thought") != Some(&Value::Bool(true)) => {
                let call = call
                    .as_object()
                    .ok_or(ProviderTranscriptError::InvalidItem("Gemini function call"))?;
                reject_unknown_fields(call, &["id", "name", "args"], "Gemini function fields")?;
                if !call.get("name").is_some_and(nonempty_string)
                    || call.get("args").is_some_and(|args| !args.is_object())
                {
                    return Err(ProviderTranscriptError::InvalidItem("Gemini function call"));
                }
                if let Some(id) = call.get("id") {
                    let id = id
                        .as_str()
                        .filter(|id| !id.trim().is_empty())
                        .ok_or(ProviderTranscriptError::InvalidItem("Gemini function id"))?;
                    if !wire_ids.insert(id) {
                        return Err(ProviderTranscriptError::InvalidItem(
                            "Gemini duplicate function id",
                        ));
                    }
                }
                calls += 1;
            }
            _ => return Err(ProviderTranscriptError::InvalidItem("Gemini part payload")),
        }
    }
    let mut normalized_ids = HashSet::new();
    if !signed
        || calls != ids.len()
        || ids
            .iter()
            .any(|id| !nonempty_string(id) || !normalized_ids.insert(id.as_str()))
    {
        return Err(ProviderTranscriptError::InvalidItem(
            "Gemini signed content or tool bindings",
        ));
    }
    Ok(ProviderTranscriptItemKind::GeminiModelContent)
}

fn nonempty_string(value: &Value) -> bool {
    value.as_str().is_some_and(|text| !text.trim().is_empty())
}
