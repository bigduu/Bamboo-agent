//! Last-moment outbound-request masking.
//!
//! Keyword masking is applied as a single, field-agnostic SCAN of the fully
//! serialized request body ([`serde_json::Value`]) right before it leaves the
//! process — the last moment "on the way out". Masking text field-by-field on the
//! structured request is whack-a-mole (it has historically missed tool-call
//! arguments, reasoning, etc.); scanning the final body instead catches EVERY text
//! value — `content`, tool-call `arguments`, `instructions`, `input`, tool
//! results — regardless of which field or wire format (Chat Completions, Responses,
//! Anthropic, Gemini) carries it, and automatically covers any future field.
//!
//! To never corrupt the wire, string values under [`STRUCTURAL_KEYS`] (identifiers,
//! enums, names, signatures, opaque blobs the protocol matches on) are skipped — so
//! tool-call correlation (`call_id`), routing (`model`), dispatch (`name`, `type`,
//! `role`), thinking verification (`signature`), and encrypted/binary payloads stay
//! intact. The scan is idempotent and a no-op when masking is disabled.

use bamboo_config::KeywordMaskingConfig;
use serde_json::Value;

/// JSON object keys whose direct string VALUES are structural — identifiers,
/// enums, names, signatures, opaque/binary blobs the wire protocol matches on.
/// Masking these would break tool-call correlation, routing, role/type dispatch,
/// thinking-signature verification, or corrupt base64/encrypted payloads, so their
/// direct string value is never masked. (Nested objects/arrays under such a key are
/// still scanned normally — only the immediate string is exempt.)
///
/// ASSUMPTION: these positions carry protocol tokens (IDs, enums, names, MIME
/// types, URIs, base64) — NOT user free text. A secret deliberately embedded in one
/// of them (e.g. a credential in a `url`/`fileUri` query string) therefore ships
/// UNMASKED; that is an accepted trade-off (masking these would corrupt the wire).
/// Both snake_case (OpenAI/Anthropic) and camelCase (Gemini) spellings are listed so
/// the exemption is consistent across wire formats.
const STRUCTURAL_KEYS: &[&str] = &[
    // Identifiers / correlation
    "id",
    "call_id",
    "tool_call_id",
    "item_id",
    "response_id",
    "previous_response_id",
    // Enums / structure
    "type",
    "role",
    "object",
    "finish_reason",
    "status",
    "index",
    // Routing / dispatch
    "model",
    "name",
    "url",
    "fileUri", // Gemini file URI (aligns with `url`)
    // Opaque / verification / binary
    "signature",
    "encrypted_content",
    "data",
    "media_type",
    "mimeType", // Gemini MIME type (aligns with `media_type`)
];

/// Mask keyword matches in every non-structural string value of an outbound request
/// `body`, in place. No-op when `config` has no enabled entries. See the module doc.
pub fn mask_outbound_body(body: &mut Value, config: &KeywordMaskingConfig) {
    if config.entries.is_empty() {
        return;
    }
    mask_value(body, config, false);
}

fn mask_value(value: &mut Value, config: &KeywordMaskingConfig, under_structural_key: bool) {
    match value {
        // A string directly under a structural key is exempt (falls through to `_`).
        Value::String(text) if !under_structural_key => {
            let masked = config.apply_masking(text);
            if masked != *text {
                *text = masked;
            }
        }
        Value::Array(items) => {
            // Array elements carry no key context — always scanned.
            for item in items {
                mask_value(item, config, false);
            }
        }
        Value::Object(map) => {
            for (key, val) in map.iter_mut() {
                // A structural key exempts only its DIRECT string value; nested
                // structures under it are still scanned (with their own key context).
                let structural = STRUCTURAL_KEYS.contains(&key.as_str());
                mask_value(val, config, structural);
            }
        }
        // Numbers / bools / null carry no text.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::keyword_masking::{KeywordEntry, MatchType};
    use serde_json::json;

    fn config(pattern: &str) -> KeywordMaskingConfig {
        KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: pattern.to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        }
    }

    #[test]
    fn masks_content_and_tool_call_arguments_but_not_structural_fields() {
        // A Responses-style input array carrying a secret in BOTH message content
        // and a function_call's arguments — the field the old masking missed.
        let mut body = json!({
            "model": "gpt-secret-5",
            "instructions": "system has a secret",
            "input": [
                { "type": "message", "role": "user", "content": "a secret here" },
                {
                    "type": "function_call",
                    "call_id": "call_secret_1",
                    "name": "secret_tool",
                    "arguments": "{\"q\":\"the secret value\"}"
                },
                { "type": "function_call_output", "call_id": "call_secret_1", "output": "secret output" }
            ]
        });
        mask_outbound_body(&mut body, &config("secret"));

        // Structural fields preserved (correlation, routing, dispatch, enums).
        assert_eq!(body["model"], "gpt-secret-5"); // model id untouched
        assert_eq!(body["input"][1]["call_id"], "call_secret_1");
        assert_eq!(body["input"][2]["call_id"], "call_secret_1");
        assert_eq!(body["input"][1]["name"], "secret_tool"); // tool name untouched
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["input"][1]["type"], "function_call");
        // Text values masked everywhere they appear.
        assert_eq!(body["instructions"], "system has a [MASKED]");
        assert_eq!(body["input"][0]["content"], "a [MASKED] here");
        assert_eq!(
            body["input"][1]["arguments"],
            "{\"q\":\"the [MASKED] value\"}"
        );
        assert_eq!(body["input"][2]["output"], "[MASKED] output");
    }

    #[test]
    fn masks_typed_content_array_text_but_not_image_url_or_signature() {
        let mut body = json!({
            "messages": [{
                "role": "assistant",
                "content": [
                    { "type": "text", "text": "secret text" },
                    { "type": "image_url", "image_url": { "url": "https://x/secret.png" } }
                ],
                "tool_calls": [{
                    "id": "tc_1",
                    "type": "function",
                    "function": { "name": "secret_fn", "arguments": "secret args" }
                }]
            }],
            "thinking": { "type": "thinking", "signature": "secret-signature-blob" }
        });
        mask_outbound_body(&mut body, &config("secret"));

        assert_eq!(body["messages"][0]["content"][0]["text"], "[MASKED] text");
        // url under structural key "url" untouched (don't corrupt the link).
        assert_eq!(
            body["messages"][0]["content"][1]["image_url"]["url"],
            "https://x/secret.png"
        );
        // tool_calls: arguments masked, id/name preserved.
        assert_eq!(body["messages"][0]["tool_calls"][0]["id"], "tc_1");
        assert_eq!(
            body["messages"][0]["tool_calls"][0]["function"]["name"],
            "secret_fn"
        );
        assert_eq!(
            body["messages"][0]["tool_calls"][0]["function"]["arguments"],
            "[MASKED] args"
        );
        // thinking signature (verification blob) untouched.
        assert_eq!(body["thinking"]["signature"], "secret-signature-blob");
    }

    #[test]
    fn is_idempotent_and_noop_when_disabled() {
        let mut body = json!({ "content": "a secret" });
        let disabled = KeywordMaskingConfig::default();
        mask_outbound_body(&mut body, &disabled);
        assert_eq!(body["content"], "a secret", "no entries → no-op");

        let cfg = config("secret");
        mask_outbound_body(&mut body, &cfg);
        assert_eq!(body["content"], "a [MASKED]");
        // Second pass changes nothing (the keyword is already gone).
        mask_outbound_body(&mut body, &cfg);
        assert_eq!(body["content"], "a [MASKED]");
    }

    #[test]
    fn gemini_camelcase_structural_keys_are_exempt_like_snake_case() {
        // Gemini serializes to camelCase; mimeType/fileUri are the binary/URI routing
        // tokens that align with OpenAI/Anthropic's media_type/url and must be exempt
        // (matching across wire formats), while surrounding text is still masked.
        let mut body = json!({
            "contents": [{
                "role": "user",
                "parts": [
                    { "text": "a secret prompt" },
                    { "inlineData": { "mimeType": "secret/type", "data": "c2VjcmV0" } },
                    { "fileData": { "mimeType": "x/secret", "fileUri": "gs://b/secret.png" } }
                ]
            }]
        });
        mask_outbound_body(&mut body, &config("secret"));
        let part = |i: usize| &body["contents"][0]["parts"][i];
        assert_eq!(part(0)["text"], "a [MASKED] prompt"); // free text masked
        assert_eq!(part(1)["inlineData"]["mimeType"], "secret/type"); // exempt
        assert_eq!(part(1)["inlineData"]["data"], "c2VjcmV0"); // base64 exempt
        assert_eq!(part(2)["fileData"]["mimeType"], "x/secret"); // exempt
        assert_eq!(part(2)["fileData"]["fileUri"], "gs://b/secret.png"); // exempt
    }

    #[test]
    fn documented_residual_keyword_equal_to_a_json_key_inside_arguments_rewrites_the_key() {
        // LOCKS the one documented residual (flagged by review): `arguments` is a
        // JSON-encoded STRING masked as opaque text, so a keyword that collides with a
        // JSON key inside it rewrites that key — corrupting the args. This pins the
        // CURRENT behavior so any future change (e.g. JSON-aware masking) is deliberate.
        let mut body = json!({
            "input": [{
                "type": "function_call",
                "call_id": "c1",
                "name": "search",
                "arguments": "{\"q\":\"hello\"}"
            }]
        });
        // A pathological keyword equal to the JSON key `q`.
        mask_outbound_body(&mut body, &config("q"));
        assert_eq!(
            body["input"][0]["arguments"], "{\"[MASKED]\":\"hello\"}",
            "documented residual: keyword==JSON key corrupts tool-arg JSON (accepted trade-off)"
        );
    }
}
