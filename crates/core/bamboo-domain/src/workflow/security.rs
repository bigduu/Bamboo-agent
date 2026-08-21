//! Shared fail-closed credential classification for Workflow values.

use serde_json::Value;

use super::ValueRef;

/// Reject raw credential-shaped material while permitting the orchestration
/// engine's explicit `{ "$secret": "name" }` reference contract.
pub fn reject_secret_material(value: &Value) -> Result<(), String> {
    reject_secret_material_inner(value, false, true)
}

/// Reject credential material in a Workflow definition while permitting typed
/// non-literal bindings and explicit secret references resolved by the
/// orchestration runtime.
pub fn reject_secret_material_in_definition(value: &Value) -> Result<(), String> {
    reject_secret_material_inner(value, true, true)
}

/// Instruction Workflow arguments are persisted and injected into a prompt;
/// that path has no secret-reference resolver, so both raw values and opaque
/// handles fail closed.
pub fn reject_instruction_secret_material(value: &Value) -> Result<(), String> {
    reject_secret_material_inner(value, false, false)
}

fn normalized_credential_key(key: &str) -> String {
    key.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn public_token_metadata_key(normalized: &str) -> bool {
    matches!(
        normalized,
        "budgettokens"
            | "completiontokens"
            | "contextwindowtokens"
            | "inputtokens"
            | "maxcompletiontokens"
            | "maxcontexttokens"
            | "maxinputtokens"
            | "maxoutputtokens"
            | "maxprompttokens"
            | "maxtokens"
            | "maxtotaltokens"
            | "outputtokens"
            | "prompttokens"
            | "totaltokens"
    )
}

fn secret_bearing_key(key: &str) -> bool {
    let normalized = normalized_credential_key(key);
    matches!(
        normalized.as_str(),
        "auth"
            | "authentication"
            | "authorization"
            | "bearer"
            | "cookie"
            | "cookies"
            | "credential"
            | "credentials"
            | "oauth"
            | "oauth2"
            | "password"
            | "passwords"
            | "passphrase"
            | "passphrases"
            | "privatekey"
            | "privatekeys"
            | "secret"
            | "secrets"
            | "token"
            | "tokens"
    ) || normalized.contains("apikey")
        || normalized.ends_with("accesskey")
        || normalized.ends_with("accesskeys")
        || normalized.ends_with("authkey")
        || normalized.ends_with("authkeys")
        || normalized.ends_with("clientsecret")
        || normalized.ends_with("clientsecrets")
        || normalized.ends_with("clientsecretref")
        || normalized.ends_with("clientsecretreference")
        || normalized.ends_with("credential")
        || normalized.ends_with("credentials")
        || normalized.ends_with("credentialref")
        || normalized.ends_with("credentialrefs")
        || normalized.ends_with("credentialreference")
        || normalized.ends_with("credentialreferences")
        || normalized.ends_with("devicekey")
        || normalized.ends_with("devicekeys")
        || normalized.ends_with("encryptionkey")
        || normalized.ends_with("encryptionkeys")
        || normalized.ends_with("password")
        || normalized.ends_with("passwords")
        || normalized.ends_with("passphrase")
        || normalized.ends_with("passphrases")
        || normalized.ends_with("privatekey")
        || normalized.ends_with("privatekeys")
        || normalized.ends_with("privatekeyref")
        || normalized.ends_with("privatekeyreference")
        || normalized.ends_with("secret")
        || normalized.ends_with("secrets")
        || normalized.ends_with("secretref")
        || normalized.ends_with("secretrefs")
        || normalized.ends_with("secretreference")
        || normalized.ends_with("secretreferences")
        || normalized.ends_with("secretkey")
        || normalized.ends_with("secretkeys")
        || normalized.ends_with("signingkey")
        || normalized.ends_with("signingkeys")
        || normalized.ends_with("token")
        || normalized.ends_with("tokenref")
        || normalized.ends_with("tokenrefs")
        || normalized.ends_with("tokenreference")
        || normalized.ends_with("tokenreferences")
        || (normalized.ends_with("tokens") && !public_token_metadata_key(&normalized))
}

fn string_looks_like_secret(value: &str) -> bool {
    let trimmed = value.trim();
    let bearer = trimmed
        .get(.."Bearer ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("Bearer "));
    let basic = trimmed
        .get(.."Basic ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("Basic "));
    trimmed.starts_with("capability://")
        || bearer
        || basic
        || trimmed.starts_with("sk-")
        || trimmed.starts_with("ghp_")
        || trimmed.starts_with("github_pat_")
        || trimmed.starts_with("glpat-")
        || trimmed.starts_with("hf_")
        || trimmed.starts_with("xoxb-")
        || trimmed.starts_with("xoxp-")
        || trimmed.starts_with("AIza")
        || (trimmed.starts_with("AKIA") && trimmed.len() >= 16)
        || trimmed.starts_with("-----BEGIN PRIVATE KEY-----")
        || trimmed.starts_with("-----BEGIN RSA PRIVATE KEY-----")
        || trimmed.starts_with("-----BEGIN OPENSSH PRIVATE KEY-----")
}

fn reject_secret_material_inner(
    value: &Value,
    allow_bindings: bool,
    allow_secret_references: bool,
) -> Result<(), String> {
    fn walk(
        value: &Value,
        key: Option<&str>,
        allow_bindings: bool,
        allow_secret_references: bool,
    ) -> Result<(), String> {
        if value.as_object().is_some_and(|object| {
            object.len() == 1
                && object
                    .get("$secret")
                    .and_then(Value::as_str)
                    .is_some_and(|handle| !handle.trim().is_empty())
        }) {
            return if allow_secret_references {
                Ok(())
            } else {
                Err(
                    "opaque credential handles are not enabled for instruction Workflows"
                        .to_string(),
                )
            };
        }
        let safe_binding = allow_bindings
            && serde_json::from_value::<ValueRef>(value.clone())
                .is_ok_and(|reference| !matches!(reference, ValueRef::Literal { .. }));
        let typed_secret_schema_annotation = allow_bindings
            && key.is_some_and(|key| normalized_credential_key(key) == "xbamboosecret")
            && value.is_boolean();
        if key.is_some_and(secret_bearing_key) && !safe_binding && !typed_secret_schema_annotation {
            return Err("secret-bearing fields are not accepted by Workflows".to_string());
        }
        if value.as_str().is_some_and(string_looks_like_secret) {
            return Err("opaque credential handles are not enabled for Workflows".to_string());
        }
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if key == "properties" {
                        let properties = value.as_object().ok_or_else(|| {
                            "workflow schema properties must be an object".to_string()
                        })?;
                        for schema in properties.values() {
                            walk(schema, None, allow_bindings, allow_secret_references)?;
                        }
                    } else {
                        walk(value, Some(key), allow_bindings, allow_secret_references)?;
                    }
                }
            }
            Value::Array(array) => {
                for value in array {
                    walk(value, None, allow_bindings, allow_secret_references)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(value, None, allow_bindings, allow_secret_references)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instruction_arguments_reject_keys_prefixes_and_secret_references() {
        for value in [
            serde_json::json!({"api_key": "ordinary-looking"}),
            serde_json::json!({"nested": {"client_tokens": ["value"]}}),
            serde_json::json!({"X-Client-Tokens": ["value"]}),
            serde_json::json!({"secrets": {"primary": "value"}}),
            serde_json::json!({"credential_ref": "internal-provider-key"}),
            serde_json::json!({"secret_reference": "internal-secret-name"}),
            serde_json::json!({"value": "sk-secret"}),
            serde_json::json!({"value": "xoxb-secret"}),
            serde_json::json!({"value": "bEaReR opaque-value"}),
            serde_json::json!({"value": "basic opaque-value"}),
            serde_json::json!({"value": {"$secret": "provider-key"}}),
        ] {
            assert!(reject_instruction_secret_material(&value).is_err());
        }
        assert!(reject_instruction_secret_material(&serde_json::json!({
            "focus": "security",
            "paths": ["src/lib.rs"],
            "max_tokens": 4096,
            "input_tokens": 128
        }))
        .is_ok());
    }

    #[test]
    fn definitions_allow_only_the_typed_secret_schema_annotation() {
        assert!(reject_secret_material_in_definition(&serde_json::json!({
            "input_schema": {
                "type": "object",
                "properties": {
                    "credential": {
                        "type": "object",
                        "x-bamboo-secret": true
                    }
                }
            }
        }))
        .is_ok());
        assert!(reject_secret_material_in_definition(&serde_json::json!({
            "x-bamboo-secret": "raw-secret"
        }))
        .is_err());
        assert!(reject_instruction_secret_material(&serde_json::json!({
            "x-bamboo-secret": true
        }))
        .is_err());
    }
}
