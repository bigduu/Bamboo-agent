use std::collections::BTreeSet;

use serde::de::{Deserializer, Visitor};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{NamedAgentDefinition, NamedAgentDiagnosticCode as Code, NamedAgentLimits};

const MAX_FRONTMATTER_BYTES: usize = 16_384;
const MAX_DESCRIPTION_BYTES: usize = 512;
const MAX_TOOL_DECLARATIONS: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    schema_version: u32,
    #[serde(deserialize_with = "strict_string")]
    name: String,
    #[serde(deserialize_with = "strict_string")]
    description: String,
    #[serde(default, deserialize_with = "strict_optional_string")]
    model_hint: Option<String>,
    #[serde(default)]
    tools: ToolDeclarations,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolDeclarations {
    #[serde(default, deserialize_with = "strict_string_list")]
    allow: Vec<String>,
    #[serde(default, deserialize_with = "strict_string_list")]
    deny: Vec<String>,
}

// serde_yaml's native String deserializer coerces numeric/bool scalars to text.
// Use deserialize_any with only string visitors to enforce the v1 field types.
fn strict_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    struct StringVisitor;
    impl<'de> Visitor<'de> for StringVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_owned())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }
    }
    deserializer.deserialize_any(StringVisitor)
}

struct StrictString(String);

impl<'de> Deserialize<'de> for StrictString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        strict_string(deserializer).map(Self)
    }
}

fn strict_optional_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<StrictString>::deserialize(deserializer).map(|value| value.map(|v| v.0))
}

fn strict_string_list<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    Vec::<StrictString>::deserialize(deserializer)
        .map(|values| values.into_iter().map(|v| v.0).collect())
}

pub(super) fn parse(bytes: &[u8], limits: NamedAgentLimits) -> Result<NamedAgentDefinition, Code> {
    let text = std::str::from_utf8(bytes).map_err(|_| Code::InvalidUtf8)?;
    if contains_credential_material(text) {
        return Err(Code::CredentialMaterial);
    }
    let normalized = text.replace("\r\n", "\n");
    let remainder = normalized
        .strip_prefix("---\n")
        .ok_or(Code::MissingFrontmatter)?;
    let (header, body) = remainder
        .split_once("\n---\n")
        .ok_or(Code::MissingFrontmatter)?;
    if header.len() > MAX_FRONTMATTER_BYTES || unsupported_yaml_features(header) {
        return Err(Code::MalformedFrontmatter);
    }
    // Typed deserialization rejects duplicate/unknown fields and non-scalar
    // values without copying parser errors or source snippets into diagnostics.
    let header: Frontmatter =
        serde_yaml::from_str(header).map_err(|_| Code::MalformedFrontmatter)?;
    // YAML quoted scalars decode escapes. Scan every decoded value too, before
    // publishing even its otherwise safe name/description as metadata.
    if [header.name.as_str(), header.description.as_str()]
        .into_iter()
        .chain(header.model_hint.as_deref())
        .chain(header.tools.allow.iter().map(String::as_str))
        .chain(header.tools.deny.iter().map(String::as_str))
        .any(contains_credential_material)
    {
        return Err(Code::CredentialMaterial);
    }
    if header.schema_version != 1 {
        return Err(Code::UnsupportedSchemaVersion);
    }
    if !valid_name(&header.name) {
        return Err(Code::InvalidName);
    }
    if !valid_description(&header.description) {
        return Err(Code::InvalidDescription);
    }
    if header
        .model_hint
        .as_deref()
        .is_some_and(|hint| !valid_identifier(hint))
    {
        return Err(Code::InvalidModelHint);
    }
    let allow = header.tools.allow.iter().collect::<BTreeSet<_>>();
    let deny = header.tools.deny.iter().collect::<BTreeSet<_>>();
    if header.tools.allow.len() > MAX_TOOL_DECLARATIONS
        || header.tools.deny.len() > MAX_TOOL_DECLARATIONS
        || allow.len() != header.tools.allow.len()
        || deny.len() != header.tools.deny.len()
        || !allow.is_disjoint(&deny)
        || allow
            .iter()
            .chain(deny.iter())
            .any(|name| !valid_identifier(name))
    {
        return Err(Code::InvalidToolDeclarations);
    }
    let body = body.trim();
    if body.is_empty() {
        return Err(Code::EmptyPrompt);
    }
    if body.len() > limits.max_prompt_bytes {
        return Err(Code::PromptTooLarge);
    }
    Ok(NamedAgentDefinition {
        name: header.name,
        description: header.description,
        revision: hex::encode(Sha256::digest(bytes)),
        model_hint: header.model_hint,
        tool_allow: header.tools.allow,
        tool_deny: header.tools.deny,
        system_prompt: body.to_owned(),
    })
}

fn valid_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

fn valid_identifier(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

fn valid_description(description: &str) -> bool {
    !description.is_empty()
        && description.len() <= MAX_DESCRIPTION_BYTES
        && description.trim() == description
        && !description.chars().any(|c| {
            c.is_control()
                || matches!(c, '/' | '\\' | '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
}

// Aliases, anchors and tags are unnecessary for this scalar/list schema. Reject
// unquoted YAML indicators before deserialization to bound alias expansion work.
fn unsupported_yaml_features(header: &str) -> bool {
    let mut chars = header.chars().peekable();
    let mut quote = None;
    let mut comment = false;
    let mut boundary = true;
    let mut quote_allowed = true;
    let mut line_start = true;
    let mut flow_depth = 0usize;
    let mut quoted_scalar_end = false;
    while let Some(c) = chars.next() {
        if comment {
            if c == '\n' {
                comment = false;
                boundary = true;
                line_start = true;
            }
            continue;
        }
        if let Some(current) = quote {
            if c == '\\' && current == '"' {
                chars.next();
            } else if c == '\'' && current == '\'' && chars.peek() == Some(&'\'') {
                chars.next(); // YAML single quotes escape themselves by doubling.
            } else if c == current {
                quote = None;
                boundary = false;
                quoted_scalar_end = true;
            }
            continue;
        }
        if c == '#' && boundary {
            comment = true;
            continue;
        }
        // A quote starts a quoted YAML scalar only at a value/flow/sequence
        // boundary, never inside an already started plain scalar (owner's).
        if quote_allowed && matches!(c, '"' | '\'') {
            quote = Some(c);
            quote_allowed = false;
            boundary = false;
            line_start = false;
            quoted_scalar_end = false;
            continue;
        }
        if boundary && matches!(c, '&' | '*' | '!') {
            return true;
        }
        if c == '\n' {
            boundary = true;
            line_start = true;
            continue;
        }
        if c.is_whitespace() {
            boundary = true;
            continue;
        }
        let sequence_start =
            c == '-' && line_start && chars.peek().is_some_and(|next| next.is_whitespace());
        let mapping_value = c == ':'
            && (chars.peek().is_none_or(|next| next.is_whitespace())
                || (flow_depth > 0
                    && (quoted_scalar_end
                        || chars.peek().is_some_and(|next| matches!(next, '[' | '{')))));
        let flow_start = matches!(c, '[' | '{') && quote_allowed;
        let flow_separator = c == ',' && flow_depth > 0;
        if flow_start {
            flow_depth += 1;
        } else if matches!(c, ']' | '}') && flow_depth > 0 {
            flow_depth -= 1;
        }
        // Commas/brackets/colon-without-space in a block plain scalar are
        // ordinary text, and must not enable a following apostrophe as a quote.
        quote_allowed = flow_start || flow_separator || mapping_value || sequence_start;
        boundary = quote_allowed;
        line_start = false;
        quoted_scalar_end = false;
    }
    false
}

fn credential_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "apikey"
            | "accesstoken"
            | "refreshtoken"
            | "bearertoken"
            | "token"
            | "secret"
            | "secretkey"
            | "clientsecret"
            | "password"
            | "credentials"
            | "credential"
            | "authorization"
            | "privatekey"
    )
}

fn contains_credential_material(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    if lower
        .lines()
        .any(|line| line.contains("-----begin ") && line.contains("private key"))
    {
        return true;
    }
    for (index, separator) in text.char_indices() {
        if !matches!(separator, ':' | '=') {
            continue;
        }
        let before = text[..index].trim_end_matches([' ', '\t', '"', '\'', '`']);
        let key = before
            .rsplit(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-')))
            .next()
            .unwrap_or_default();
        if credential_key(key) {
            return true;
        }
    }
    // Recognizable token families only. Arbitrary prose secrets are not claimed
    // to be detectable. This scans all fields and the prompt, not just YAML keys.
    text.split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-')))
        .any(|token| {
            (token.starts_with("sk-") && token.len() >= 23)
                || (token.starts_with("ghp_") && token.len() >= 40)
                || (token.starts_with("github_pat_") && token.len() >= 40)
                || (token.starts_with("AKIA") && token.len() == 20)
                || (["xoxb-", "xoxp-", "xoxa-", "xoxr-", "xoxs-"]
                    .iter()
                    .any(|prefix| token.starts_with(prefix))
                    && token.len() >= 20)
        })
}
