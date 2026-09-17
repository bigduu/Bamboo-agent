//! Privacy boundary for AutoDream durable extraction.
//!
//! The extractor is intentionally conservative: a source item containing a
//! credential-like value is replaced as a whole before provider dispatch, and
//! a model candidate containing the same shapes is rejected before either
//! durable sink sees it. This module never logs the rejected value.

use std::collections::HashSet;
use std::sync::OnceLock;

use bamboo_memory::auto_dream::{DurableExtractionCandidate, LedgerExtractionCandidate};
use regex::Regex;

pub(crate) const REDACTED_EXTRACTION_SOURCE: &str =
    "[sensitive content omitted before durable-memory extraction]";

fn secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:(?:api[\s_-]?key|account[\s_-]?key|shared[\s_-]?access[\s_-]?(?:key|signature)|password|passwd|passcode|passphrase|otp|one[\s_-]?time[\s_-]?(?:password|passcode|code)|verification[\s_-]?code|security[\s_-]?code|recovery[\s_-]?code|mfa[\s_-]?code|2fa[\s_-]?code|credential|private[\s_-]?key|secret[\s_-]?key|client[\s_-]?secret|access[\s_-]?key|auth[\s_-]?key|signing[\s_-]?key|encryption[\s_-]?key|(?:basic|proxy|http)[\s_-]?auth|(?:api|auth|access|refresh|bearer)[\s_-]?token|session[\s_-]*(?:cookie|token|id))[\"']?\s*(?::|=)|cookie[\"']?\s*(?::|=))\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("secret assignment regex must compile")
    })
}

fn generic_secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)(?:^|[\s(\"'])(?:secret|token)[\"']?\s*(?::|=)\s*[\"']?[^\s\"',;}]+"#)
            .expect("generic secret assignment regex must compile")
    })
}

fn present_tense_secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:(?:api[\s_-]?key|account[\s_-]?key|shared[\s_-]?access[\s_-]?(?:key|signature)|password|passwd|passcode|passphrase|otp|one[\s_-]?time[\s_-]?(?:password|passcode|code)|verification[\s_-]?code|security[\s_-]?code|recovery[\s_-]?code|mfa[\s_-]?code|2fa[\s_-]?code|credential|private[\s_-]?key|secret[\s_-]?key|client[\s_-]?secret|access[\s_-]?key|auth[\s_-]?key|signing[\s_-]?key|encryption[\s_-]?key|(?:basic|proxy|http)[\s_-]?auth|(?:api|auth|access|refresh|bearer)[\s_-]?token|session[\s_-]?(?:cookie|token|id))|(?:my|our|your)\s+(?:secret|token|pin)|(?:account|auth|authentication|login|security|verification|recovery|mfa|2fa|bank|card|payment|unlock|device)[\s_-]+pin|pin[\s_-]+(?:code|number))\s+is\s+[\"']?(?P<value>[^\s\"',;}]+)"#,
        )
        .expect("present-tense secret assignment regex must compile")
    })
}

fn past_tense_secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:api[\s_-]?key|account[\s_-]?key|password|passwd|passcode|passphrase|credential|private[\s_-]?key|secret[\s_-]?key|client[\s_-]?secret|access[\s_-]?key|auth[\s_-]?key|signing[\s_-]?key|encryption[\s_-]?key|(?:api|auth|access|refresh|bearer|session)[\s_-]?token|session[\s_-]?(?:cookie|id))\s+was\s+[\"']?(?P<value>[^\s\"',;}]+)"#,
        )
        .expect("past-tense secret assignment regex must compile")
    })
}

fn is_credential_state_predicate(value: &str) -> bool {
    matches!(
        value,
        "changed"
            | "configured"
            | "encrypted"
            | "expired"
            | "forgotten"
            | "hashed"
            | "invalid"
            | "masked"
            | "not"
            | "optional"
            | "redacted"
            | "removed"
            | "required"
            | "reset"
            | "revoked"
            | "rotated"
            | "stored"
            | "updated"
            | "valid"
    )
}

fn captures_non_state_credential_value(pattern: &Regex, value: &str) -> bool {
    pattern.captures_iter(value).any(|captures| {
        let Some(candidate) = captures.name("value") else {
            return false;
        };
        let candidate = candidate
            .as_str()
            .trim()
            .trim_matches(|character: char| character.is_ascii_punctuation())
            .to_ascii_lowercase();
        !is_credential_state_predicate(&candidate)
    })
}

fn contains_present_tense_secret_assignment(value: &str) -> bool {
    captures_non_state_credential_value(present_tense_secret_assignment_pattern(), value)
}

fn contains_past_tense_secret_assignment(value: &str) -> bool {
    captures_non_state_credential_value(past_tense_secret_assignment_pattern(), value)
}

fn pin_credential_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:(?:my|our|your)\s+pin\s*(?::|=)|(?:account|auth|authentication|login|security|verification|recovery|mfa|2fa|bank|card|payment|unlock|device)[\s_-]+pin\s*(?::|=)|pin[\s_-]+(?:code|number)\s*(?::|=))\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("credential-context PIN assignment regex must compile")
    })
}

fn short_credential_config_field_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?im)(?:^[ \t]*(?:[-*][ \t]+)?|[,{][ \t]*)[\"']?(?:pwd|pass)[\"']?[ \t]*(?::|=)[ \t]*[\"'][^\"'\r\n]{1,1024}[\"']"#,
        )
        .expect("short credential config-field regex must compile")
    })
}

fn redis_password_directive_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?im)^[ \t]*(?:config[ \t]+set[ \t]+)?(?:requirepass|masterauth)[ \t]+(?:[\"'][^\"'\r\n]{1,1024}[\"']|[^\s#;\"']{1,1024})(?:[ \t]*(?:#.*)?)?$"#,
        )
        .expect("Redis password directive regex must compile")
    })
}

fn markdown_table_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?im)^[ \t]*\|[ \t]*(?:api[\s_-]?key|account[\s_-]?key|password|passwd|pwd|pass|passcode|passphrase|credential|private[\s_-]?key|secret(?:[\s_-]?key)?|client[\s_-]?secret|access[\s_-]?key|auth[\s_-]?key|signing[\s_-]?key|encryption[\s_-]?key|(?:api|auth|access|refresh|bearer|session)[\s_-]?token|session[\s_-]?(?:cookie|id)|pin)[ \t]*\|[ \t]*(?P<value>[^|\r\n]{1,1024}?)[ \t]*(?:\|[^\r\n]*)?$"#,
        )
        .expect("Markdown credential table regex must compile")
    })
}

fn standalone_pin_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?im)^[ \t]*(?:[-*][ \t]+)?pin[ \t]*(?::|=|\bis\b)[ \t]*[\"']?[0-9]{3,12}\b"#)
            .expect("standalone PIN credential regex must compile")
    })
}

fn cli_credential_flag_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|\s)--(?:api[-_]?key|account[-_]?key|password|passwd|passcode|passphrase|private[-_]?key|secret[-_]?key|client[-_]?secret|access[-_]?key|auth[-_]?key|signing[-_]?key|encryption[-_]?key|basic[-_]?auth|proxy[-_]?auth|http[-_]?auth|(?:api|auth|access|refresh|bearer|session)[-_]?token|cookie)(?:\s+|=)[\"']?[^\s\"',;}]+"#,
        )
        .expect("CLI credential flag regex must compile")
    })
}

fn curl_user_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\bcurl(?:\.exe)?\b[^\r\n]{0,2048}?(?:[ \t]+--user(?:[ \t]+|=)|[ \t]+-u(?:[ \t]+|=)?)[\"']?(?P<user>[^:\s\"';&|]{0,256}):(?P<password>[^\s\"',;}&|]{1,1024})"#,
        )
        .expect("curl user-password regex must compile")
    })
}

fn contains_curl_user_credential(value: &str) -> bool {
    curl_user_credential_pattern()
        .captures_iter(value)
        .any(|captures| {
            let Some(password) = captures.name("password") else {
                return false;
            };
            let password = password.as_str();
            // Shell/template references do not contain a credential value.
            // Keep the boundary focused on literal user-password pairs while
            // still catching curl's spaced, equals, and joined short forms.
            !password.starts_with('$')
                && !password.starts_with('<')
                && !password.starts_with("{{")
                && !password.starts_with('%')
        })
}

fn netrc_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|\s)(?:machine\s+[^\s]+\s+login\s+[^\s]+\s+password\s+[^\s]+|machine\s+[^\s]+\s+password\s+[^\s]+(?:\s+login\s+[^\s]+)?|default\s+login\s+[^\s]+\s+password\s+[^\s]+|default\s+password\s+[^\s]+)"#,
        )
        .expect("netrc credential regex must compile")
    })
}

fn sql_password_clause_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?is)\b(?:alter|create)\s+(?:role|user)\b[^;]{0,512}\bpassword\s+(?:e|u&)?[\"'][^\"'\r\n]{1,1024}[\"']"#,
        )
        .expect("SQL password clause regex must compile")
    })
}

fn mysql_identified_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?is)\b(?:alter|create)\s+user\b[^;]{0,512}\bidentified\s+(?:(?:with|via)\s+[a-z0-9_.$-]+\s+)?(?:by|as)\s+(?:password\s+)?[\"'][^\"'\r\n]{1,1024}[\"']"#,
        )
        .expect("MySQL identified credential regex must compile")
    })
}

fn xml_credential_tag_name_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)^\s*(?:[a-z_][a-z0-9_.-]*:)?(?:password|passwd|passcode|passphrase|credential|secret|token|api[-_]?key|private[-_]?key|client[-_]?secret|access[-_]?key|auth[-_]?key|signing[-_]?key|encryption[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|session[-_]?cookie)(?:\s|/|$)"#,
        )
        .expect("XML credential tag-name regex must compile")
    })
}

fn xml_credential_attribute_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\b(?:password|passwd|passcode|passphrase|credential|secret|token|api[-_]?key|private[-_]?key|client[-_]?secret|access[-_]?key|auth[-_]?key|signing[-_]?key|encryption[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|session[-_]?cookie)\s*=\s*[\"'][^\"'\r\n]{1,1024}[\"']"#,
        )
        .expect("XML credential attribute regex must compile")
    })
}

fn xml_credential_name_attribute_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\b(?:name|key)\s*=\s*[\"'](?:password|passwd|passcode|passphrase|credential|secret|token|api[-_]?key|private[-_]?key|client[-_]?secret|access[-_]?key|auth[-_]?key|signing[-_]?key|encryption[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|session[-_]?cookie)[\"']"#,
        )
        .expect("XML credential name attribute regex must compile")
    })
}

fn xml_value_attribute_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)\bvalue\s*=\s*[\"'][^\"'\r\n]{1,1024}[\"']"#)
            .expect("XML value attribute regex must compile")
    })
}

fn xml_element_body_contains_text(body: &str) -> bool {
    let mut bounded_end = body.len().min(4_096);
    while !body.is_char_boundary(bounded_end) {
        bounded_end = bounded_end.saturating_sub(1);
    }
    let body = &body[..bounded_end];
    let mut cursor = 0usize;
    let mut nested_depth = 0usize;

    while cursor < body.len() {
        let remainder = &body[cursor..];
        if let Some(cdata) = remainder.strip_prefix("<![CDATA[") {
            let (text, consumed) = match cdata.find("]]>") {
                Some(end) => (&cdata[..end], "<![CDATA[".len() + end + "]]>".len()),
                None => (cdata, remainder.len()),
            };
            if text.chars().any(|character| !character.is_whitespace()) {
                return true;
            }
            cursor += consumed;
            continue;
        }
        if remainder.starts_with("<!--") {
            let consumed = remainder
                .find("-->")
                .map_or(remainder.len(), |end| end + "-->".len());
            cursor += consumed;
            continue;
        }
        if remainder.starts_with("</") {
            if nested_depth == 0 {
                break;
            }
            let Some(end) = remainder.find('>') else {
                break;
            };
            nested_depth -= 1;
            cursor += end + 1;
            continue;
        }
        if remainder.starts_with('<') {
            let Some(end) = remainder.find('>') else {
                break;
            };
            let tag = &remainder[1..end];
            let trimmed = tag.trim_start();
            if !trimmed.starts_with('!')
                && !trimmed.starts_with('?')
                && !tag.trim_end().ends_with('/')
            {
                nested_depth += 1;
            }
            cursor += end + 1;
            continue;
        }

        let text_end = remainder.find('<').unwrap_or(remainder.len());
        if remainder[..text_end]
            .chars()
            .any(|character| !character.is_whitespace())
        {
            return true;
        }
        cursor += text_end;
    }
    false
}

fn contains_xml_credential(value: &str) -> bool {
    let mut remainder = value;
    while let Some(open) = remainder.find('<') {
        remainder = &remainder[open + 1..];
        let Some(close) = remainder.find('>') else {
            break;
        };
        if close > 4_096 {
            remainder = &remainder[close + 1..];
            continue;
        }
        let tag = &remainder[..close];
        let trimmed_tag = tag.trim_start();
        if trimmed_tag.starts_with('/')
            || trimmed_tag.starts_with('!')
            || trimmed_tag.starts_with('?')
        {
            remainder = &remainder[close + 1..];
            continue;
        }

        if xml_credential_attribute_pattern().is_match(tag)
            || (xml_credential_name_attribute_pattern().is_match(tag)
                && xml_value_attribute_pattern().is_match(tag))
        {
            return true;
        }

        if xml_credential_tag_name_pattern().is_match(tag) {
            if xml_value_attribute_pattern().is_match(tag) {
                return true;
            }
            if !tag.trim_end().ends_with('/') {
                let body = &remainder[close + 1..];
                if xml_element_body_contains_text(body) {
                    return true;
                }
            }
        }
        remainder = &remainder[close + 1..];
    }
    false
}

fn parse_pgpass_fields(line: &str) -> Option<Vec<String>> {
    if line.chars().count() > 4_096 {
        return None;
    }

    let mut fields = Vec::with_capacity(5);
    let mut field = String::new();
    let mut escaped = false;
    for character in line.chars() {
        if escaped {
            field.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ':' {
            fields.push(std::mem::take(&mut field));
        } else {
            field.push(character);
        }
    }
    if escaped {
        return None;
    }
    fields.push(field);
    (fields.len() == 5).then_some(fields)
}

fn contains_pgpass_record(value: &str) -> bool {
    value.lines().any(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return false;
        }
        let Some(fields) = parse_pgpass_fields(line) else {
            return false;
        };
        let [host, port, database, user, password] = fields.as_slice() else {
            return false;
        };
        let host_is_bounded_pg_target = host == "*"
            || host.eq_ignore_ascii_case("localhost")
            || host.contains('.')
            || host.contains(':')
            || host.contains('/');
        let port_is_valid = port == "*"
            || (port.len() <= 5
                && port.bytes().all(|byte| byte.is_ascii_digit())
                && port.parse::<u16>().is_ok_and(|port| port > 0));
        host_is_bounded_pg_target
            && port_is_valid
            && [database, user, password]
                .iter()
                .all(|field| !field.is_empty() && !field.chars().any(char::is_whitespace))
    })
}

fn environment_credential_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9_])(?P<name>(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)*_(?:access_key_id|secret_key_base|storage_account_key|api_key|access_key|secret_key|private_key|client_key|auth_key|basic_auth|proxy_auth|http_auth|signing_key|encryption_key|token|pat|secret|password|passcode|otp)|secret_key_base|pgpassword|basic_auth|proxy_auth|http_auth))\s*(?::|=)\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("environment credential assignment regex must compile")
    })
}

fn ambiguous_environment_credential_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9_])(?:(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)*)_)?(?:auth|login|user|account|admin|root|credential|secret|service|server|client|app|application|device|database|db|sql|postgres|postgresql|pg|mysql|mariadb|redis|mongo|mongodb|cache|broker|smtp|imap|pop3|ftp|sftp|ssh|registry|repository|repo|vault|keystore|keychain)_(?:pass|pwd|pin)\s*(?::|=)\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("ambiguous environment credential assignment regex must compile")
    })
}

fn contains_environment_credential_assignment(value: &str) -> bool {
    environment_credential_assignment_pattern()
        .captures_iter(value)
        .any(|captures| {
            captures
                .name("name")
                .is_some_and(|name| !name.as_str().eq_ignore_ascii_case("max_token"))
        })
        || ambiguous_environment_credential_assignment_pattern().is_match(value)
}

fn docker_auth_config_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)(?:^|[^a-z0-9_])docker_auth_config\s*(?::|=)")
            .expect("Docker authentication config regex must compile")
    })
}

fn docker_auth_field_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?is)[\"']auth[\"']\s*:\s*[\"'][^\"'\s]{4,}[\"']"#)
            .expect("Docker authentication field regex must compile")
    })
}

fn contains_docker_auth_config(value: &str) -> bool {
    if docker_auth_config_pattern().is_match(value) {
        return true;
    }
    let lowercase = value.to_ascii_lowercase();
    let Some(auths_index) = lowercase
        .find("\"auths\"")
        .or_else(|| lowercase.find("'auths'"))
    else {
        return false;
    };
    let mut bounded_end = auths_index.saturating_add(4_096).min(value.len());
    while !value.is_char_boundary(bounded_end) {
        bounded_end = bounded_end.saturating_sub(1);
    }
    docker_auth_field_pattern().is_match(&value[auths_index..bounded_end])
}

fn known_secret_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)(?:\bsk-(?:proj-)?[a-z0-9_-]{12,}|\bgh[pousr]_[a-z0-9]{20,}|\bgithub_pat_[a-z0-9_]{20,}|\bglpat-[a-z0-9_-]{20,}|\bxox[baprs]-[a-z0-9-]{10,}|\bAIza[a-z0-9_-]{20,}|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|\beyJ[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\.[a-z0-9_-]{8,})",
        )
        .expect("known secret regex must compile")
    })
}

fn authorization_secret_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)\b(?:proxy-)?authorization\s*:\s*[^\r\n]+")
            .expect("authorization secret regex must compile")
    })
}

fn bare_authorization_scheme_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)\b(?:bearer|basic)\s+(?P<token>[a-z0-9._~+/=-]{12,})")
            .expect("bare authorization scheme regex must compile")
    })
}

fn contains_authorization_secret(value: &str) -> bool {
    authorization_secret_pattern().is_match(value)
        || bare_authorization_scheme_pattern()
            .captures_iter(value)
            .any(|captures| {
                captures.name("token").is_some_and(|token| {
                    let token = token.as_str();
                    let has_non_letter = token.bytes().any(|byte| !byte.is_ascii_alphabetic());
                    let lowercase_count = token
                        .bytes()
                        .filter(|byte| byte.is_ascii_lowercase())
                        .count();
                    let uppercase_count = token
                        .bytes()
                        .filter(|byte| byte.is_ascii_uppercase())
                        .count();
                    has_non_letter || (lowercase_count >= 2 && uppercase_count >= 2)
                })
            })
}

fn credential_url_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]{0,128}:[^/\s@]{1,128}@")
            .expect("credential URL regex must compile")
    })
}

fn looks_like_technical_path_token(token: &str) -> bool {
    if token.matches('/').count() < 2 {
        return false;
    }

    let segments = token
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let has_directory_marker = segments.iter().any(|segment| {
        [
            "app",
            "apps",
            "bin",
            "config",
            "crates",
            "docs",
            "etc",
            "examples",
            "fixtures",
            "home",
            "lib",
            "opt",
            "packages",
            "scripts",
            "src",
            "test",
            "tests",
            "tmp",
            "users",
            "usr",
            "var",
            "workspace",
            "workspaces",
        ]
        .iter()
        .any(|marker| segment.eq_ignore_ascii_case(marker))
    });
    let has_known_file_extension = segments.last().is_some_and(|segment| {
        segment.rsplit_once('.').is_some_and(|(stem, suffix)| {
            !stem.is_empty()
                && [
                    "c", "cc", "cfg", "conf", "cpp", "css", "go", "h", "hpp", "htm", "html", "ini",
                    "java", "js", "json", "jsx", "kt", "kts", "lock", "md", "mjs", "mm", "php",
                    "proto", "py", "rb", "rs", "scss", "sh", "sql", "swift", "toml", "ts", "tsx",
                    "txt", "xml", "yaml", "yml", "zsh",
                ]
                .iter()
                .any(|extension| suffix.eq_ignore_ascii_case(extension))
        })
    });

    has_directory_marker && (token.starts_with('/') || has_known_file_extension)
}

fn ascii_shannon_entropy(token: &str) -> f64 {
    let mut counts = [0usize; 128];
    for byte in token.bytes() {
        if byte.is_ascii() {
            counts[byte as usize] += 1;
        }
    }
    let length = token.len() as f64;
    counts
        .into_iter()
        .filter(|count| *count > 0)
        .map(|count| {
            let probability = count as f64 / length;
            -probability * probability.log2()
        })
        .sum()
}

fn hash_context_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)\b(?:sha(?:-?(?:1|224|256|384|512))?|md5|hash|digest|checksum|commit|revision|etag|fingerprint|content[-_ ]address|object[-_ ]id)\b",
        )
        .expect("hash-context regex must compile")
    })
}

fn contains_opaque_hex_secret_token(value: &str) -> bool {
    if hash_context_pattern().is_match(value) || looks_like_technical_path_token(value.trim()) {
        return false;
    }
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|token| {
            let length = token.len();
            (32..=4_096).contains(&length)
                && token.bytes().all(|byte| byte.is_ascii_hexdigit())
                && token.bytes().any(|byte| byte.is_ascii_digit())
                && token.bytes().any(|byte| byte.is_ascii_alphabetic())
                && token.bytes().collect::<HashSet<_>>().len() >= 8
                && ascii_shannon_entropy(token) >= 3.2
        })
}

fn contains_high_entropy_secret_token(value: &str) -> bool {
    value
        .split(|character: char| {
            !(character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.' | '+' | '/' | '='))
        })
        .any(|token| {
            let length = token.len();
            if !(24..=4_096).contains(&length) || token.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return false;
            }
            if looks_like_technical_path_token(token) {
                return false;
            }
            let distinct_bytes = token.bytes().collect::<HashSet<_>>().len();
            distinct_bytes >= 12
                && ascii_shannon_entropy(token) >= 4.0
                && token.bytes().any(|byte| byte.is_ascii_lowercase())
                && token.bytes().any(|byte| byte.is_ascii_uppercase())
                && token.bytes().any(|byte| byte.is_ascii_digit())
        })
}

fn contains_secret_like_value_without_markdown_normalization(value: &str) -> bool {
    value.contains("-----BEGIN PRIVATE KEY-----")
        || value.contains("-----BEGIN RSA PRIVATE KEY-----")
        || value.contains("-----BEGIN EC PRIVATE KEY-----")
        || value.contains("-----BEGIN OPENSSH PRIVATE KEY-----")
        || secret_assignment_pattern().is_match(value)
        || generic_secret_assignment_pattern().is_match(value)
        || contains_present_tense_secret_assignment(value)
        || contains_past_tense_secret_assignment(value)
        || pin_credential_assignment_pattern().is_match(value)
        || standalone_pin_credential_pattern().is_match(value)
        || short_credential_config_field_pattern().is_match(value)
        || redis_password_directive_pattern().is_match(value)
        || captures_non_state_credential_value(markdown_table_credential_pattern(), value)
        || cli_credential_flag_pattern().is_match(value)
        || contains_curl_user_credential(value)
        || netrc_credential_pattern().is_match(value)
        || sql_password_clause_pattern().is_match(value)
        || mysql_identified_credential_pattern().is_match(value)
        || contains_xml_credential(value)
        || contains_pgpass_record(value)
        || contains_environment_credential_assignment(value)
        || contains_docker_auth_config(value)
        || known_secret_pattern().is_match(value)
        || contains_authorization_secret(value)
        || credential_url_pattern().is_match(value)
        || contains_high_entropy_secret_token(value)
}

/// Return a second, comparison-only representation with lightweight Markdown
/// delimiters removed. The original value is still checked first, so this
/// cannot make an existing detector less effective. This catches formatted
/// labels such as `**Password**: value`, `**API key:** value`, and
/// `` `password`: value `` without ever returning or persisting the normalized
/// text.
fn without_lightweight_markdown_delimiters(value: &str) -> Option<String> {
    value
        .chars()
        .any(|character| matches!(character, '*' | '_' | '~' | '`'))
        .then(|| {
            value
                .chars()
                .filter(|character| !matches!(character, '*' | '_' | '~' | '`'))
                .collect()
        })
}

fn contains_non_hex_secret_like_value(value: &str) -> bool {
    contains_secret_like_value_without_markdown_normalization(value)
        || without_lightweight_markdown_delimiters(value)
            .as_deref()
            .is_some_and(contains_secret_like_value_without_markdown_normalization)
}

pub(crate) fn contains_secret_like_value(value: &str) -> bool {
    contains_non_hex_secret_like_value(value) || contains_opaque_hex_secret_token(value)
}

pub(crate) fn sanitize_extraction_source(value: &str) -> String {
    if contains_secret_like_value(value) {
        REDACTED_EXTRACTION_SOURCE.to_string()
    } else {
        value.to_string()
    }
}

/// Return false when any complete source, or any ordered pair of sources,
/// forms a credential. Checking both orders is important because model output
/// and task metadata do not guarantee that the label precedes the value.
pub(crate) fn extraction_sources_are_secret_safe(sources: &[&str]) -> bool {
    let has_hash_context = sources
        .iter()
        .any(|source| hash_context_pattern().is_match(source));
    if sources.iter().any(|source| {
        contains_non_hex_secret_like_value(source)
            || (!has_hash_context && contains_opaque_hex_secret_token(source))
    }) {
        return false;
    }

    sources.iter().enumerate().all(|(left_index, left)| {
        sources
            .iter()
            .enumerate()
            .filter(|(right_index, _)| *right_index != left_index)
            .all(|(_, right)| {
                let pair = format!("{left}: {right}");
                !contains_non_hex_secret_like_value(&pair)
                    && (has_hash_context || !contains_opaque_hex_secret_token(&pair))
            })
    })
}

/// Sanitize a label/content pair together so a split credential such as
/// `Password` + `hunter2` cannot bypass field-local checks.
pub(crate) fn sanitize_extraction_source_pair(label: &str, content: &str) -> (String, String) {
    if !extraction_sources_are_secret_safe(&[label, content]) {
        (
            REDACTED_EXTRACTION_SOURCE.to_string(),
            REDACTED_EXTRACTION_SOURCE.to_string(),
        )
    } else {
        (label.to_string(), content.to_string())
    }
}

pub(crate) fn durable_candidate_is_secret_safe(candidate: &DurableExtractionCandidate) -> bool {
    let mut sources = vec![
        candidate.title.as_str(),
        candidate.kind.as_str(),
        candidate.content.as_str(),
    ];
    if let Some(scope) = candidate.scope.as_deref() {
        sources.push(scope);
    }
    sources.extend(candidate.tags.iter().map(String::as_str));
    if let Some(session_id) = candidate.session_id.as_deref() {
        sources.push(session_id);
    }
    if let Some(confidence) = candidate.confidence.as_deref() {
        sources.push(confidence);
    }
    extraction_sources_are_secret_safe(&sources)
}

pub(crate) fn ledger_candidate_is_secret_safe(candidate: &LedgerExtractionCandidate) -> bool {
    let mut sources = vec![candidate.title.as_str(), candidate.kind.as_str()];
    if let Some(due_at) = candidate.due_at.as_deref() {
        sources.push(due_at);
    }
    if let Some(starts_at) = candidate.starts_at.as_deref() {
        sources.push(starts_at);
    }
    if let Some(excerpt) = candidate.excerpt.as_deref() {
        sources.push(excerpt);
    }
    if let Some(session_id) = candidate.session_id.as_deref() {
        sources.push(session_id);
    }
    if let Some(confidence) = candidate.confidence.as_deref() {
        sources.push(confidence);
    }
    extraction_sources_are_secret_safe(&sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detector_rejects_credentials_without_hiding_technical_prose() {
        for (case, value) in [
            ("tokenizer word", "tokenizer is tiktoken"),
            ("token budget", "MAX_TOKEN=1000"),
            ("dependency pin", "pin is a dependency reference"),
            ("hardware pin", "GPIO_PIN=13"),
            ("compiler pass", "COMPILER_PASS=inline"),
            ("auth mode", "AUTH_MODE=basic"),
            ("Docker auth mode", "DOCKER_AUTH_MODE=credential-store"),
            ("project key", "PROJECT_KEY=abc"),
            ("absolute path", "/Users/Alice/Project2/config.toml"),
            ("relative path", "src/HTTP2Client/Config.toml"),
            ("basic plan", "I prefer the basic plan"),
            ("bearer bonds", "We trade bearer bonds"),
            ("credential file flag", "deploy --password-file secrets.txt"),
            ("token budget flag", "runner --token-budget 1000"),
            (
                "curl user without password",
                "curl --user alice https://example.test",
            ),
            (
                "curl user-password variables",
                "curl --user '$CURL_USER:$CURL_PASSWORD' https://example.test",
            ),
            ("git upstream flag", "git push -u origin:main"),
            (
                "machine login prose",
                "machine learning login flows enforce password policy",
            ),
            (
                "SQL password policy",
                "ALTER ROLE alice SET password_policy = 'strict';",
            ),
            (
                "MySQL generated password",
                "CREATE USER 'alice' IDENTIFIED BY RANDOM PASSWORD;",
            ),
            (
                "past-tense password reset",
                "The database password was reset yesterday.",
            ),
            (
                "past-tense password requirement",
                "A password was required for the legacy login flow.",
            ),
            (
                "present-tense password requirement",
                "The staging password is required for deploys.",
            ),
            (
                "present-tense PIN configuration",
                "The login PIN is configured by the identity provider.",
            ),
            (
                "present-tense token state",
                "My token is revoked after account deletion.",
            ),
            ("ordinary pass field", "pass: true"),
            ("commented Redis password", "# requirepass hunter2"),
            ("empty Redis password", "requirepass \"\""),
            (
                "Markdown password requirement",
                "| Password | required | authentication policy |",
            ),
            (
                "Markdown password policy",
                "| Password policy | rotate quarterly |",
            ),
            (
                "XML password policy element",
                "<password-policy>rotate quarterly</password-policy>",
            ),
            (
                "XML password policy property",
                "<property name=\"password_policy\" value=\"strict\"/>",
            ),
            (
                "empty XML password CDATA",
                "<password><![CDATA[   ]]></password>",
            ),
            ("colon-separated timestamp", "2026:09:17:20:53"),
            ("colon-separated code fields", "crate:123:module:item:value"),
            (
                "SHA-256 digest",
                "SHA-256 digest: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            ),
            (
                "Git commit hash",
                "commit 0123456789abcdef0123456789abcdef01234567",
            ),
        ] {
            assert!(
                !contains_secret_like_value(value),
                "ordinary technical case was classified as a secret: {case}"
            );
            assert_eq!(
                sanitize_extraction_source(value),
                value,
                "ordinary technical case was redacted: {case}"
            );
        }

        for (case, value) in [
            (
                "API key",
                "OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz",
            ),
            ("spaced API key", "API key: hunter2"),
            ("spaced private key", "private key: hunter2"),
            ("spaced secret key", "secret key: hunter2"),
            ("bold password label", "**Password**: hunter2"),
            ("bold API key assignment", "**API key:** hunter2"),
            ("inline-code password label", "`password`: hunter2"),
            (
                "authorization header",
                "Authorization: Bearer AbCdEfGhIjKlMnOpQrStUvWxYz123456",
            ),
            ("bare basic credential", "Basic dXNlcjpwYXNz"),
            (
                "session cookie",
                "session cookie: 0123456789abcdef0123456789abcdef",
            ),
            ("natural password", "my password is hunter2"),
            (
                "past-tense database password",
                "The database password was hunter2",
            ),
            ("personal token", "my token is abc"),
            ("personal PIN", "my PIN is 1234"),
            ("database password", "PGPASSWORD=abc"),
            ("login PIN", "LOGIN_PIN=123"),
            ("standalone PIN", "PIN: 1234"),
            ("mixed-case standalone PIN", "Pin: 1234"),
            ("lowercase standalone PIN", "pin: 1234"),
            ("equals standalone PIN", "PIN = 1234"),
            ("word-assigned standalone PIN", "pin is 1234"),
            ("CLI password", "deploy --password hunter2"),
            ("CLI API key", "deploy --api-key hunter2"),
            ("CLI client secret", "deploy --client-secret=hunter2"),
            (
                "curl long user-password flag",
                "curl --user alice:hunter2 https://example.test",
            ),
            (
                "curl equals user-password flag",
                "curl --user=alice:hunter2 https://example.test",
            ),
            (
                "curl short user-password flag",
                "curl -u alice:hunter2 https://example.test",
            ),
            (
                "curl joined short user-password flag",
                "curl -ualice:hunter2 https://example.test",
            ),
            (
                "curl password-only flag",
                "curl -u :hunter2 https://example.test",
            ),
            (
                "netrc machine record",
                "machine example.test login alice password hunter2",
            ),
            (
                "netrc default record",
                "default login alice password hunter2",
            ),
            (
                "PostgreSQL ALTER ROLE password",
                "ALTER ROLE alice WITH PASSWORD 'hunter2';",
            ),
            (
                "PostgreSQL CREATE USER password",
                "CREATE USER alice PASSWORD E'hunter2';",
            ),
            (
                "MySQL CREATE USER credential",
                "CREATE USER 'alice'@'localhost' IDENTIFIED BY 'hunter2';",
            ),
            (
                "MySQL plugin credential",
                "ALTER USER 'alice'@'localhost' IDENTIFIED WITH mysql_native_password AS 'hunter2';",
            ),
            (
                "MongoDB pwd field",
                "db.createUser({user: \"alice\", pwd: \"hunter2\"})",
            ),
            ("short pass config field", "pass = 'hunter2'"),
            ("Redis requirepass", "requirepass hunter2"),
            ("Redis masterauth", "masterauth hunter2"),
            (
                "Redis CONFIG SET password",
                "CONFIG SET requirepass 'hunter2'",
            ),
            ("Markdown password row", "| Password | hunter2 |"),
            (
                "Markdown API key row",
                "| API key | hunter2 | production |",
            ),
            ("XML password element", "<password>hunter2</password>"),
            (
                "XML password CDATA",
                "<password><![CDATA[hunter2]]></password>",
            ),
            (
                "nested XML password CDATA",
                "<password><value><![CDATA[hunter2]]></value></password>",
            ),
            (
                "nested XML password element",
                "<password><value>hunter2</value></password>",
            ),
            ("XML password attribute", "<database password=\"hunter2\"/>"),
            (
                "XML credential property",
                "<property name=\"password\" value=\"hunter2\"/>",
            ),
            (
                "XML credential value attribute",
                "<password value=\"hunter2\"/>",
            ),
            (
                "PostgreSQL password-file record",
                "db.example.test:5432:app:alice:hunter2",
            ),
            (
                "PostgreSQL password-file escaped password",
                "localhost:5432:app:alice:hun\\:ter2",
            ),
            (
                "connection-string key",
                "Endpoint=sb://example.test/;SharedAccessKeyName=writer;SharedAccessKey=abc",
            ),
            (
                "Docker auth JSON",
                "{\"auths\":{\"registry.example\":{\"auth\":\"dXNlcjpwYXNz\"}}}",
            ),
            (
                "credential URL",
                "postgres://user:password-value@example.test/database",
            ),
            (
                "unlabelled opaque hexadecimal token",
                "0123456789abcdef0123456789abcdef",
            ),
            ("opaque token", "mF9/Bx7Qa2cD8/Zp4Ln6Rt3Vy5Kw1Hs0Je"),
            ("private key", "-----BEGIN OPENSSH PRIVATE KEY-----"),
        ] {
            assert!(
                contains_secret_like_value(value),
                "secret case was not detected: {case}"
            );
            assert!(
                sanitize_extraction_source(value) == REDACTED_EXTRACTION_SOURCE,
                "secret case was not redacted: {case}"
            );
        }
    }

    #[test]
    fn pair_and_candidate_checks_reject_split_credentials() {
        let (label, content) = sanitize_extraction_source_pair("Password", "hunter2");
        assert!(
            label == REDACTED_EXTRACTION_SOURCE,
            "split label was not redacted"
        );
        assert!(
            content == REDACTED_EXTRACTION_SOURCE,
            "split content was not redacted"
        );

        let (label, content) = sanitize_extraction_source_pair("**Password**", "hunter2");
        assert!(
            label == REDACTED_EXTRACTION_SOURCE && content == REDACTED_EXTRACTION_SOURCE,
            "Markdown-wrapped split credential was not redacted"
        );

        let memory = DurableExtractionCandidate {
            title: "Password".to_string(),
            kind: "reference".to_string(),
            content: "hunter2".to_string(),
            scope: Some("global".to_string()),
            tags: vec!["credential".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(!durable_candidate_is_secret_safe(&memory));

        let reversed_memory = DurableExtractionCandidate {
            title: "hunter2".to_string(),
            kind: "reference".to_string(),
            content: "Password".to_string(),
            scope: Some("global".to_string()),
            tags: vec!["database".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(
            !durable_candidate_is_secret_safe(&reversed_memory),
            "content used as the credential label must reject the candidate"
        );

        let tag_labelled_memory = DurableExtractionCandidate {
            title: "Production database".to_string(),
            kind: "reference".to_string(),
            content: "hunter2".to_string(),
            scope: Some("global".to_string()),
            tags: vec!["password".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(
            !durable_candidate_is_secret_safe(&tag_labelled_memory),
            "a tag used as the credential label must reject the candidate"
        );

        let hash_candidate = DurableExtractionCandidate {
            title: "Build artifact SHA-256 digest".to_string(),
            kind: "reference".to_string(),
            content: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            scope: Some("project".to_string()),
            tags: vec!["checksum".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(
            durable_candidate_is_secret_safe(&hash_candidate),
            "an explicitly labelled technical digest must remain compatible"
        );

        for (field, candidate) in [
            (
                "kind",
                DurableExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "password".to_string(),
                    content: "Production database".to_string(),
                    scope: Some("global".to_string()),
                    tags: vec![],
                    session_id: Some("session-1".to_string()),
                    confidence: Some("high".to_string()),
                },
            ),
            (
                "scope",
                DurableExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "reference".to_string(),
                    content: "Production database".to_string(),
                    scope: Some("password".to_string()),
                    tags: vec![],
                    session_id: Some("session-1".to_string()),
                    confidence: Some("high".to_string()),
                },
            ),
            (
                "confidence",
                DurableExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "reference".to_string(),
                    content: "Production database".to_string(),
                    scope: Some("global".to_string()),
                    tags: vec![],
                    session_id: Some("session-1".to_string()),
                    confidence: Some("password".to_string()),
                },
            ),
        ] {
            assert!(
                !durable_candidate_is_secret_safe(&candidate),
                "raw durable-candidate field {field} must participate in pair checks"
            );
        }

        let ledger = LedgerExtractionCandidate {
            title: "PIN".to_string(),
            excerpt: Some("1234".to_string()),
            ..LedgerExtractionCandidate::default()
        };
        assert!(!ledger_candidate_is_secret_safe(&ledger));

        let reversed_ledger = LedgerExtractionCandidate {
            title: "1234".to_string(),
            excerpt: Some("PIN".to_string()),
            ..LedgerExtractionCandidate::default()
        };
        assert!(
            !ledger_candidate_is_secret_safe(&reversed_ledger),
            "excerpt used as the credential label must reject the candidate"
        );

        for (field, candidate) in [
            (
                "kind",
                LedgerExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "password".to_string(),
                    ..LedgerExtractionCandidate::default()
                },
            ),
            (
                "due_at",
                LedgerExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "todo".to_string(),
                    due_at: Some("password".to_string()),
                    ..LedgerExtractionCandidate::default()
                },
            ),
            (
                "starts_at",
                LedgerExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "todo".to_string(),
                    starts_at: Some("password".to_string()),
                    ..LedgerExtractionCandidate::default()
                },
            ),
            (
                "confidence",
                LedgerExtractionCandidate {
                    title: "hunter2".to_string(),
                    kind: "todo".to_string(),
                    confidence: Some("password".to_string()),
                    ..LedgerExtractionCandidate::default()
                },
            ),
        ] {
            assert!(
                !ledger_candidate_is_secret_safe(&candidate),
                "raw ledger-candidate field {field} must participate in pair checks"
            );
        }
    }
}
