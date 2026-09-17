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
            r#"(?i)(?:^|[^a-z0-9])(?:(?:api[\s_-]?key|account[\s_-]?key|shared[\s_-]?access[\s_-]?(?:key|signature)|password|passwd|passcode|passphrase|otp|one[\s_-]?time[\s_-]?(?:password|passcode|code)|verification[\s_-]?code|security[\s_-]?code|recovery[\s_-]?code|mfa[\s_-]?code|2fa[\s_-]?code|credential|private[\s_-]?key|secret[\s_-]?key|client[\s_-]?secret|access[\s_-]?key|auth[\s_-]?key|signing[\s_-]?key|encryption[\s_-]?key|(?:basic|proxy|http)[\s_-]?auth|(?:api|auth|access|refresh|bearer)[\s_-]?token|session[\s_-]*(?:cookie|token|id))[\"']?\s*(?::|=|\bis\b)|cookie[\"']?\s*(?::|=))\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("secret assignment regex must compile")
    })
}

fn generic_secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:(?:^|[\s(\"'])(?:secret|token)[\"']?\s*(?::|=)|(?:^|[^a-z0-9])(?:my|our|your)\s+(?:secret|token)[\"']?\s+\bis\b)\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("generic secret assignment regex must compile")
    })
}

fn pin_credential_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:(?:my|our|your)\s+pin\s+(?:is|:|=)|(?:account|auth|authentication|login|security|verification|recovery|mfa|2fa|bank|card|payment|unlock|device)[\s_-]+pin\s*(?::|=|\bis\b)|pin[\s_-]+(?:code|number)\s*(?::|=|\bis\b))\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("credential-context PIN assignment regex must compile")
    })
}

fn standalone_pin_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?im)^[ \t]*(?:[-*][ \t]+)?pin[ \t]*(?::|=|\bis\b)[ \t]*[\"']?[0-9]{3,12}\b"#)
            .expect("standalone PIN credential regex must compile")
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

pub(crate) fn contains_secret_like_value(value: &str) -> bool {
    value.contains("-----BEGIN PRIVATE KEY-----")
        || value.contains("-----BEGIN RSA PRIVATE KEY-----")
        || value.contains("-----BEGIN EC PRIVATE KEY-----")
        || value.contains("-----BEGIN OPENSSH PRIVATE KEY-----")
        || secret_assignment_pattern().is_match(value)
        || generic_secret_assignment_pattern().is_match(value)
        || pin_credential_assignment_pattern().is_match(value)
        || standalone_pin_credential_pattern().is_match(value)
        || contains_environment_credential_assignment(value)
        || contains_docker_auth_config(value)
        || known_secret_pattern().is_match(value)
        || contains_authorization_secret(value)
        || credential_url_pattern().is_match(value)
        || contains_high_entropy_secret_token(value)
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
    if sources
        .iter()
        .any(|source| contains_secret_like_value(source))
    {
        return false;
    }

    sources.iter().enumerate().all(|(left_index, left)| {
        sources
            .iter()
            .enumerate()
            .filter(|(right_index, _)| *right_index != left_index)
            .all(|(_, right)| !contains_secret_like_value(&format!("{left}: {right}")))
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
    let mut sources = vec![candidate.title.as_str(), candidate.content.as_str()];
    sources.extend(candidate.tags.iter().map(String::as_str));
    if let Some(session_id) = candidate.session_id.as_deref() {
        sources.push(session_id);
    }
    extraction_sources_are_secret_safe(&sources)
}

pub(crate) fn ledger_candidate_is_secret_safe(candidate: &LedgerExtractionCandidate) -> bool {
    let mut sources = vec![candidate.title.as_str()];
    if let Some(excerpt) = candidate.excerpt.as_deref() {
        sources.push(excerpt);
    }
    if let Some(session_id) = candidate.session_id.as_deref() {
        sources.push(session_id);
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
            ("personal token", "my token is abc"),
            ("database password", "PGPASSWORD=abc"),
            ("login PIN", "LOGIN_PIN=123"),
            ("standalone PIN", "PIN: 1234"),
            ("mixed-case standalone PIN", "Pin: 1234"),
            ("lowercase standalone PIN", "pin: 1234"),
            ("equals standalone PIN", "PIN = 1234"),
            ("word-assigned standalone PIN", "pin is 1234"),
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
    }
}
