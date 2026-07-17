//! Shared `-m`/`--model` CLI-flag grammar.
//!
//! Every model-selecting flag in the `bamboo` binary (`-p -m`, `actor run
//! -m`/`actor serve -m`, `broker-agent spawn --model`) accepts the same two
//! shapes: `provider:model`, or a bare `model` id whose provider is filled in
//! by the caller's own default policy (local config default, an explicit
//! `--provider` flag, or "resolved later by the runtime" for a remote
//! worker). Before this module each call site re-implemented the
//! trim/split/validate step by hand and drifted: `bamboo -p -m` used to
//! *require* the colon while `actor run -m` already accepted a bare id, and
//! `broker-agent spawn --model` silently accepted a malformed `provider:`
//! (empty half) that the other two rejected (#246).
//!
//! [`parse_model_spec`] is the one place that grammar is decided. Callers
//! still own defaulting (what a bare model or an absent flag falls back to)
//! since that legitimately differs by context.

/// A parsed `-m`/`--model` value.
///
/// `provider` is `None` for a bare `model` id (no colon) — the caller decides
/// what that means (bind to a `--provider` flag, the configured default
/// provider, or leave it for a remote runtime to resolve).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedModelSpec {
    pub provider: Option<String>,
    pub model: String,
}

/// Parse a `-m`/`--model` flag value against the one grammar shared by the
/// whole `bamboo` binary:
///
/// - blank / whitespace-only    → `Ok(None)` — treated as "flag omitted".
/// - `"provider:model"`         → `Ok(Some({provider: Some(provider), model}))`.
/// - `"model"` (no colon)       → `Ok(Some({provider: None, model}))`.
/// - `"provider:"` / `":model"` (an empty half around a colon) → `Err` — a
///   typo'd colon should fail fast rather than silently produce an empty
///   provider or model.
///
/// Both halves (and a colon-less bare value) are trimmed of surrounding
/// whitespace, so `" openai : gpt-4o "` parses the same as `"openai:gpt-4o"`.
pub fn parse_model_spec(raw: &str) -> Result<Option<ParsedModelSpec>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    match trimmed.split_once(':') {
        Some((p, m)) => {
            let (p, m) = (p.trim(), m.trim());
            if p.is_empty() || m.is_empty() {
                return Err(format!("'{trimmed}' must be 'provider:model'"));
            }
            Ok(Some(ParsedModelSpec {
                provider: Some(p.to_string()),
                model: m.to_string(),
            }))
        }
        None => Ok(Some(ParsedModelSpec {
            provider: None,
            model: trimmed.to_string(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_is_none() {
        assert_eq!(parse_model_spec("").unwrap(), None);
        assert_eq!(parse_model_spec("   ").unwrap(), None);
    }

    #[test]
    fn colon_form_splits_both_halves() {
        assert_eq!(
            parse_model_spec("openai:gpt-4o").unwrap(),
            Some(ParsedModelSpec {
                provider: Some("openai".into()),
                model: "gpt-4o".into(),
            })
        );
    }

    #[test]
    fn bare_model_has_no_provider() {
        assert_eq!(
            parse_model_spec("gpt-4o").unwrap(),
            Some(ParsedModelSpec {
                provider: None,
                model: "gpt-4o".into(),
            })
        );
    }

    #[test]
    fn surrounding_and_inner_whitespace_is_trimmed() {
        assert_eq!(
            parse_model_spec(" openai : gpt-4o ").unwrap(),
            Some(ParsedModelSpec {
                provider: Some("openai".into()),
                model: "gpt-4o".into(),
            })
        );
        assert_eq!(
            parse_model_spec("  gpt-4o  ").unwrap(),
            Some(ParsedModelSpec {
                provider: None,
                model: "gpt-4o".into(),
            })
        );
    }

    #[test]
    fn empty_provider_half_errors() {
        assert!(parse_model_spec(":gpt-4o").is_err());
    }

    #[test]
    fn empty_model_half_errors() {
        assert!(parse_model_spec("openai:").is_err());
    }
}
