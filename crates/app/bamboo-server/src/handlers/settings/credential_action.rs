use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::AppError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CredentialState {
    Configured,
    FromEnv,
    Missing,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CredentialStatusView {
    pub(crate) credential_ref: Option<String>,
    pub(crate) state: CredentialState,
    pub(crate) configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<bamboo_config::CredentialSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<DateTime<Utc>>,
}

pub(crate) fn credential_status_view(
    reference: Option<&bamboo_config::CredentialRef>,
    expected_configured: bool,
    status: Option<&bamboo_config::CredentialStatus>,
    health: &bamboo_config::CredentialStoreHealth,
) -> CredentialStatusView {
    let state = if !expected_configured {
        CredentialState::Missing
    } else if health.status != bamboo_config::SectionStatus::Healthy {
        CredentialState::Error
    } else {
        match status.filter(|status| status.configured) {
            Some(status) if status.source == bamboo_config::CredentialSource::Environment => {
                CredentialState::FromEnv
            }
            Some(_) => CredentialState::Configured,
            None => CredentialState::Error,
        }
    };
    let configured = matches!(
        state,
        CredentialState::Configured | CredentialState::FromEnv
    );
    let status = status.filter(|status| configured && status.configured);
    CredentialStatusView {
        credential_ref: reference.map(|reference| reference.as_str().to_string()),
        state,
        configured,
        source: status.map(|status| status.source),
        updated_at: status.and_then(|status| status.updated_at),
    }
}

/// Explicit mutation intent for one secret value.
///
/// The value is carried only by `replace`; `keep` and `clear` cannot
/// accidentally deserialize plaintext. Custom `Debug` keeps request logging
/// secret-free.
#[derive(Clone)]
pub(crate) enum CredentialAction {
    Keep,
    Replace { value: String },
    Clear,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CredentialActionKind {
    Keep,
    Replace,
    Clear,
}

#[derive(Default)]
struct ActionValue {
    present: bool,
    value: Option<String>,
}

impl<'de> Deserialize<'de> for ActionValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            present: true,
            value: Option::<String>::deserialize(deserializer)?,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialActionWire {
    action: CredentialActionKind,
    #[serde(default)]
    value: ActionValue,
}

impl<'de> Deserialize<'de> for CredentialAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = CredentialActionWire::deserialize(deserializer)?;
        match (wire.action, wire.value.present, wire.value.value) {
            (CredentialActionKind::Keep, false, None) => Ok(Self::Keep),
            (CredentialActionKind::Clear, false, None) => Ok(Self::Clear),
            (CredentialActionKind::Replace, true, Some(value)) => Ok(Self::Replace { value }),
            (CredentialActionKind::Replace, _, _) => Err(serde::de::Error::custom(
                "replace action requires a string value",
            )),
            (CredentialActionKind::Keep | CredentialActionKind::Clear, true, _) => Err(
                serde::de::Error::custom("keep and clear actions must not include a value"),
            ),
            (CredentialActionKind::Keep | CredentialActionKind::Clear, false, Some(_)) => {
                unreachable!("an absent action value cannot contain data")
            }
        }
    }
}

impl CredentialAction {
    pub(crate) fn validate(&self, label: &str) -> Result<(), AppError> {
        let Self::Replace { value } = self else {
            return Ok(());
        };
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "{label} replace requires a nonempty value"
            )));
        }
        if bamboo_config::patch::is_masked_api_key(value) {
            return Err(AppError::BadRequest(format!(
                "{label} value must not be a mask"
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn replacement(&self) -> Option<&str> {
        match self {
            Self::Replace { value } => Some(value),
            Self::Keep | Self::Clear => None,
        }
    }

    pub(crate) fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
}

impl std::fmt::Debug for CredentialAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep => formatter.write_str("CredentialAction::Keep"),
            Self::Replace { .. } => formatter.write_str("CredentialAction::Replace([REDACTED])"),
            Self::Clear => formatter.write_str("CredentialAction::Clear"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_shape_is_explicit_and_debug_is_redacted() {
        let replace: CredentialAction =
            serde_json::from_str(r#"{"action":"replace","value":"secret-value"}"#).unwrap();
        assert_eq!(replace.replacement(), Some("secret-value"));
        assert!(!format!("{replace:?}").contains("secret-value"));

        assert!(serde_json::from_str::<CredentialAction>(
            r#"{"action":"clear","value":"must-not-be-accepted"}"#
        )
        .is_err());
        assert!(
            serde_json::from_str::<CredentialAction>(r#"{"action":"keep","value":null}"#).is_err()
        );
        assert!(serde_json::from_str::<CredentialAction>(r#"{"action":"replace"}"#).is_err());
        assert!(serde_json::from_str::<CredentialAction>(
            r#"{"action":"replace","value":"secret","future":"field"}"#
        )
        .is_err());
    }

    #[test]
    fn replace_rejects_empty_and_mask_values() {
        let empty = CredentialAction::Replace {
            value: " ".to_string(),
        };
        assert!(empty.validate("test credential").is_err());
        let masked = CredentialAction::Replace {
            value: "********".to_string(),
        };
        assert!(masked.validate("test credential").is_err());
    }

    #[test]
    fn status_view_distinguishes_configured_environment_missing_and_error() {
        let reference = bamboo_config::credential_ref("notification", "ntfy", "token").unwrap();
        let mut status = bamboo_config::CredentialStatus {
            credential_ref: reference.clone(),
            configured: true,
            source: bamboo_config::CredentialSource::User,
            updated_at: None,
        };
        let healthy = bamboo_config::CredentialStoreHealth::committed(3);
        assert_eq!(
            credential_status_view(Some(&reference), true, Some(&status), &healthy).state,
            CredentialState::Configured
        );
        status.source = bamboo_config::CredentialSource::Environment;
        assert_eq!(
            credential_status_view(Some(&reference), true, Some(&status), &healthy).state,
            CredentialState::FromEnv
        );
        assert_eq!(
            credential_status_view(Some(&reference), true, None, &healthy).state,
            CredentialState::Error
        );
        assert_eq!(
            credential_status_view(Some(&reference), false, Some(&status), &healthy).state,
            CredentialState::Missing
        );
        let degraded = bamboo_config::CredentialStoreHealth {
            status: bamboo_config::SectionStatus::Degraded,
            ..healthy
        };
        assert_eq!(
            credential_status_view(Some(&reference), true, Some(&status), &degraded).state,
            CredentialState::Error
        );
    }
}
