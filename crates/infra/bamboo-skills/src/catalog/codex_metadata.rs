// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Adapted from Codex 7f892275e31002f0422477c6219189284560e689:
// ext/skills/src/loader/metadata.rs and skills/src/interface.rs.
// Bamboo projects only short-description and implicit invocation into its host publication.

use serde::Deserialize;

#[derive(Default)]
pub(super) struct OpenAiMetadata {
    pub short_description: Option<String>,
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Deserialize)]
struct MetadataFile {
    #[serde(default)]
    interface: Option<serde_yaml::Value>,
    #[serde(default)]
    policy: Option<Policy>,
}

#[derive(Deserialize)]
struct Policy {
    #[serde(default)]
    allow_implicit_invocation: Option<bool>,
}

#[derive(Deserialize)]
struct Interface {
    #[serde(default)]
    short_description: Option<String>,
}

pub(super) fn parse_openai_metadata(raw: &[u8]) -> OpenAiMetadata {
    // As upstream, optional sidecar errors do not reject SKILL.md. Descriptive
    // field errors are isolated so a valid implicit deny is never discarded.
    let parsed: MetadataFile = match serde_yaml::from_slice(raw) {
        Ok(parsed) => parsed,
        Err(_) => {
            tracing::warn!("Ignoring invalid optional agents/openai.yaml metadata");
            return OpenAiMetadata::default();
        }
    };
    let short_description = parsed
        .interface
        .and_then(|interface| serde_yaml::from_value::<Interface>(interface).ok())
        .and_then(|interface| interface.short_description)
        .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|value| !value.is_empty() && value.chars().count() <= 1024);
    OpenAiMetadata {
        short_description,
        allow_implicit_invocation: parsed
            .policy
            .and_then(|policy| policy.allow_implicit_invocation),
    }
}
