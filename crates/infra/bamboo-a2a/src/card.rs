use super::error::{A2AClientError, A2AClientResult};
use super::types::AgentCard;

#[derive(Debug, Clone)]
pub struct AgentCardValidation {
    pub rpc_url: String,
    pub protocol_version: String,
    pub streaming_supported: bool,
    pub selected_skill_id: Option<String>,
    pub default_input_modes: Vec<String>,
    pub default_output_modes: Vec<String>,
}

/// Validate an Agent Card for JSON-RPC v1.0 MVP usage.
///
/// Rules:
/// 1. `supportedInterfaces` must contain at least one JSONRPC interface.
/// 2. `protocolVersion` major must equal 1.
/// 3. If `required_streaming`, `capabilities.streaming` must be `Some(true)`.
/// 4. Default/skill input/output must include `text/plain`.
/// 5. If `preferred_skill` provided, find a matching skill.
pub fn validate_agent_card_for_jsonrpc_mvp(
    card: &AgentCard,
    required_streaming: bool,
    preferred_skill: Option<&str>,
) -> A2AClientResult<AgentCardValidation> {
    // 1. Find JSONRPC interface
    let jsonrpc_interface = card
        .supported_interfaces
        .iter()
        .find(|iface| iface.protocol_binding.eq_ignore_ascii_case("JSONRPC"))
        .ok_or_else(|| {
            A2AClientError::InvalidAgentCard("Agent Card has no JSONRPC interface".to_string())
        })?;

    // 2. Validate protocol version major == 1
    let protocol_version = &jsonrpc_interface.protocol_version;
    let major = protocol_version
        .split('.')
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| {
            A2AClientError::InvalidAgentCard(format!(
                "Invalid protocol version: {}",
                protocol_version
            ))
        })?;
    if major != 1 {
        return Err(A2AClientError::VersionNotSupported(format!(
            "Protocol major version {} != 1",
            major
        )));
    }

    // 3. Check streaming support
    let streaming_supported = card.capabilities.streaming.unwrap_or(false);
    if required_streaming && !streaming_supported {
        return Err(A2AClientError::CapabilityMismatch(
            "Agent Card does not support streaming".to_string(),
        ));
    }

    // 4. Check text/plain support in defaults or skills
    let has_text_plain_default = card
        .default_input_modes
        .iter()
        .any(|m| m.eq_ignore_ascii_case("text/plain"))
        || card
            .default_output_modes
            .iter()
            .any(|m| m.eq_ignore_ascii_case("text/plain"));

    let selected_skill_id = if let Some(skill_name) = preferred_skill {
        let skill = card
            .skills
            .iter()
            .find(|s| {
                s.id.eq_ignore_ascii_case(skill_name)
                    || s.name.eq_ignore_ascii_case(skill_name)
                    || s.tags.iter().any(|t| t.eq_ignore_ascii_case(skill_name))
            })
            .ok_or_else(|| {
                A2AClientError::CapabilityMismatch(format!(
                    "Preferred skill '{}' not found in Agent Card",
                    skill_name
                ))
            })?;
        let has_text_plain_skill = skill
            .input_modes
            .iter()
            .any(|m| m.eq_ignore_ascii_case("text/plain"))
            || skill
                .output_modes
                .iter()
                .any(|m| m.eq_ignore_ascii_case("text/plain"));
        if !has_text_plain_default && !has_text_plain_skill {
            return Err(A2AClientError::CapabilityMismatch(
                "Agent Card does not support text/plain input/output".to_string(),
            ));
        }
        Some(skill.id.clone())
    } else if !has_text_plain_default {
        let has_text_plain_any_skill = card.skills.iter().any(|s| {
            s.input_modes
                .iter()
                .any(|m| m.eq_ignore_ascii_case("text/plain"))
                || s.output_modes
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case("text/plain"))
        });
        if !has_text_plain_any_skill {
            return Err(A2AClientError::CapabilityMismatch(
                "Agent Card does not support text/plain input/output".to_string(),
            ));
        }
        None
    } else {
        None
    };

    Ok(AgentCardValidation {
        rpc_url: jsonrpc_interface.url.clone(),
        protocol_version: protocol_version.clone(),
        streaming_supported,
        selected_skill_id,
        default_input_modes: card.default_input_modes.clone(),
        default_output_modes: card.default_output_modes.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AgentCapabilities, AgentInterface, AgentSkill};

    fn make_card(streaming: Option<bool>, interfaces: Vec<AgentInterface>) -> AgentCard {
        AgentCard {
            name: "Test Agent".to_string(),
            description: "Test".to_string(),
            supported_interfaces: interfaces,
            provider: None,
            version: "1.0.0".to_string(),
            documentation_url: None,
            capabilities: AgentCapabilities {
                streaming,
                push_notifications: None,
                extensions: vec![],
                extended_agent_card: None,
            },
            security_schemes: Default::default(),
            security_requirements: vec![],
            default_input_modes: vec!["text/plain".to_string()],
            default_output_modes: vec!["text/plain".to_string()],
            skills: vec![],
            signatures: vec![],
            icon_url: None,
        }
    }

    #[test]
    fn validate_agent_card_selects_jsonrpc_interface() {
        let card = make_card(
            Some(true),
            vec![
                AgentInterface {
                    url: "https://grpc.example.com".to_string(),
                    protocol_binding: "GRPC".to_string(),
                    tenant: None,
                    protocol_version: "1.0".to_string(),
                },
                AgentInterface {
                    url: "https://rpc.example.com".to_string(),
                    protocol_binding: "JSONRPC".to_string(),
                    tenant: None,
                    protocol_version: "1.0".to_string(),
                },
            ],
        );
        let result = validate_agent_card_for_jsonrpc_mvp(&card, true, None).unwrap();
        assert_eq!(result.rpc_url, "https://rpc.example.com");
        assert_eq!(result.protocol_version, "1.0");
        assert!(result.streaming_supported);
    }

    #[test]
    fn validate_agent_card_rejects_missing_streaming_when_required() {
        let card = make_card(
            Some(false),
            vec![AgentInterface {
                url: "https://rpc.example.com".to_string(),
                protocol_binding: "JSONRPC".to_string(),
                tenant: None,
                protocol_version: "1.0".to_string(),
            }],
        );
        let err = validate_agent_card_for_jsonrpc_mvp(&card, true, None).unwrap_err();
        match err {
            A2AClientError::CapabilityMismatch(msg) => {
                assert!(msg.contains("streaming"));
            }
            other => panic!("expected CapabilityMismatch, got {:?}", other),
        }
    }

    #[test]
    fn validate_agent_card_rejects_no_jsonrpc_interface() {
        let card = make_card(
            Some(true),
            vec![AgentInterface {
                url: "https://grpc.example.com".to_string(),
                protocol_binding: "GRPC".to_string(),
                tenant: None,
                protocol_version: "1.0".to_string(),
            }],
        );
        let err = validate_agent_card_for_jsonrpc_mvp(&card, false, None).unwrap_err();
        match err {
            A2AClientError::InvalidAgentCard(msg) => {
                assert!(msg.contains("no JSONRPC"));
            }
            other => panic!("expected InvalidAgentCard, got {:?}", other),
        }
    }

    #[test]
    fn validate_agent_card_selects_preferred_skill() {
        let mut card = make_card(
            Some(true),
            vec![AgentInterface {
                url: "https://rpc.example.com".to_string(),
                protocol_binding: "JSONRPC".to_string(),
                tenant: None,
                protocol_version: "1.0".to_string(),
            }],
        );
        card.skills = vec![
            AgentSkill {
                id: "skill-1".to_string(),
                name: "Code Review".to_string(),
                description: "Review code".to_string(),
                tags: vec!["review".to_string()],
                examples: vec![],
                input_modes: vec!["text/plain".to_string()],
                output_modes: vec!["text/plain".to_string()],
                security_requirements: vec![],
            },
            AgentSkill {
                id: "skill-2".to_string(),
                name: "Implementation".to_string(),
                description: "Implement".to_string(),
                tags: vec!["impl".to_string()],
                examples: vec![],
                input_modes: vec!["text/plain".to_string()],
                output_modes: vec!["text/plain".to_string()],
                security_requirements: vec![],
            },
        ];
        let result = validate_agent_card_for_jsonrpc_mvp(&card, true, Some("impl")).unwrap();
        assert_eq!(result.selected_skill_id, Some("skill-2".to_string()));
    }
}
