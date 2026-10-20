//! Scalar preparation only: the host still must observe this dispatch's actual cap.
use bamboo_agent_core::tools::ToolError;

/// Compose a finite Skill page-envelope ceiling with a known current token cap.
///
/// The bundled/default counters emit at most one token per UTF-8 byte, so a
/// complete page within this envelope also fits the positive hard token cap.
/// Zero retains the Engine's no-hard-cap meaning, with a finite byte ceiling.
/// This helper authorizes nothing and is not connected to a live resolver.
pub fn skill_response_byte_budget(
    response_bytes: usize,
    current_max_tool_output_tokens: Option<u32>,
) -> Result<usize, ToolError> {
    let tokens = current_max_tool_output_tokens.ok_or_else(|| {
        ToolError::Execution("Skill response requires a known current tool-output cap".into())
    })?;
    if response_bytes == 0 {
        return Err(ToolError::Execution(
            "Skill response envelope has no usable byte budget".into(),
        ));
    }
    let bytes = response_bytes.min(super::MAX_SKILLS_LIST_BYTES);
    Ok(if tokens == 0 {
        bytes
    } else {
        bytes.min(tokens as usize)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_budget_distinguishes_unknown_zero_and_positive_caps() {
        assert!(skill_response_byte_budget(8_000, None).is_err());
        assert!(skill_response_byte_budget(0, Some(0)).is_err());
        assert!(skill_response_byte_budget(0, Some(32)).is_err());
        assert_eq!(skill_response_byte_budget(8_000, Some(0)).unwrap(), 8_000);
        assert_eq!(skill_response_byte_budget(8_000, Some(1)).unwrap(), 1);
        assert_eq!(skill_response_byte_budget(8_000, Some(256)).unwrap(), 256);
        assert_eq!(
            skill_response_byte_budget(8_000, Some(16_000)).unwrap(),
            8_000
        );
    }

    #[test]
    fn skill_budget_stays_finite_at_integer_and_page_bounds() {
        let ceiling = 512 * 1024;
        for response in [1, 255, ceiling - 1, ceiling, ceiling + 1, usize::MAX] {
            for cap in [0, 1, 256, ceiling as u32, u32::MAX] {
                let bytes = skill_response_byte_budget(response, Some(cap)).unwrap();
                assert!(bytes > 0 && bytes <= response && bytes <= ceiling);
                if cap > 0 {
                    assert!(bytes <= cap as usize);
                }
            }
        }
        assert_eq!(
            skill_response_byte_budget(usize::MAX, Some(0)).unwrap(),
            ceiling
        );
        assert_eq!(
            skill_response_byte_budget(usize::MAX, Some(u32::MAX)).unwrap(),
            ceiling
        );
    }
}
