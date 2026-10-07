#[cfg(test)]
use crate::error::ResponseResult;

#[cfg(test)]
pub(super) fn validate_and_normalize_model(model: Option<&str>) -> ResponseResult<Option<String>> {
    let Some(model) = model else {
        return Ok(None);
    };
    let normalized = model.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    if normalized == "unknown" {
        return Ok(None);
    }
    Ok(Some(normalized.to_string()))
}
