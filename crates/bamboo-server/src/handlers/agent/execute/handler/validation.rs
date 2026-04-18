#[cfg(test)]
use actix_web::HttpResponse;

#[cfg(test)]
pub(super) fn validate_and_normalize_model(
    model: Option<&str>,
) -> Result<Option<String>, HttpResponse> {
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
