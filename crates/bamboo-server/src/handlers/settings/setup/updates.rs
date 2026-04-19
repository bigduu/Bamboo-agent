use serde_json::Value;

use crate::error::AppError;
use bamboo_infrastructure::Config;

pub(super) fn set_setup_complete(
    config: &mut Config,
    completed_at: String,
) -> Result<(), AppError> {
    let setup_entry = config
        .extra
        .entry("setup".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let setup_obj = setup_entry
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("config.setup must be a JSON object".to_string()))?;

    setup_obj.insert("completed".to_string(), Value::Bool(true));
    setup_obj.insert("completed_at".to_string(), Value::String(completed_at));
    setup_obj.insert("version".to_string(), Value::Number(1.into()));
    Ok(())
}

pub(super) fn set_setup_incomplete(config: &mut Config, reset_at: String) {
    if let Some(setup_entry) = config.extra.get_mut("setup") {
        if let Some(setup_obj) = setup_entry.as_object_mut() {
            setup_obj.insert("completed".to_string(), Value::Bool(false));
            setup_obj.insert("reset_at".to_string(), Value::String(reset_at));
        }
    }
}
