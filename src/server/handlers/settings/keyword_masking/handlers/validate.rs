use crate::{core::keyword_masking::KeywordEntry, server::error::AppError};
use actix_web::{web, HttpResponse};

use super::super::validation::validate_entries_only;
use super::payload::{validation_error_payload, validation_success_payload};

/// Validates keyword masking entries without saving.
pub async fn validate_keyword_entries(
    payload: web::Json<Vec<KeywordEntry>>,
) -> Result<HttpResponse, AppError> {
    let body = match validate_entries_only(payload.into_inner()) {
        Ok(()) => validation_success_payload(),
        Err(validation_errors) => validation_error_payload(validation_errors),
    };

    Ok(HttpResponse::Ok().json(body))
}
