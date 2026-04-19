use bamboo_infrastructure::keyword_masking::KeywordEntry;
use crate::error::AppError;
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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure::keyword_masking::{KeywordEntry, MatchType};
    use actix_web::{test, web, App};

    #[actix_web::test]
    async fn test_validate_empty_entries() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload: Vec<KeywordEntry> = vec![];
        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], true);
    }

    #[actix_web::test]
    async fn test_validate_valid_entries() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload = vec![KeywordEntry {
            pattern: "secret".to_string(),
            match_type: MatchType::Exact,
            enabled: true,
        }];

        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], true);
    }

    #[actix_web::test]
    async fn test_validate_invalid_regex_pattern() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload = vec![KeywordEntry {
            pattern: "[invalid(regex".to_string(),
            match_type: MatchType::Regex,
            enabled: true,
        }];

        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], false);
        assert!(body["errors"].as_array().unwrap().len() > 0);
    }

    #[actix_web::test]
    async fn test_validate_multiple_entries_with_mixed_validity() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload = vec![
            KeywordEntry {
                pattern: "valid".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            },
            KeywordEntry {
                pattern: "[invalid".to_string(),
                match_type: MatchType::Regex,
                enabled: true,
            },
        ];

        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], false);
    }

    #[actix_web::test]
    async fn test_validate_valid_regex_pattern() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload = vec![KeywordEntry {
            pattern: r"\d{4}-\d{4}-\d{4}-\d{4}".to_string(),
            match_type: MatchType::Regex,
            enabled: true,
        }];

        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], true);
    }

    #[actix_web::test]
    async fn test_validate_disabled_entry() {
        let app = test::init_service(
            App::new().route("/validate", web::post().to(validate_keyword_entries)),
        )
        .await;

        let payload = vec![KeywordEntry {
            pattern: "test".to_string(),
            match_type: MatchType::Exact,
            enabled: false,
        }];

        let req = test::TestRequest::post()
            .uri("/validate")
            .set_json(&payload)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["valid"], true);
    }
}
