use super::*;

#[actix_web::test]
async fn test_validate_config_patch_reports_domain_errors() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    // Invalid proxy URL should be reported under proxy domain.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(json!({
            "http_proxy": "http://"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["valid"], false);
    assert!(!result["errors"]["proxy"].as_array().unwrap().is_empty());

    // Invalid setup shape should be reported under setup domain.
    let req = test::TestRequest::post()
        .uri("/v1/bamboo/config/validate")
        .set_json(json!({
            "setup": "nope"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["valid"], false);
    assert!(!result["errors"]["setup"].as_array().unwrap().is_empty());
}
