//! Owner control only: never registered as a model/Worker tool.
use super::*;
use crate::app_state::ticket_application::ReadyPolicyRequest;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/ready-policy")
            .route(web::get().to(status))
            .route(web::put().to(control)),
    );
}

pub async fn status(
    state: web::Data<AppState>,
    req: HttpRequest,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .ready_policy_status(user(&state, &req).await?)
            .await?,
    ))
}

pub async fn control(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<ReadyPolicyRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .control_ready_policy(user(&state, &req).await?, body.into_inner())
            .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{dev::Service, http::Method, test, App};
    use serde_json::json;

    #[actix_web::test]
    async fn verified_worker_credentials_cannot_observe_or_install_owner_policy() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            Box::pin(AppState::new_with_memory_store(
                home.path().into(),
                bamboo_memory::memory_store::MemoryStore::new(home.path().join("jiandu")),
            ))
            .await
            .unwrap(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .wrap_fn(|request, service| {
                    request
                        .extensions_mut()
                        .insert(crate::codex_run_tokens::CodexRunAuthContext {
                            session_id: "real-worker-auth-context".into(),
                        });
                    service.call(request)
                })
                .configure(crate::routes::configure_routes),
        )
        .await;
        for method in [Method::GET, Method::PUT] {
            let response = test::call_service(
                &app,
                test::TestRequest::default()
                    .method(method)
                    .uri("/api/v1/tickets/ready-policy")
                    .peer_addr("127.0.0.1:43210".parse().unwrap())
                    .set_json(
                        json!({"process_id":uuid::Uuid::new_v4(),"operation_id":"forged",
                    "expected_revision":0,"action":{"kind":"revoke"}}),
                    )
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        assert_eq!(
            state
                .tickets
                .ready_policy_status(Principal::User {
                    user_id: "host-owner".into()
                })
                .await
                .unwrap()["revision"],
            0
        );
        assert!(state.tickets.service().is_err());
        state.shutdown().await;
    }
}
