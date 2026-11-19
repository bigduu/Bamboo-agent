//! Owner HTTP boundary; no model-facing tool or role from JSON.
use super::*;
use crate::app_state::ticket_application::WorkTokenBudgetSetup;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetQuery {
    pub configuration_revision: Option<u64>,
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/token-budget")
            .route(web::get().to(status))
            .route(web::put().to(setup)),
    );
}

pub async fn status(
    state: web::Data<AppState>,
    req: HttpRequest,
    query: web::Query<BudgetQuery>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .work_token_budget_status(user(&state, &req).await?, query.configuration_revision)
            .await?,
    ))
}

pub async fn setup(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<WorkTokenBudgetSetup>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .configure_work_token_budget(user(&state, &req).await?, body.into_inner())
            .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{dev::Service, http::Method, test, App};
    use serde_json::json;

    #[actix_web::test]
    async fn verified_worker_credentials_cannot_read_or_configure_owner_budget() {
        let home = tempfile::tempdir().unwrap();
        let physical = home.path().canonicalize().unwrap();
        let state = web::Data::new(
            Box::pin(AppState::new_with_memory_store(
                physical.clone(),
                bamboo_memory::memory_store::MemoryStore::new(physical.join("jiandu")),
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
                            session_id: "verified-budget-worker".into(),
                        });
                    service.call(request)
                })
                .configure(crate::routes::configure_routes),
        )
        .await;
        for method in [Method::GET, Method::PUT] {
            let response = test::call_service(&app, test::TestRequest::default()
                .method(method).uri("/api/v1/tickets/token-budget")
                .peer_addr("127.0.0.1:43210".parse().unwrap())
                .set_json(json!({"binding":{"scope_id":"forged","supervisor_session_id":"supervisor","binding_revision":1},
                    "authority_epoch":1,"expected_revision":0,"limits":{"work_tokens":{},"utc_day_tokens":1,"total_tokens":1}}))
                .to_request()).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        assert!(state.tickets.service().is_err());
        state.shutdown().await;
    }
}
