use actix_web::{web, HttpResponse, Responder};

use crate::state::AppState;

pub async fn handler(_state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "session_id": path.into_inner(),
        "messages": []
    }))
}
