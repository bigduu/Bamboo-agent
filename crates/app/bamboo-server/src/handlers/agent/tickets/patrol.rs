//! One explicit owner read, without policy installation or runtime activation.
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRequest {
    pub project_id: String,
    pub work_id: String,
    pub assignment_id: String,
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/patrol/status", web::post().to(status));
}

pub async fn status(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<StatusRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let principal = user(&state, &req).await?;
    let result = state
        .tickets
        .patrol_status(
            principal,
            &body.project_id,
            &body.work_id,
            &body.assignment_id,
        )
        .await?;
    user(&state, &req).await?;
    Ok(HttpResponse::Ok().json(result))
}

#[cfg(test)]
mod tests;
