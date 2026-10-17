//! Owner-authenticated planning routes over the existing Ticket authority.
use super::*;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/sprints")
            .route("/list", web::post().to(list))
            .route("/inspect", web::post().to(inspect))
            .route("/command", web::post().to(command)),
    );
}

pub async fn list(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<SprintListRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    state
        .project_store
        .get(&body.project_id)
        .map_err(|e| Error::InvalidTransition(e.to_string()))?;
    Ok(HttpResponse::Ok().json(service.list_sprints(&authority, &body)?))
}

pub async fn inspect(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<SprintInspectRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    state
        .project_store
        .get(&body.project_id)
        .map_err(|e| Error::InvalidTransition(e.to_string()))?;
    Ok(HttpResponse::Ok().json(service.inspect_sprint(&authority, &body)?))
}

pub async fn command(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<SprintCommand>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .sprint_command(user(&state, &req).await?, &body)
            .await?,
    ))
}
