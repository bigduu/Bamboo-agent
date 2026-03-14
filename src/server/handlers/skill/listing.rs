use crate::agent::skill::SkillFilter;
use actix_web::{web, HttpResponse};

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::types::{ListSkillsQuery, SkillListResponse};

/// GET /skills - List all skills
pub async fn list_skills(
    state: web::Data<AppState>,
    query: web::Query<ListSkillsQuery>,
) -> Result<HttpResponse, AppError> {
    let mut filter = SkillFilter::new();
    if let Some(search) = query.search.clone() {
        filter = filter.with_search(search);
    }

    let refresh = query.refresh.unwrap_or(false);
    let skills = state
        .skill_manager
        .as_ref()
        .store()
        .list_skills(Some(filter), refresh)
        .await;

    Ok(HttpResponse::Ok().json(SkillListResponse {
        total: skills.len(),
        skills,
    }))
}

/// GET /skills/{id} - Get skill detail
pub async fn get_skill(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let skill = state
        .skill_manager
        .as_ref()
        .store()
        .get_skill(&id)
        .await
        .map_err(|_| AppError::NotFound(format!("Skill {} not found", id)))?;

    Ok(HttpResponse::Ok().json(skill))
}
