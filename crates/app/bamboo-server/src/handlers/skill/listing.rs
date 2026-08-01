use actix_web::{web, HttpResponse};
use bamboo_skills::types::SkillFilter;

use crate::app_state::AppState;
use crate::error::AppError;

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
    let include_disabled = query.include_disabled.unwrap_or(false);
    let disabled_skill_ids = {
        let config = state.config.read().await;
        config.disabled_skill_ids()
    };
    let store = state.skill_manager.as_ref().store();
    let (skills, _skill_catalog) = store
        .skills_and_catalog_snapshot(Some(filter), refresh)
        .await;
    let skills = skills
        .into_iter()
        .filter(|skill| include_disabled || !disabled_skill_ids.contains(&skill.id))
        .collect::<Vec<_>>();

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
    let store = state.skill_manager.as_ref().store();
    let skill = store
        .get_skill(&id)
        .await
        .map_err(|_| AppError::NotFound(format!("Skill {} not found", id)))?;

    Ok(HttpResponse::Ok().json(skill))
}
