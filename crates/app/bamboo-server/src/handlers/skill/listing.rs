use std::collections::{HashMap, HashSet};

use actix_web::{web, HttpResponse};
use bamboo_skills::catalog::{WorkflowCatalogSnapshot, WorkflowStatus};
use bamboo_skills::store::SkillQuality;
use bamboo_skills::types::{SkillDefinition, SkillFilter};

use crate::app_state::AppState;
use crate::error::AppError;

use super::types::{
    ListSkillsQuery, SkillAvailability, SkillListResponse, SkillManagementEntry,
    SkillManagementSnapshot,
};

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
    let (skills, management) = if query.include_management.unwrap_or(false) {
        let (mut skills, catalog, quality) = store
            .skills_catalog_and_quality_snapshot(None, refresh)
            .await;
        let management = project_management(catalog, &skills, &quality);
        skills.retain(|skill| filter.matches(skill));
        (skills, Some(management))
    } else {
        let (skills, _) = store
            .skills_and_catalog_snapshot(Some(filter), refresh)
            .await;
        (skills, None)
    };
    let skills = skills
        .into_iter()
        .filter(|skill| include_disabled || !disabled_skill_ids.contains(&skill.id))
        .collect::<Vec<_>>();

    Ok(HttpResponse::Ok().json(SkillListResponse {
        total: skills.len(),
        skills,
        management,
    }))
}

fn project_management(
    catalog: WorkflowCatalogSnapshot,
    skills: &[SkillDefinition],
    quality: &HashMap<String, SkillQuality>,
) -> SkillManagementSnapshot {
    let readable_ids: HashSet<_> = skills.iter().map(|skill| skill.id.as_str()).collect();
    let entries: Vec<_> = catalog
        .entries
        .into_iter()
        .filter(|entry| entry.winner)
        .map(|entry| {
            let availability = match (entry.status, readable_ids.contains(entry.id.as_str())) {
                (WorkflowStatus::Valid, true) => SkillAvailability::Ready,
                (WorkflowStatus::Invalid, true) => SkillAvailability::Retained,
                (_, false) => SkillAvailability::ViewOnly,
            };
            let quality = quality
                .get(&entry.id)
                .cloned()
                .unwrap_or_else(SkillQuality::unavailable);
            SkillManagementEntry {
                id: entry.id,
                name: entry.name,
                description: entry.description,
                source: entry.source,
                revision: entry.revision,
                availability,
                quality,
            }
        })
        .collect();
    SkillManagementSnapshot {
        revision: catalog.revision,
        total: entries.len(),
        entries,
    }
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
