use actix_web::web;

use super::{
    get_available_tools, get_available_workflows, get_filtered_tools, get_skill, list_skills,
};

/// Configure skill routes
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/skills", web::get().to(list_skills))
        .route("/skills/{id}", web::get().to(get_skill))
        .route(
            "/skills/available-tools",
            web::get().to(get_available_tools),
        )
        .route("/skills/filtered-tools", web::get().to(get_filtered_tools))
        .route(
            "/skills/available-workflows",
            web::get().to(get_available_workflows),
        );
}
