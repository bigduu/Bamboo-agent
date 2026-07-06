//! The `bamboo skills list` / `bamboo mcp list` read CLI.
//!
//! These inspect the configured skill and MCP surfaces the way the running
//! server does, but OFFLINE — straight from `<data_dir>` — so a headless / CI /
//! server-less user can see what the agent would load without first booting
//! `bamboo serve` and hitting `GET /api/v1/skills` or `/api/v1/mcp/servers`
//! (both of which sit behind the access-password middleware). The config /
//! skills store are the single source of truth; this module only resolves the
//! data dir and pretty-prints.

use std::path::PathBuf;

use bamboo_skills::{SkillStore, SkillStoreConfig};
use colored::Colorize;

/// Resolve the effective data dir (`--data-dir` or `~/.bamboo`).
fn resolve_data_dir(data_dir: Option<PathBuf>) -> PathBuf {
    data_dir.unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir)
}

/// Truncate a cell to `max` chars with an ellipsis, so table columns line up.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let head: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{head}…")
}

/// `bamboo skills list` — the skills the agent would discover under
/// `<data_dir>/skills`, read directly from disk (no server).
pub async fn skills_list(data_dir: Option<PathBuf>) -> anyhow::Result<()> {
    let data_dir = resolve_data_dir(data_dir);
    let skills_dir = data_dir.join("skills");

    // Same construction the server uses (app_state/init.rs), minus project-local
    // and mode-specific discovery — this is the global surface.
    let store = SkillStore::new(SkillStoreConfig {
        skills_dir: skills_dir.clone(),
        project_dir: None,
        active_mode: None,
    });
    let skills = store.list_skills(None, true).await;

    if skills.is_empty() {
        println!(
            "(no skills in {})",
            bamboo_config::paths::path_to_display_string(&skills_dir)
        );
        return Ok(());
    }

    // Plain (un-colored) header cells so the widths line up — ANSI escapes would
    // otherwise be counted against the padding.
    println!("{:<28} {:<32} DESCRIPTION", "ID", "NAME");
    for skill in &skills {
        println!(
            "{:<28} {:<32} {}",
            truncate(&skill.id, 28),
            truncate(&skill.name, 32),
            truncate(&skill.description, 60)
        );
    }
    println!("\n{} skill(s).", skills.len());
    Ok(())
}

/// `bamboo mcp list` — the MCP servers configured in `<data_dir>/config.json`,
/// read directly (no server). Live connection status / tool counts require a
/// running server (`GET /api/v1/mcp/servers`); this is the static config view.
pub async fn mcp_list(data_dir: Option<PathBuf>) -> anyhow::Result<()> {
    use bamboo_domain::mcp_config::TransportConfig;

    let data_dir = resolve_data_dir(data_dir);
    let config = bamboo_config::Config::from_data_dir(Some(data_dir));
    let servers = &config.mcp.servers;

    if servers.is_empty() {
        println!("(no MCP servers configured)");
        return Ok(());
    }

    // Plain cells (no ANSI in the table) so the columns line up; color is used
    // only in the summary line below.
    println!("{:<24} {:<8} {:<16} NAME", "ID", "ENABLED", "TRANSPORT");
    for server in servers {
        let transport = match &server.transport {
            TransportConfig::Stdio(_) => "stdio",
            TransportConfig::Sse(_) => "sse",
            TransportConfig::StreamableHttp(_) => "streamable_http",
        };
        let name = server.name.as_deref().unwrap_or("");
        println!(
            "{:<24} {:<8} {:<16} {}",
            truncate(&server.id, 24),
            if server.enabled { "yes" } else { "no" },
            transport,
            name
        );
    }
    let live = servers.iter().filter(|s| s.enabled).count();
    println!(
        "\n{live} enabled of {}. Live status: {}",
        servers.len(),
        "bamboo status".cyan()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_shortens_long_cells() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello-world", 5), "hell…");
    }

    #[tokio::test]
    async fn skills_list_empty_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        // A data dir with no skills/ subtree must not error — just report empty.
        assert!(skills_list(Some(dir.path().to_path_buf())).await.is_ok());
    }
}
