//! Manual dev tool for driving `ServerPluginInstaller`/`plugin_source` before
//! the CLI (`bamboo plugin install`) / HTTP (`/api/v1/plugins`) Wave-2
//! branches land. NOT a production entry point — just the quickest way to
//! exercise the installer seam end to end against a real (throwaway)
//! `--data-dir` while those branches are still in flight.
//!
//! Usage:
//! ```text
//! cargo run -p bamboo-server --example install_plugin -- \
//!     install <data-dir> <local-plugin-dir>
//! cargo run -p bamboo-server --example install_plugin -- \
//!     upgrade <data-dir> <local-plugin-dir>
//! cargo run -p bamboo-server --example install_plugin -- \
//!     uninstall <data-dir> <plugin-id>
//! ```

use std::path::PathBuf;

use bamboo_plugin::{InstallDisposition, PluginInstaller};
use bamboo_server::plugin_installer::ServerPluginInstaller;
use bamboo_server::plugin_source::{install_server_plugin_from_source, PluginSourceInput};
use bamboo_server::AppState;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage =
        "usage: install_plugin <install|upgrade|uninstall|list> <data-dir> [plugin-dir-or-id]";
    let Some(verb) = args.get(1) else {
        eprintln!("{usage}");
        std::process::exit(2);
    };
    let Some(data_dir) = args.get(2).map(PathBuf::from) else {
        eprintln!("{usage}");
        std::process::exit(2);
    };

    let state = AppState::new(data_dir.clone())
        .await
        .expect("failed to initialize AppState at the given --data-dir");
    let data = actix_web::web::Data::new(state);
    let installer = ServerPluginInstaller::new(data.clone());

    match verb.as_str() {
        "install" | "upgrade" => {
            let Some(plugin_dir) = args.get(3).map(PathBuf::from) else {
                eprintln!("{usage}");
                std::process::exit(2);
            };
            let disposition = if verb == "upgrade" {
                InstallDisposition::Upgrade
            } else {
                InstallDisposition::FailIfInstalled
            };
            let plugins_root = data_dir.join("plugins");
            let trust = data.config.read().await.plugin_trust.clone();
            match install_server_plugin_from_source(
                &installer,
                PluginSourceInput::LocalDir(plugin_dir),
                &plugins_root,
                &trust,
                disposition,
                None,
            )
            .await
            {
                Ok(entry) => {
                    println!(
                        "installed: {}\nregistered: {:#?}",
                        entry.id, entry.registered
                    );
                }
                Err(error) => {
                    eprintln!("install failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        "uninstall" => {
            let Some(id) = args.get(3) else {
                eprintln!("{usage}");
                std::process::exit(2);
            };
            match installer.uninstall(id).await {
                Ok(()) => println!("uninstalled: {id}"),
                Err(error) => {
                    eprintln!("uninstall failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        "list" => match installer.list().await {
            Ok(plugins) => {
                for plugin in plugins {
                    println!(
                        "{} v{} -> {}",
                        plugin.id,
                        plugin.version,
                        plugin.plugin_dir.display()
                    );
                }
            }
            Err(error) => {
                eprintln!("list failed: {error}");
                std::process::exit(1);
            }
        },
        _ => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    }
}
