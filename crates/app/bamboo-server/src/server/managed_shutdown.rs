//! Cooperative shutdown for a desktop host's own Bamboo process.

use actix_server::Server;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Private, per-process directory supplied by a host using `serve --parent-pid`.
/// Bamboo creates `ready`; the owner requests shutdown by creating an empty
/// `stop` file and then waits for Bamboo to exit.
pub const MANAGED_SHUTDOWN_DIR_ENV: &str = "BAMBOO_MANAGED_SHUTDOWN_DIR";

pub(super) struct ManagedShutdown {
    directory: PathBuf,
}

impl ManagedShutdown {
    pub(super) fn new(directory: PathBuf) -> io::Result<Self> {
        if !directory.is_absolute() || !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed shutdown requires an absolute, private directory",
            ));
        }
        for name in ["ready", "stop"] {
            match fs::symlink_metadata(directory.join(name)) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error),
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "managed shutdown directory already contains a marker",
                    ));
                }
            }
        }
        Ok(Self { directory })
    }

    pub(super) async fn run(self, server: Server) -> io::Result<()> {
        File::create_new(self.directory.join("ready"))?;
        let handle = server.handle();
        let marker = self.directory.join("stop");
        tokio::pin!(server);

        tokio::select! {
            result = &mut server => result,
            () = wait_for_marker(&marker) => {
                tracing::info!("Managed Bamboo shutdown requested");
                let ((), result) = tokio::join!(handle.stop(true), server);
                result
            }
        }
    }
}

async fn wait_for_marker(marker: &Path) {
    loop {
        if fs::symlink_metadata(marker)
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() == 0)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{web, App, HttpResponse, HttpServer};
    use std::net::TcpListener;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[test]
    fn refuses_to_reuse_an_existing_marker() {
        let directory = tempfile::tempdir().unwrap();
        File::create_new(directory.path().join("stop")).unwrap();
        assert!(ManagedShutdown::new(directory.path().to_path_buf()).is_err());
    }

    #[actix_web::test]
    async fn stop_marker_allows_a_serving_request_to_finish() {
        let directory = tempfile::tempdir().unwrap();
        let shutdown = ManagedShutdown::new(directory.path().to_path_buf()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let in_handler = entered.clone();
        let allow_finish = finish.clone();
        let server = HttpServer::new(move || {
            let entered = in_handler.clone();
            let finish = allow_finish.clone();
            App::new().route(
                "/",
                web::get().to(move || {
                    let entered = entered.clone();
                    let finish = finish.clone();
                    async move {
                        entered.notify_one();
                        finish.notified().await;
                        HttpResponse::Ok().body("finished")
                    }
                }),
            )
        })
        .disable_signals()
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let stopping = actix_web::rt::spawn(shutdown.run(server));
        let request = actix_web::rt::spawn(async move {
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!("http://{address}/"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(directory.path().join("ready").is_file());
        File::create_new(directory.path().join("stop")).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::TcpStream::connect(address).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!stopping.is_finished());
        finish.notify_one();
        assert_eq!(request.await.unwrap(), "finished");
        tokio::time::timeout(Duration::from_secs(5), stopping)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
