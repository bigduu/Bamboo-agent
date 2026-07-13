use std::path::{Path, PathBuf};

use crate::error::AppError;
use bamboo_compression::limits::ModelLimit;
use bamboo_llm::Config;
use serde_json::{Map, Value};

use super::super::super::redaction::redact_config_for_api;

const MODEL_LIMITS_KEY: &str = "model_limits";

pub(super) fn config_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("config.json")
}

pub(super) fn model_limits_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("model_limits.json")
}

/// The standalone connect.json sibling of config.json (#455) — bamboo-connect
/// platform-bridge credentials (bot tokens, allowlists) live here, split out
/// of config.json by `Config::save_to_dir`/`Config::merge_connect_config`.
pub(super) fn connect_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("connect.json")
}

pub(super) async fn read_model_limits_file(app_data_dir: &Path) -> Result<Option<Value>, AppError> {
    let path = model_limits_file_path(app_data_dir);
    match tokio::fs::try_exists(&path).await {
        Ok(false) => Ok(None),
        Ok(true) => {
            let content = tokio::fs::read_to_string(&path)
                .await
                .map_err(AppError::StorageError)?;
            let limits: Vec<ModelLimit> = serde_json::from_str(&content)?;
            Ok(Some(serde_json::to_value(limits)?))
        }
        Err(error) => Err(AppError::StorageError(error)),
    }
}

pub(super) async fn write_model_limits_file(
    app_data_dir: &Path,
    value: Option<&Value>,
) -> Result<(), AppError> {
    let path = model_limits_file_path(app_data_dir);
    match value {
        None => remove_file_if_exists(&path).await,
        Some(value) => {
            // Persist ALL user-provided overrides, including rows that happen to
            // match the current global default. The previous diff-only design
            // silently dropped rows matching the compile-time default, which made
            // it impossible to explicitly pin a model whose real limit equals the
            // default — and when the default later changed, those implicit
            // settings were lost.
            let limits: Vec<ModelLimit> = serde_json::from_value(value.clone())?;

            if limits.is_empty() {
                return remove_file_if_exists(&path).await;
            }

            let content = serde_json::to_string_pretty(&limits)?;
            atomic_write(&path, content.as_bytes()).await
        }
    }
}

/// Write `bytes` to `path` atomically: write to a unique temp file in the same
/// directory, fsync it, then rename over the target (rename is atomic on POSIX),
/// and fsync the parent dir to persist the rename. A crash mid-write leaves the
/// old file intact and a stray `.tmp.*` rather than a half-written, corrupt
/// `model_limits.json` — parity with config.json's atomic write (#42).
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let tmp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    {
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(AppError::StorageError)?;
        tokio::io::AsyncWriteExt::write_all(&mut file, bytes)
            .await
            .map_err(AppError::StorageError)?;
        file.sync_all().await.map_err(AppError::StorageError)?;
    }
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(AppError::StorageError)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = tokio::fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }
    Ok(())
}

async fn remove_file_if_exists(path: &Path) -> Result<(), AppError> {
    if tokio::fs::try_exists(path)
        .await
        .map_err(AppError::StorageError)?
    {
        tokio::fs::remove_file(path)
            .await
            .map_err(AppError::StorageError)?;
    }
    Ok(())
}

pub(super) fn take_model_limits_patch(patch_obj: &mut Map<String, Value>) -> Option<Value> {
    patch_obj.remove(MODEL_LIMITS_KEY)
}

pub(super) async fn redacted_config_json(
    config: &Config,
    app_data_dir: &Path,
) -> Result<Value, AppError> {
    let mut config_for_response = config.clone();
    config_for_response.refresh_proxy_auth_encrypted()?;
    config_for_response.refresh_provider_api_keys_encrypted()?;
    // A secret set earlier in THIS request only exists as plaintext on
    // `config` until `save_to_dir` (which clones internally) refreshes it —
    // refresh here too so the immediate response's redaction sees it as
    // "configured" (mirrors the provider-API-key refresh above).
    config_for_response.refresh_notifications_encrypted()?;
    config_for_response.refresh_connect_platform_tokens_encrypted()?;
    let value = serde_json::to_value(&config_for_response)?;
    let mut redacted = redact_config_for_api(value, &config_for_response);

    if let Some(model_limits) = read_model_limits_file(app_data_dir).await? {
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(MODEL_LIMITS_KEY.to_string(), model_limits);
        }
    } else if let Some(value) = config.extra.get(MODEL_LIMITS_KEY) {
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(MODEL_LIMITS_KEY.to_string(), value.clone());
        }
    }

    Ok(redacted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// `rows` model limits all tagged with `ctx`: a multi-KB payload that's
    /// internally identifiable (every row carries the same `ctx`, so a
    /// cross-writer mix is detectable).
    fn limits(ctx: u32, rows: u32) -> Value {
        let v: Vec<ModelLimit> = (0..rows)
            .map(|r| ModelLimit {
                model_pattern: format!("model-{ctx}-{r}"),
                max_context_tokens: ctx,
                max_output_tokens: Some(ctx),
                safety_margin: Some(ctx),
            })
            .collect();
        serde_json::to_value(v).unwrap()
    }

    /// Concurrent writers replace model_limits.json while a reader hammers it.
    /// Asserts the invariants the atomic temp+rename gives us: every read that
    /// finds the file parses as complete/valid model_limits, the final file is
    /// exactly one writer's internally-consistent payload, and no temp files
    /// leak.
    ///
    /// NOTE: this is a concurrency + invariant smoke test, NOT a deterministic
    /// torn-read reproducer. The `O_TRUNC`→write window a non-atomic
    /// `tokio::fs::write` exposes is microsecond-scale and was empirically not
    /// reliably hittable in-process here (even with ~1MB payloads + a tight
    /// reader), so the reader assertion is best-effort. The atomicity itself is
    /// guaranteed structurally by `atomic_write` (temp + fsync + rename), not by
    /// this test racing the window.
    #[tokio::test]
    async fn concurrent_model_limits_writes_are_atomic_for_readers() {
        let dir = tempfile::tempdir().unwrap();
        let app_dir = dir.path().to_path_buf();
        let path = model_limits_file_path(&app_dir);

        const ROWS: u32 = 256; // ~25KB payload.

        // Seed a valid file so the reader always has something to read.
        write_model_limits_file(&app_dir, Some(&limits(1, ROWS)))
            .await
            .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let path = path.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut reads = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(content) = tokio::fs::read_to_string(&path).await {
                        serde_json::from_str::<Vec<ModelLimit>>(&content)
                            .expect("reader observed a torn / half-written model_limits.json");
                        reads += 1;
                    }
                    tokio::task::yield_now().await;
                }
                reads
            })
        };

        const N: u32 = 24;
        let mut handles = Vec::with_capacity(N as usize);
        for i in 2..2 + N {
            let app_dir = app_dir.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..8 {
                    write_model_limits_file(&app_dir, Some(&limits(i, ROWS)))
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let reads = reader.await.unwrap();
        assert!(
            reads > 0,
            "reader actually observed the file during the writes"
        );

        // Final state is exactly one writer's complete, internally-consistent payload.
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let parsed: Vec<ModelLimit> = serde_json::from_str(&content).unwrap();
        assert_eq!(
            parsed.len(),
            ROWS as usize,
            "a complete writer payload survives"
        );
        let ctx = parsed[0].max_context_tokens;
        assert!(
            parsed.iter().all(|l| l.max_context_tokens == ctx),
            "every row is from the SAME writer (no cross-writer mix)"
        );

        // The atomic write renames its temp over the target; none should leak.
        let mut entries = tokio::fs::read_dir(&app_dir).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            let name = e.file_name();
            assert!(
                !name.to_string_lossy().contains(".tmp."),
                "atomic write left a stray temp file: {name:?}"
            );
        }
    }
}
