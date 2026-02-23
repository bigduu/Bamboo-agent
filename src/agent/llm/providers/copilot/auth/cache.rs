use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;

/// Token cache for Copilot
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenCache {
    pub token: String,
    #[serde(rename = "expires_at")]
    pub expires_at: u64,
}

impl TokenCache {
    /// Get cache file path
    pub fn cache_path() -> PathBuf {
        chat_core::paths::bamboo_dir().join("copilot_token.json")
    }

    /// Load token from cache
    pub async fn load() -> Option<Self> {
        let path = Self::cache_path();
        if !path.exists() {
            return None;
        }

        let content = fs::read_to_string(&path).await.ok()?;
        let cache: TokenCache = serde_json::from_str(&content).ok()?;

        Some(cache)
    }

    /// Save token to cache
    pub async fn save(&self) -> Result<(), String> {
        let path = Self::cache_path();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create cache directory: {}", e))?;
        }

        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize token cache: {}", e))?;

        fs::write(&path, content)
            .await
            .map_err(|e| format!("Failed to write token cache: {}", e))?;

        // Set restrictive permissions (Unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            let _ = std::fs::set_permissions(&path, perms);
        }

        Ok(())
    }

    /// Delete cache file
    pub async fn delete() -> Result<(), String> {
        let path = Self::cache_path();
        if path.exists() {
            fs::remove_file(&path)
                .await
                .map_err(|e| format!("Failed to delete token cache: {}", e))?;
        }
        Ok(())
    }

    /// Check if token is still valid
    /// Returns true if token has more than 5 minutes remaining
    pub fn is_valid(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Consider expired 5 minutes before actual expiry
        self.expires_at > now + 300
    }

    /// Get remaining time in seconds
    pub fn remaining_seconds(&self) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.expires_at as i64 - now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_cache_valid() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Valid: expires in 1 hour
        let cache = TokenCache {
            token: "test".to_string(),
            expires_at: now + 3600,
        };
        assert!(cache.is_valid());

        // Invalid: expires in 1 minute
        let cache = TokenCache {
            token: "test".to_string(),
            expires_at: now + 60,
        };
        assert!(!cache.is_valid());
    }
}
