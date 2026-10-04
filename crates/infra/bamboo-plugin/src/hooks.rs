//! Strict, deliberately small portable command-hook contract.
use crate::{PluginError, PluginManifest, PluginResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookDeclaration {
    pub config: String,
    /// Every executable script/dependency the reviewer must inspect. The receipt
    /// additionally hashes the complete bundle; undeclared changes cannot hide.
    pub scripts: Vec<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PortableEvent {
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    Stop,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableConfig {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(deserialize_with = "unique_events")]
    pub hooks: BTreeMap<PortableEvent, Vec<PortableGroup>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableGroup {
    #[serde(default)]
    pub matcher: Option<String>,
    pub hooks: Vec<PortableCommand>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableCommand {
    #[serde(rename = "type")]
    pub kind: CommandKind,
    pub command: String,
    /// Mandatory seconds, unlike the upstream defaults.
    pub timeout: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandKind {
    #[serde(rename = "command")]
    Command,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookState {
    NeedsReview,
    Disabled,
    Active,
    Unsupported,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookRegistration {
    pub plugin_id: String,
    pub version: String,
    pub config: String,
    pub digest: String,
    #[serde(default)]
    pub trusted_digest: Option<String>,
    #[serde(default)]
    pub enabled: bool,
}
impl HookRegistration {
    pub fn state(&self, current_digest: &str) -> HookState {
        if self.digest != current_digest || self.trusted_digest.as_deref() != Some(current_digest) {
            HookState::NeedsReview
        } else if self.enabled {
            HookState::Active
        } else {
            HookState::Disabled
        }
    }
    /// Caller must obtain an explicit review confirmation for this exact digest.
    /// A stale UI/request cannot approve replacement bytes.
    pub fn confirm_review(&mut self, reviewed_digest: &str) -> PluginResult<()> {
        if self.digest != reviewed_digest {
            return Err(invalid("stale hook review digest"));
        }
        self.trusted_digest = Some(reviewed_digest.to_owned());
        self.enabled = true;
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> PluginError {
    PluginError::InvalidManifest(message.into())
}
pub fn bundle_path(root: &Path, relative: &str) -> PluginResult<PathBuf> {
    let path = Path::new(relative);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|c| {
            !matches!(
                c,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    {
        return Err(invalid(format!("unsafe hook bundle path: {relative}")));
    }
    let canonical_root = root.canonicalize().map_err(|e| invalid(e.to_string()))?;
    let target = root
        .join(path)
        .canonicalize()
        .map_err(|e| invalid(e.to_string()))?;
    if !target.starts_with(canonical_root) {
        return Err(invalid("hook path escapes plugin root"));
    }
    Ok(target)
}
impl PortableConfig {
    pub fn parse(bytes: &[u8]) -> PluginResult<Self> {
        if bytes.len() > 64 * 1024 {
            return Err(invalid("hook config exceeds 64 KiB"));
        }
        let config: Self = serde_json::from_slice(bytes)
            .map_err(|e| invalid(format!("unsupported hook config: {e}")))?;
        let mut count = 0;
        for (event, groups) in &config.hooks {
            for group in groups {
                if let Some(matcher) = group
                    .matcher
                    .as_deref()
                    .filter(|s| !s.is_empty() && *s != "*")
                {
                    if !matches!(
                        event,
                        PortableEvent::PreToolUse | PortableEvent::PostToolUse
                    ) {
                        return Err(invalid("matcher is only supported for tool events"));
                    }
                    regex::Regex::new(matcher)
                        .map_err(|e| invalid(format!("unsupported portable matcher: {e}")))?;
                }
                if group.hooks.is_empty() {
                    return Err(invalid("empty hook group"));
                }
                for command in &group.hooks {
                    count += 1;
                    if command.command.trim().is_empty() || !(1..=600).contains(&command.timeout) {
                        return Err(invalid(
                            "command and explicit timeout (1..600 seconds) required",
                        ));
                    }
                }
            }
        }
        if count == 0 || count > 64 {
            return Err(invalid("hook config requires 1..64 commands"));
        }
        Ok(config)
    }
}
/// Hash entry types, paths and file bytes with length framing, in sorted order. Symlinks and
/// special files are rejected; plugin data lives outside the reviewed bundle.
fn hash_tree(root: &Path, dir: &Path, hash: &mut Sha256, total: &mut u64) -> PluginResult<()> {
    let mut paths = std::fs::read_dir(dir)
        .map_err(|e| invalid(e.to_string()))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(e.to_string()))?;
    paths.sort();
    for path in paths {
        let metadata = std::fs::symlink_metadata(&path).map_err(|e| invalid(e.to_string()))?;
        if metadata.is_dir() {
            let name = path.strip_prefix(root).unwrap().to_string_lossy();
            hash.update(b"directory");
            hash.update((name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash_tree(root, &path, hash, total)?;
        } else if metadata.is_file() {
            *total = total.saturating_add(metadata.len());
            if *total > 64 * 1024 * 1024 {
                return Err(invalid("reviewed hook bundle exceeds 64 MiB"));
            }
            let name = path.strip_prefix(root).unwrap().to_string_lossy();
            let bytes = std::fs::read(&path).map_err(|e| invalid(e.to_string()))?;
            hash.update(b"file");
            hash.update((name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        } else {
            return Err(invalid(
                "hook bundles cannot contain symlinks or special files",
            ));
        }
    }
    Ok(())
}
pub fn registrations(
    manifest: &PluginManifest,
    root: &Path,
) -> PluginResult<Vec<HookRegistration>> {
    if manifest.provides.hooks.len() > 16 {
        return Err(invalid("at most 16 hook configs per plugin"));
    }
    if manifest.provides.hooks.is_empty() {
        return Ok(vec![]);
    }
    let mut hash = Sha256::new();
    hash.update(manifest.id.as_bytes());
    hash.update([0]);
    hash.update(manifest.version.as_bytes());
    hash_tree(root, root, &mut hash, &mut 0)?;
    let digest = format!("{:x}", hash.finalize());
    let mut registrations = vec![];
    for declaration in &manifest.provides.hooks {
        if declaration.scripts.is_empty() {
            return Err(invalid("hook declaration requires reviewed scripts"));
        }
        for script in &declaration.scripts {
            if !bundle_path(root, script)?.is_file() {
                return Err(invalid("reviewed script is not a file"));
            }
        }
        let path = bundle_path(root, &declaration.config)?;
        PortableConfig::parse(&std::fs::read(path).map_err(|e| invalid(e.to_string()))?)?;
        if registrations
            .iter()
            .any(|r: &HookRegistration| r.config == declaration.config)
        {
            return Err(invalid("duplicate hook config"));
        }
        registrations.push(HookRegistration {
            plugin_id: manifest.id.clone(),
            version: manifest.version.clone(),
            config: declaration.config.clone(),
            digest: digest.clone(),
            trusted_digest: None,
            enabled: false,
        });
    }
    Ok(registrations)
}

fn unique_events<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<PortableEvent, Vec<PortableGroup>>, D::Error> {
    struct Events;
    impl<'de> serde::de::Visitor<'de> for Events {
        type Value = BTreeMap<PortableEvent, Vec<PortableGroup>>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("unique portable event entries")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut events = BTreeMap::new();
            while let Some((key, value)) = map.next_entry()? {
                if events.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate hook event"));
                }
            }
            Ok(events)
        }
    }
    deserializer.deserialize_map(Events)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_directory_changes_invalidate_reviewed_tree() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("script.sh"), "test -d flag").unwrap();
        let digest = || {
            let mut hash = Sha256::new();
            hash_tree(temp.path(), temp.path(), &mut hash, &mut 0).unwrap();
            format!("{:x}", hash.finalize())
        };
        let original = digest();
        let mut receipt = HookRegistration {
            plugin_id: "fixture".into(),
            version: "0.1.0".into(),
            config: "hooks.json".into(),
            digest: original.clone(),
            trusted_digest: None,
            enabled: false,
        };
        receipt.confirm_review(&original).unwrap();
        std::fs::create_dir(temp.path().join("flag")).unwrap();
        let added = digest();
        assert_eq!(receipt.state(&added), HookState::NeedsReview);
        std::fs::rename(temp.path().join("flag"), temp.path().join("renamed")).unwrap();
        assert_ne!(digest(), added);
        std::fs::remove_dir(temp.path().join("renamed")).unwrap();
        assert_eq!(digest(), original);
        assert_eq!(receipt.state(&digest()), HookState::Active);
    }

    #[test]
    fn rejects_unsupported_protocol_surface_and_regex() {
        for input in [
            r#"{"hooks":{"SessionStart":[]}}"#,
            r#"{"hooks":{"Stop":[],"Stop":[]}}"#,
            r#"{"hooks":{"PreToolUse":[{"hooks":[{"type":"command","command":"true"}]}]}}"#,
            r#"{"hooks":{"Stop":[{"hooks":[{"type":"agent","command":"true","timeout":1}]}]}}"#,
            r#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"true","timeout":1,"async":true}]}]}}"#,
            r#"{"hooks":{"PreToolUse":[{"matcher":"(?<=x)y","hooks":[{"type":"command","command":"true","timeout":1}]}]}}"#,
            r#"{"hooks":{"PreToolUse":[{"matcher":"(x)\\1","hooks":[{"type":"command","command":"true","timeout":1}]}]}}"#,
        ] {
            assert!(PortableConfig::parse(input.as_bytes()).is_err(), "{input}");
        }
    }
    #[test]
    fn explicit_trust_is_exact_and_changes_invalidate_it() {
        let mut r = HookRegistration {
            plugin_id: "test".into(),
            version: "1.0.0".into(),
            config: "hooks.json".into(),
            digest: "old".into(),
            trusted_digest: None,
            enabled: false,
        };
        assert_eq!(r.state("old"), HookState::NeedsReview);
        assert!(r.confirm_review("new").is_err());
        r.confirm_review("old").unwrap();
        assert_eq!(r.state("old"), HookState::Active);
        assert_eq!(r.state("new"), HookState::NeedsReview);
        r.enabled = false;
        assert_eq!(r.state("old"), HookState::Disabled);
    }
    #[test]
    fn rejects_escape_paths_and_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        assert!(bundle_path(temp.path(), "../outside").is_err());
        assert!(bundle_path(temp.path(), "/etc/passwd").is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("/etc/passwd", temp.path().join("escape")).unwrap();
            assert!(bundle_path(temp.path(), "escape").is_err());
        }
    }
}
