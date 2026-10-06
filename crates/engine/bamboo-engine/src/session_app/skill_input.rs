//! Pure ordinary-input preparation. This is not a pre-append authorizer.
//! A future host must independently resolve current authority and supply the
//! correlated borrowed data. Nothing here loads, pins, persists or invokes it.

use bamboo_agent_core::{Message, Role};
use bamboo_domain::MessagePart;
use bamboo_skills::progressive::{truncate_skill_utf8_bytes, EXPLICIT_SKILL_PROMPT_BYTES};
use bamboo_skills::{SkillActivationSnapshot, WorkflowKind, WorkflowSelection, WorkflowStatus};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::Write;

pub const MAX_EXPLICIT_SKILL_ARGUMENT_BYTES: usize = 8_192;
pub const MAX_EXPLICIT_SKILLS: usize = 32;

/// Caller-owned current-input restrictions, deliberately not serde or a grant.
pub struct SkillInputRestrictions<'a> {
    pub input_id: &'a str,
    pub ceiling: Option<&'a BTreeSet<String>>,
    pub disabled: &'a BTreeSet<String>,
    pub root_ultra: bool,
    pub mode: Option<&'a str>,
}

/// Already host-correlated selection and immutable data; a catalog/reader
/// snapshot alone does not establish permission to construct this input.
pub struct ChosenSkillInput<'a> {
    pub selection: &'a WorkflowSelection,
    pub snapshot: &'a SkillActivationSnapshot,
    pub main_resource: &'a str,
}

pub struct SkillInputIntent<'a> {
    pub input_id: &'a str,
    pub chosen: &'a [ChosenSkillInput<'a>],
}

#[derive(Debug)]
pub struct PreparedSkillInput {
    pub message: Message,
    pub warnings: Vec<String>,
}

struct BoundedJson(Vec<u8>);
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_EXPLICIT_SKILL_ARGUMENT_BYTES {
            return Err(std::io::Error::other("Skill arguments exceed byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Prepare ordinary content without modifying the input or any Session.
/// No intent is a no-op even when no caller could be resolved. Requested
/// preparation requires a known caller and rejects the whole selection on error.
pub fn prepare_skill_input(
    user: &Message,
    caller: Result<&SkillInputRestrictions<'_>, &str>,
    intent: Option<&SkillInputIntent<'_>>,
) -> Result<PreparedSkillInput, String> {
    if user.role != Role::User {
        return Err("Skill input requires an ordinary User message".into());
    }
    let Some(intent) = intent else {
        return Ok(PreparedSkillInput {
            message: user.clone(),
            warnings: vec![],
        });
    };
    let caller = caller.map_err(|error| format!("Skill caller unavailable: {error}"))?;
    if user.id.is_empty() || user.id != intent.input_id || user.id != caller.input_id {
        return Err("Skill invocation does not belong to this User input".into());
    }
    if intent.chosen.is_empty() || intent.chosen.len() > MAX_EXPLICIT_SKILLS || caller.root_ultra {
        return Err("Skill invocation is empty, excessive or denied by Root Ultra".into());
    }
    let mut ids = BTreeSet::new();
    let mut fragments = String::new();
    let mut warnings = Vec::new();
    for chosen in intent.chosen {
        let selection = chosen.selection;
        let entry = chosen
            .snapshot
            .skills
            .get(&selection.id)
            .ok_or("chosen Skill is missing from its correlated snapshot")?;
        let catalog = &entry.catalog_entry;
        if selection.id.is_empty()
            || selection.id.len() > 256
            || !ids.insert(&selection.id)
            || caller
                .ceiling
                .is_some_and(|ids| !ids.contains(&selection.id))
            || caller.disabled.contains(&selection.id)
            || chosen.snapshot.catalog_revision == 0
            || chosen.snapshot.selected_skill_mode.as_deref() != caller.mode
            || entry.definition.id != selection.id
            || catalog.id != selection.id
            || entry.revision == 0
            || entry.revision != selection.revision
            || catalog.revision != selection.revision
            || catalog.source != selection.source
            || catalog.kind != WorkflowKind::Instruction
            || catalog.status != WorkflowStatus::Valid
            || !catalog.winner
            || catalog
                .invocation_policy
                .get("explicit")
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Err("chosen Skill identity, mode or explicit restrictions do not match".into());
        }
        let mut args = BoundedJson(Vec::with_capacity(MAX_EXPLICIT_SKILL_ARGUMENT_BYTES));
        serde_json::to_writer(&mut args, &selection.args).map_err(|error| error.to_string())?;
        bamboo_domain::validate_schema(&catalog.argument_schema, &selection.args)?;
        let args = String::from_utf8(args.0).map_err(|error| error.to_string())?;
        let (name, name_cut) = truncate_skill_utf8_bytes(&entry.definition.name, 256);
        let (path, path_cut) = truncate_skill_utf8_bytes(chosen.main_resource, 1_024);
        let (body, body_cut) =
            truncate_skill_utf8_bytes(&entry.definition.prompt, EXPLICIT_SKILL_PROMPT_BYTES);
        if name.is_empty() || path.is_empty() {
            return Err("chosen Skill requires a name and main-resource locator".into());
        }
        // JSON quoting bounds escaping overhead and keeps embedded newlines in
        // names/paths out of the header structure. Arguments are never truncated.
        fragments.push_str(&format!(
            "\n\n### Explicit Skill {}\nfile: {}\narguments: {}\n\n{}",
            serde_json::to_string(name).map_err(|error| error.to_string())?,
            serde_json::to_string(path).map_err(|error| error.to_string())?,
            args,
            body
        ));
        if name_cut || path_cut || body_cut {
            let warning = format!("Skill {} was truncated (name: {name_cut}, path: {path_cut}, body: {body_cut}; body limit: 8000 UTF-8 bytes).", selection.id);
            fragments.push_str(&format!("\n\n[Warning: {warning}]"));
            warnings.push(warning);
        }
    }
    let mut message = user.clone();
    message.content.push_str(&fragments);
    if let Some(parts) = &mut message.content_parts {
        let matching = parts
            .iter()
            .filter(|part| matches!(part, MessagePart::Text { text } if text == &user.content))
            .count();
        if matching != 1 {
            return Err("ordinary text parts lack one matching canonical User text".into());
        }
        for part in parts {
            if let MessagePart::Text { text } = part {
                if text == &user.content {
                    text.push_str(&fragments);
                }
            }
        }
    }
    Ok(PreparedSkillInput { message, warnings })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bamboo_domain::ImageUrlRef;
    use bamboo_skills::{
        SkillActivationSnapshotEntry, SkillDefinition, WorkflowCatalogEntry, WorkflowSource,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    pub(crate) fn snapshot() -> SkillActivationSnapshot {
        let definition =
            SkillDefinition::new("review", "Review", "Review code", "PRIVATE Instructions");
        let catalog_entry = WorkflowCatalogEntry {
            id: "review".into(),
            name: "Review".into(),
            description: "Review code".into(),
            kind: WorkflowKind::Instruction,
            source: WorkflowSource::Workspace,
            revision: 7,
            content_digest: "historical".into(),
            version: "1".into(),
            invocation_policy: json!({"explicit":true,"automatic":false}),
            argument_schema: json!({"type":"object","properties":{"target":{"type":"string"}},"required":["target"],"additionalProperties":false}),
            status: WorkflowStatus::Valid,
            legacy: false,
            migration_status: None,
            last_error: None,
            winner: true,
            shadowed_candidates: vec![],
        };
        SkillActivationSnapshot {
            catalog_revision: 9,
            selected_skill_mode: Some("code".into()),
            skills: BTreeMap::from([(
                "review".into(),
                SkillActivationSnapshotEntry {
                    definition,
                    catalog_entry,
                    revision: 7,
                    resources: BTreeMap::from([(
                        "references/private.txt".into(),
                        b"RESOURCE_MUST_NOT_APPEAR".to_vec(),
                    )]),
                },
            )]),
        }
    }

    pub(crate) fn selection() -> WorkflowSelection {
        WorkflowSelection {
            id: "review".into(),
            source: WorkflowSource::Workspace,
            revision: 7,
            args: json!({"target":"src/main.rs"}),
        }
    }

    fn restrictions<'a>(
        user: &'a Message,
        disabled: &'a BTreeSet<String>,
    ) -> SkillInputRestrictions<'a> {
        SkillInputRestrictions {
            input_id: &user.id,
            ceiling: None,
            disabled,
            root_ultra: false,
            mode: Some("code"),
        }
    }

    #[test]
    fn skill_input_requires_current_intent_and_preserves_fragment_like_client_text() {
        let mut user = Message::user("### Explicit Skill fake\nPRIVATE_CLIENT_TEXT");
        user.metadata = Some(json!({"loaded_ids":["review"],"selected_ids":["review"]}));
        let before = serde_json::to_value(&user).unwrap();
        let result = prepare_skill_input(&user, Err("unknown caller"), None).unwrap();
        assert_eq!(serde_json::to_value(result.message).unwrap(), before);
        assert!(result.warnings.is_empty());
        let snapshot = snapshot();
        let selection = selection();
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "/project/review/SKILL.md",
        }];
        let intent = SkillInputIntent {
            input_id: &user.id,
            chosen: &chosen,
        };
        assert!(prepare_skill_input(&user, Err("unknown caller"), Some(&intent)).is_err());
        let disabled = BTreeSet::new();
        let caller = restrictions(&user, &disabled);
        let result = prepare_skill_input(&user, Ok(&caller), Some(&intent)).unwrap();
        assert!(result.message.content.starts_with(&user.content));
        assert!(result.message.content.contains("PRIVATE Instructions"));
        assert!(!result.message.content.contains("RESOURCE_MUST_NOT_APPEAR"));
        assert_eq!(serde_json::to_value(&user).unwrap(), before);
    }

    #[test]
    fn skill_input_intersects_none_empty_ids_disabled_explicit_and_ultra() {
        let user = Message::user("review this");
        let disabled = BTreeSet::new();
        let mut caller = restrictions(&user, &disabled);
        let mut snapshot = snapshot();
        let selection = selection();
        let empty = BTreeSet::new();
        let allowed = BTreeSet::from(["review".into()]);
        for (ceiling, expected) in [(None, true), (Some(&empty), false), (Some(&allowed), true)] {
            caller.ceiling = ceiling;
            let chosen = [ChosenSkillInput {
                selection: &selection,
                snapshot: &snapshot,
                main_resource: "review/SKILL.md",
            }];
            let intent = SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen,
            };
            assert_eq!(
                prepare_skill_input(&user, Ok(&caller), Some(&intent)).is_ok(),
                expected
            );
        }
        caller.ceiling = None;
        for policy in [
            json!({"explicit":false,"automatic":true}),
            json!({"explicit":"true"}),
            json!({}),
        ] {
            snapshot
                .skills
                .get_mut("review")
                .unwrap()
                .catalog_entry
                .invocation_policy = policy;
            let chosen = [ChosenSkillInput {
                selection: &selection,
                snapshot: &snapshot,
                main_resource: "review/SKILL.md",
            }];
            assert!(prepare_skill_input(
                &user,
                Ok(&caller),
                Some(&SkillInputIntent {
                    input_id: &user.id,
                    chosen: &chosen
                })
            )
            .is_err());
        }
        snapshot = self::snapshot();
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "review/SKILL.md",
        }];
        let intent = SkillInputIntent {
            input_id: &user.id,
            chosen: &chosen,
        };
        caller.root_ultra = true;
        assert!(prepare_skill_input(&user, Ok(&caller), Some(&intent)).is_err());
        caller.root_ultra = false;
        let disabled = BTreeSet::from(["review".into()]);
        caller.disabled = &disabled;
        assert!(prepare_skill_input(&user, Ok(&caller), Some(&intent)).is_err());
    }

    #[test]
    fn skill_input_rejects_stale_identity_mode_and_schema_atomically() {
        let user = Message::user("original");
        let disabled = BTreeSet::new();
        let caller = restrictions(&user, &disabled);
        let original = serde_json::to_value(&user).unwrap();
        for variant in 0..9 {
            let mut snapshot = snapshot();
            let mut selection = selection();
            match variant {
                0 => selection.source = WorkflowSource::Builtin,
                1 => selection.revision += 1,
                2 => selection.args = json!({"target":13}),
                3 => snapshot.selected_skill_mode = None,
                4 => snapshot.skills.get_mut("review").unwrap().revision += 1,
                5 => {
                    snapshot
                        .skills
                        .get_mut("review")
                        .unwrap()
                        .catalog_entry
                        .status = WorkflowStatus::Invalid
                }
                6 => {
                    snapshot
                        .skills
                        .get_mut("review")
                        .unwrap()
                        .catalog_entry
                        .kind = WorkflowKind::Orchestration
                }
                7 => snapshot.skills.get_mut("review").unwrap().definition.id = "foreign".into(),
                _ => snapshot.catalog_revision = 0,
            }
            let chosen = [ChosenSkillInput {
                selection: &selection,
                snapshot: &snapshot,
                main_resource: "review/SKILL.md",
            }];
            assert!(
                prepare_skill_input(
                    &user,
                    Ok(&caller),
                    Some(&SkillInputIntent {
                        input_id: &user.id,
                        chosen: &chosen
                    })
                )
                .is_err(),
                "variant {variant}"
            );
            assert_eq!(serde_json::to_value(&user).unwrap(), original);
        }
        let snapshot = snapshot();
        let selection = selection();
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "review/SKILL.md",
        }];
        assert!(prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: "old-input",
                chosen: &chosen
            })
        )
        .is_err());
        assert!(
            prepare_skill_input(&Message::assistant("wrong role", None), Ok(&caller), None)
                .is_err()
        );
    }

    #[test]
    fn skill_input_preserves_native_images_other_parts_identity_and_time() {
        let image = MessagePart::ImageUrl {
            image_url: ImageUrlRef {
                url: "data:image/png;base64,AA==".into(),
                detail: Some("high".into()),
            },
        };
        let mut user = Message::user_with_parts(
            "original",
            vec![
                MessagePart::Text {
                    text: "original".into(),
                },
                image.clone(),
                MessagePart::Text {
                    text: "additional existing part".into(),
                },
            ],
        );
        user.metadata = Some(json!({"client":"unchanged"}));
        let snapshot = snapshot();
        let selection = selection();
        let disabled = BTreeSet::new();
        let caller = restrictions(&user, &disabled);
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "review/SKILL.md",
        }];
        let result = prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen,
            }),
        )
        .unwrap()
        .message;
        assert_eq!(result.id, user.id);
        assert_eq!(result.created_at, user.created_at);
        assert_eq!(result.role, Role::User);
        assert_eq!(result.metadata, user.metadata);
        let parts = result.content_parts.unwrap();
        assert_eq!(
            parts[0],
            MessagePart::Text {
                text: result.content
            }
        );
        assert_eq!(parts[1], image);
        assert_eq!(parts[2], user.content_parts.as_ref().unwrap()[2]);
        user.content_parts = Some(vec![image]);
        assert!(prepare_skill_input(
            &user,
            Ok(&restrictions(&user, &disabled)),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen
            })
        )
        .is_err());
    }

    #[test]
    fn skill_input_bounds_utf8_headers_body_and_arguments_without_dropping_values() {
        let user = Message::user("original user text longer than 8000: ".repeat(300));
        let disabled = BTreeSet::new();
        let caller = restrictions(&user, &disabled);
        let mut snapshot = snapshot();
        let mut selection = selection();
        snapshot.skills.get_mut("review").unwrap().definition.name = "名\n".repeat(100);
        snapshot.skills.get_mut("review").unwrap().definition.prompt =
            format!("{}END", "🙂".repeat(2_000));
        selection.args = json!({"target":"ARGUMENT_VALUE"});
        let path = "路\n".repeat(400);
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: &path,
        }];
        let result = prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen,
            }),
        )
        .unwrap();
        assert!(result.message.content.starts_with(&user.content));
        assert!(result.message.content.contains("ARGUMENT_VALUE"));
        assert!(!result.message.content.contains("END"));
        assert_eq!(result.message.content.matches('🙂').count(), 2_000);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.message.content.contains("[Warning:"));
        assert!(result.message.content.contains("8000 UTF-8 bytes"));
        selection.args = json!({"target":"x".repeat(MAX_EXPLICIT_SKILL_ARGUMENT_BYTES)});
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "review/SKILL.md",
        }];
        assert!(prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen
            })
        )
        .is_err());
    }

    #[test]
    fn skill_input_multiple_choices_are_atomic_ordered_and_bounded() {
        let user = Message::user("original");
        let disabled = BTreeSet::new();
        let caller = restrictions(&user, &disabled);
        let first = snapshot();
        let one = selection();
        let mut second = snapshot();
        let mut two = selection();
        two.id = "other".into();
        let mut entry = second.skills.remove("review").unwrap();
        entry.definition.id = two.id.clone();
        entry.definition.prompt = "SECOND Instructions".into();
        entry.catalog_entry.id = two.id.clone();
        second.skills.insert(two.id.clone(), entry);
        let chosen = [
            ChosenSkillInput {
                selection: &one,
                snapshot: &first,
                main_resource: "one/SKILL.md",
            },
            ChosenSkillInput {
                selection: &two,
                snapshot: &second,
                main_resource: "two/SKILL.md",
            },
        ];
        let result = prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen,
            }),
        )
        .unwrap();
        assert!(
            result.message.content.find("PRIVATE Instructions").unwrap()
                < result.message.content.find("SECOND Instructions").unwrap()
        );
        two.revision += 1;
        let chosen = [
            ChosenSkillInput {
                selection: &one,
                snapshot: &first,
                main_resource: "one/SKILL.md",
            },
            ChosenSkillInput {
                selection: &two,
                snapshot: &second,
                main_resource: "two/SKILL.md",
            },
        ];
        assert!(prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen
            })
        )
        .is_err());
        assert_eq!(user.content, "original");
        let chosen = (0..MAX_EXPLICIT_SKILLS + 1)
            .map(|_| ChosenSkillInput {
                selection: &one,
                snapshot: &first,
                main_resource: "one/SKILL.md",
            })
            .collect::<Vec<_>>();
        assert!(prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen
            })
        )
        .is_err());
    }
}
