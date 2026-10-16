//! Closed target owner may dispose transports without retaining an initiator.
//! No command is executed, no cursor advanced and no control receipt invented.
use super::*;
use std::{future::Future, pin::Pin};
type TransportSelector = Arc<
    dyn Fn(
            bamboo_domain::ScopedLoopControlBinding,
        )
            -> Pin<Box<dyn Future<Output = std::io::Result<Option<OwnedFilesystem>>> + Send>>
        + Send
        + Sync,
>;

fn invalid(reason: &str) -> SessionInboxError {
    SessionInboxError::InvalidClaim(reason.into())
}

impl FileSessionInbox {
    /// A race-tolerant read hint only, never an expiry/authority decision. No
    /// repair/quarantine or new sidecar/census requirement for ordinary inboxes.
    async fn has_scoped_transport_hint(
        &self,
        dir: &Path,
        target: &str,
    ) -> Result<bool, SessionInboxError> {
        let mut inspected = 0usize;
        for queue in ["new", "cur"] {
            let mut reader = match tokio::fs::read_dir(dir.join(queue)).await {
                Ok(reader) => reader,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
            };
            while let Some(entry) = reader
                .next_entry()
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?
            {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.ends_with(".json") || Self::claim_generation(&name).is_err() {
                    continue;
                }
                let file_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
                };
                if !file_type.is_file() {
                    continue;
                }
                inspected += 1;
                if inspected > self.limits.max_backlog {
                    return Err(invalid("transport hint exceeds bounded Inbox"));
                }
                let bytes = match Self::read_receipt_bytes(
                    &entry.path(),
                    ReceiptLookupMode::Observe {
                        max_bytes: self.max_transport_bytes(),
                    },
                )
                .await
                {
                    Ok(bytes) => bytes,
                    Err(error)
                        if matches!(error.kind(), ErrorKind::InvalidData | ErrorKind::NotFound) =>
                    {
                        continue
                    }
                    Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
                };
                let Ok(raw) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                    continue;
                };
                let Some(body) = raw.get("body") else {
                    continue;
                };
                if body.get("kind").and_then(serde_json::Value::as_str)
                    != Some("runtime_instruction")
                    || body
                        .pointer("/body/type")
                        .and_then(serde_json::Value::as_str)
                        != Some("runtime_instruction")
                    || body
                        .pointer("/source/type")
                        .and_then(serde_json::Value::as_str)
                        != Some("runtime")
                    || body
                        .pointer("/source/subsystem")
                        .and_then(serde_json::Value::as_str)
                        != Some(bamboo_domain::SCOPED_LOOP_SUBSYSTEM)
                    || body
                        .pointer("/body/instruction")
                        .and_then(serde_json::Value::as_str)
                        != Some(bamboo_domain::SCOPED_LOOP_INSTRUCTION)
                {
                    continue;
                }
                let Some(binding) = body.pointer("/body/data/binding") else {
                    continue;
                };
                let Ok(binding) = serde_json::from_value::<bamboo_domain::ScopedLoopControlBinding>(
                    binding.clone(),
                ) else {
                    continue;
                };
                if binding.target_session_id == target
                    && body
                        .get("target_session_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(target)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Delivery uses this before backlog accounting. It does not wait for a
    /// successor owner that could itself be blocked by the full old queue.
    pub(crate) async fn delivery_filesystem_with_transport_expiry(
        &self,
        target: &str,
        authority: InboxAuthority,
    ) -> Result<(PathBuf, OwnedFilesystem), SessionInboxError> {
        let dir = self.inbox_dir(target).await?;
        if !self.has_scoped_transport_hint(&dir, target).await? {
            return self.filesystem_with_authority(target, authority).await;
        }
        let authority = match authority {
            InboxAuthority::Lifecycle { _guard } => InboxAuthority::Root {
                _guard: self
                    .sessions
                    .scoped_transport_delivery_guards(_guard, target)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?,
            },
            existing => existing,
        };
        // Existing Supervisor/Peer/Actor/Root authorities already retain their
        // complete canonical lock set. Never re-enter public lock acquisition.
        let census = self
            .sessions
            .scoped_transport_census_locked(target)
            .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
        let (dir, filesystem) = self.filesystem_with_authority(target, authority).await?;
        let Some(census) = census else {
            return Ok((dir, filesystem));
        };
        let observer = self
            .scoped_runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|(observer, _)| observer.upgrade());
        let retained = filesystem.clone();
        let committed = census.session.clone();
        let canonical = Arc::new(census);
        let select: TransportSelector = Arc::new(move |binding| {
            let filesystem = retained.clone();
            let census = canonical.clone();
            let observer = observer.clone();
            Box::pin(async move {
                if binding.target_session_id != census.session.id {
                    return Ok(None);
                }
                if census.native_inert(&binding)? {
                    return Ok(Some(filesystem.with_check(Arc::new(move || {
                        let current = census.require_same_snapshot()?;
                        if current.native_inert(&binding)? {
                            Ok(())
                        } else {
                            Err(std::io::Error::other(
                                "native transport is no longer independently inert",
                            ))
                        }
                    }))));
                }
                if census.session.created_at != binding.target_created_at
                    || census
                        .session
                        .agent_runtime_state
                        .as_ref()
                        .and_then(|state| state.execution_id.as_deref())
                        != Some(binding.execution_id.as_str())
                {
                    return Ok(None);
                }
                let Some(observer) = observer else {
                    return Ok(None);
                };
                let candidate = binding.clone();
                let observed = observer.clone();
                // Select without retaining a publication guard across await.
                // A failed retain is only "skip", never proof of death.
                let closed = tokio::task::spawn_blocking(move || {
                    observed
                        .retain_closed_scoped_transport_blocking(&candidate)
                        .is_ok()
                })
                .await
                .map_err(std::io::Error::other)?;
                if !closed {
                    return Ok(None);
                }
                let proof = binding.clone();
                let filesystem = filesystem.with_runtime_retainer(
                    Arc::new(move || {
                        observer
                            .retain_closed_scoped_transport_blocking(&proof)
                            .map_err(std::io::Error::other)
                    }),
                    binding,
                );
                Ok(Some(filesystem.with_check(Arc::new(move || {
                    census.require_same_snapshot().map(|_| ())
                }))))
            })
        });
        self.retire_selected_scoped_with_filesystem(&dir, &committed, select)
            .await?;
        Ok((dir, filesystem))
    }
}

/// Parse transport identity independently from today's command acceptance
/// policy. Old admitted-but-unused objectives above a newly tightened limit
/// still have to be disposable; this never makes them executable again.
fn transport_input(
    envelope: &SessionMessageEnvelope,
) -> Result<bamboo_domain::ScopedLoopControlInput, SessionInboxError> {
    let bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) = &envelope.body else {
        return Err(invalid("closed Loop transport body changed"));
    };
    if envelope.kind != bamboo_domain::SessionMessageKind::RuntimeInstruction
        || envelope.source
            != (SessionMessageSource::Runtime {
                subsystem: bamboo_domain::SCOPED_LOOP_SUBSYSTEM.into(),
            })
        || instruction.instruction != bamboo_domain::SCOPED_LOOP_INSTRUCTION
        || instruction.content.is_some()
        || instruction.provider_message.is_some()
        || envelope.thread_id.is_some()
        || envelope.in_reply_to.is_some()
    {
        return Err(invalid("closed Loop transport fixed identity changed"));
    }
    let input: bamboo_domain::ScopedLoopControlInput = serde_json::from_value(
        instruction
            .data
            .clone()
            .ok_or_else(|| invalid("closed Loop input missing"))?,
    )
    .map_err(|_| invalid("closed Loop input malformed"))?;
    input
        .binding
        .validate()
        .map_err(SessionInboxError::InvalidClaim)?;
    // Version is the fixed supervisor_loop_control_v1 instruction; this DTO
    // has no independently trusted version field. Action fields are strict
    // serde variants. Keep operation/generation identity syntax independent
    // from the current objective acceptance policy.
    uuid::Uuid::parse_str(input.operation_id())
        .map_err(|_| invalid("closed Loop operation UUID invalid"))?;
    match &input.command {
        bamboo_domain::LoopControlRequest::Start {
            expected_generation: Some(generation),
            ..
        }
        | bamboo_domain::LoopControlRequest::Update { generation, .. }
        | bamboo_domain::LoopControlRequest::Stop { generation, .. } => {
            uuid::Uuid::parse_str(generation)
                .map_err(|_| invalid("closed Loop generation UUID invalid"))?;
        }
        _ => {}
    }

    let id = bamboo_domain::SessionMessageId::stable(
        "supervisor_loop_control",
        &serde_json::json!({
            "supervisor": input.binding.supervisor, "target": input.binding.target_session_id,
            "birth": input.binding.target_created_at, "grant": input.binding.grant_id,
            "owner": input.binding.owner_run_id, "operation_id": input.operation_id(),
        }),
    );
    if input.operation_id().is_empty()
        || envelope.id != id
        || envelope.target_session_id != input.binding.target_session_id
        || envelope.correlation_id.as_deref() != Some(input.operation_id())
    {
        return Err(invalid("closed Loop input identity changed"));
    }
    Ok(input)
}

impl FileSessionInbox {
    pub(crate) async fn retire_closed_loop_with_filesystem(
        &self,
        dir: &Path,
        identity: &bamboo_domain::ScopedLoopControlBinding,
        committed: &bamboo_domain::Session,
        filesystem: &OwnedFilesystem,
    ) -> Result<(), SessionInboxError> {
        let identity = identity.clone();
        let retained = filesystem.clone();
        self.retire_selected_scoped_with_filesystem(
            dir,
            committed,
            Arc::new(move |candidate| {
                let selected = (candidate.target_session_id == identity.target_session_id
                    && candidate.target_created_at == identity.target_created_at
                    && candidate.owner_run_id == identity.owner_run_id
                    && candidate.execution_id == identity.execution_id)
                    .then(|| retained.clone());
                Box::pin(async move { Ok(selected) })
            }),
        )
        .await
    }

    async fn retire_selected_scoped_with_filesystem(
        &self,
        dir: &Path,
        committed: &bamboo_domain::Session,
        select: TransportSelector,
    ) -> Result<(), SessionInboxError> {
        let mut entries = Vec::new();
        for queue in ["new", "cur"] {
            let mut reader = match tokio::fs::read_dir(dir.join(queue)).await {
                Ok(reader) => reader,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
            };
            while let Some(entry) = reader
                .next_entry()
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?
            {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Ok(generation) = Self::claim_generation(&name) else {
                    continue;
                };
                if !name.ends_with(".json")
                    || name.starts_with('.')
                    || !entry
                        .file_type()
                        .await
                        .map_err(|error| SessionInboxError::Storage(error.to_string()))?
                        .is_file()
                {
                    continue;
                }
                entries.push((generation, queue, name, entry.path()));
                if entries.len() > self.limits.max_backlog {
                    return Err(invalid("closed Loop backlog exceeds configured bound"));
                }
            }
        }
        if entries.len() > self.limits.max_backlog {
            return Err(invalid("closed Loop backlog exceeds configured bound"));
        }
        entries.sort_by_key(|entry| entry.0);
        let records = bamboo_domain::scoped_loop_consumptions(committed)
            .map_err(SessionInboxError::InvalidClaim)?;
        for (generation, queue, name, path) in entries {
            let bytes = match Self::read_receipt_bytes(
                &path,
                ReceiptLookupMode::Observe {
                    max_bytes: self.max_transport_bytes(),
                },
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::InvalidData => continue,
                Err(error) => return Err(SessionInboxError::Storage(error.to_string())),
            };
            // Selection is read-only and deliberately ignores unrelated or
            // unidentified corrupt data. Strict decoding begins only after
            // fixed carrier + exact target lifetime/run/execution selection.
            let Ok(raw) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let Some(body) = raw.get("body") else {
                continue;
            };
            if body.get("kind").and_then(serde_json::Value::as_str) != Some("runtime_instruction")
                || body
                    .pointer("/body/type")
                    .and_then(serde_json::Value::as_str)
                    != Some("runtime_instruction")
                || body
                    .pointer("/source/type")
                    .and_then(serde_json::Value::as_str)
                    != Some("runtime")
                || body
                    .pointer("/source/subsystem")
                    .and_then(serde_json::Value::as_str)
                    != Some(bamboo_domain::SCOPED_LOOP_SUBSYSTEM)
                || body
                    .pointer("/body/instruction")
                    .and_then(serde_json::Value::as_str)
                    != Some(bamboo_domain::SCOPED_LOOP_INSTRUCTION)
            {
                continue;
            }
            let Some(binding) = body.pointer("/body/data/binding") else {
                continue;
            };
            let Ok(candidate) =
                serde_json::from_value::<bamboo_domain::ScopedLoopControlBinding>(binding.clone())
            else {
                continue;
            };
            let Some(filesystem) = select(candidate.clone())
                .await
                .map_err(|error| SessionInboxError::Storage(error.to_string()))?
            else {
                continue;
            };
            let (wrapper, lease) = Self::decode_owned_wrapper(&bytes)?;
            if wrapper.kind != InboxKind::SessionEnvelope {
                return Err(invalid("closed Loop wrapper kind changed"));
            }
            match &lease {
                Some(lease)
                    if !Self::owned_enabled(dir).await?
                        || !lease.matches_transport_name(generation, &name) =>
                {
                    return Err(invalid("closed Loop lease identity changed"));
                }
                None if name != format!("{generation:020}-{}.json", wrapper.id.0) => {
                    return Err(invalid("closed Loop filename identity changed"));
                }
                _ => {}
            }
            if !Self::read_activation_watermark(dir).await?.1 {
                return Err(invalid("closed Loop lacks versioned intent"));
            }
            let intent = Self::activation_intent(&wrapper.body)?;
            if intent
                != Some(SessionInboxActivationIntent::for_expected_run(
                    &candidate.owner_run_id,
                )?)
            {
                return Err(invalid("closed Loop expected-run intent changed"));
            }
            let envelope: SessionMessageEnvelope = serde_json::from_value(wrapper.body)
                .map_err(|_| invalid("closed Loop envelope malformed"))?;
            envelope
                .validate()
                .map_err(|_| invalid("closed Loop envelope invalid"))?;
            let input = transport_input(&envelope)?;
            if input.binding != candidate {
                return Err(invalid("closed Loop binding changed"));
            }
            let digest = format!(
                "{:x}",
                Sha256::digest(bamboo_domain::canonical_json_bytes(
                    &envelope.idempotency_semantics(),
                ))
            );
            let record = records.iter().find(|record| record.input_id == envelope.id);
            if record.is_some_and(|record| {
                record.input_sha256 != digest
                    || record.binding != input.binding
                    || record.operation_id != input.operation_id()
                    || (record.configured != record.receipt.is_some())
                    || (record.configured == record.rejection.is_some())
            }) {
                return Err(invalid("closed Loop immutable consumption conflicts"));
            }
            let cursor_exact = committed.session_inbox_admission().is_some_and(|cursor| {
                cursor.contains(&envelope.id) && cursor.last_admitted_sequence >= generation
            });
            let admitted = Self::admitted_receipt_in_mode(
                dir,
                &envelope,
                ReceiptLookupMode::Observe {
                    max_bytes: self.max_transport_bytes(),
                },
            )
            .await?;
            if let Some(admitted) = admitted {
                if admitted.delivery.generation != generation
                    || admitted.intent != intent
                    || !StoredLease::same_optional_identity(admitted.lease.as_ref(), lease.as_ref())
                {
                    return Err(invalid("closed Loop permanent receipt conflicts"));
                }
                // The permanent exact ACK is already true: only remove the
                // redundant transport; do not rewrite its immutable evidence.
                filesystem
                    .remove(&path)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            } else if record.is_some() != cursor_exact {
                // A half-checkpoint is positive partial evidence, not proof
                // of disposal safety. Preserve it for strict reconciliation.
                return Err(invalid(
                    "closed Loop partial checkpoint requires reconciliation",
                ));
            } else if record.is_some() && cursor_exact {
                if queue != "cur" {
                    return Err(invalid("committed Loop is not a claimed transport"));
                }
                let claim = SessionInboxClaim {
                    claim_id: name,
                    generation,
                    // The exact v3 expected-run intent checked above is
                    // QueueOnly and always preserves the specific wait.
                    activation_policy: SessionActivationPolicy::RespectSpecificWait,
                    envelope,
                };
                // No lease is minted/renewed and no cursor is changed. The
                // canonical exact consumption + cursor proves the actual ACK.
                let authority = match lease.as_ref() {
                    Some(lease) => AckAuthority::Owned {
                        lease,
                        filesystem: &filesystem,
                    },
                    None => AckAuthority::ScopedLegacy(&filesystem),
                };
                self.ack_unlocked(dir, &candidate.target_session_id, &claim, authority)
                    .await?;
            } else {
                // Missing/evicted consumption is UNKNOWN application history,
                // not evidence that the command never executed. Retire only
                // its now-inert transport, without creating an Applied record.
                filesystem
                    .cancel_if_absent(dir, &path, &name)
                    .await
                    .map_err(|error| SessionInboxError::Storage(error.to_string()))?;
            }
        }
        Ok(())
    }
}
