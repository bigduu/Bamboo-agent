//! One transaction's exact direct-parent answer delta, never an Actor writer.
use super::*;
use bamboo_domain::{
    ParentQuestion, ParentQuestionOutcome, ParentQuestionResolution, PARENT_QUESTION_RESOLUTION_KEY,
};

fn conflict(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        bamboo_domain::SessionAuthorityConflict(format!(
            "Parent question answer authority rejected: {reason}"
        )),
    )
}

#[derive(Clone)]
struct CanonicalPair {
    main: Vec<u8>,
    runtime: Vec<u8>,
}
impl CanonicalPair {
    fn read(directory: &Path) -> io::Result<Self> {
        Ok(Self {
            main: actor_transcript::regular_bytes(&directory.join("session.json"))
                .map_err(conflict)?,
            runtime: actor_transcript::regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))
                .map_err(conflict)?,
        })
    }

    fn session(&self) -> io::Result<Session> {
        compact_main::validate_full_main(&self.main).map_err(conflict)?;
        let mut main: Session = actor_transcript::decode(&self.main).map_err(conflict)?;
        let mut runtime: Session = actor_transcript::decode(&self.runtime).map_err(conflict)?;
        supervisor::validate_overlay(&main, Some(&runtime)).map_err(conflict)?;
        for session in [&mut main, &mut runtime] {
            if session.kind == SessionKind::Root && session.root_session_id.is_empty() {
                session.root_session_id = session.id.clone();
            }
        }
        if main.id != runtime.id
            || main.created_at != runtime.created_at
            || main.kind != runtime.kind
            || main.root_session_id != runtime.root_session_id
            || main.parent_session_id != runtime.parent_session_id
            || main.parent_created_at != runtime.parent_created_at
            || main.spawn_depth != runtime.spawn_depth
            || main.authority_identity != runtime.authority_identity
            || main.project_id_meta() != runtime.project_id_meta()
        {
            return Err(conflict("canonical Session pair identity changed"));
        }
        Ok(overlay_runtime_sidecar(main, Some(runtime)))
    }
}

#[derive(Clone)]
struct ActorWitness {
    directory: PathBuf,
    record: Option<Vec<u8>>,
    marker: Option<Vec<u8>>,
}
impl ActorWitness {
    fn read(directory: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
        let path = directory.join(name);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                std::fs::read(path).map(Some).map_err(conflict)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            _ => Err(conflict("Actor witness is unavailable or nonregular")),
        }
    }

    fn capture(directory: PathBuf, session: &Session) -> io::Result<Self> {
        let record = Self::read(&directory, "actor-authority.json")?;
        let marker = Self::read(&directory, "actor-authority.initialized.json")?;
        match (&record, &marker) {
            (None, None) => {} // Existing, unactivated legacy answer API.
            (Some(record), Some(marker)) => {
                let project = session
                    .project_id_meta()
                    .map(bamboo_domain::ProjectId::parse)
                    .transpose()
                    .map_err(conflict)?;
                actor_directory::validate_census_witnesses(
                    record,
                    marker,
                    session,
                    project.as_ref(),
                )
                .map_err(conflict)?;
            }
            _ => return Err(conflict("Actor authority is incomplete")),
        }
        Ok(Self {
            directory,
            record,
            marker,
        })
    }

    fn validate(&self) -> io::Result<()> {
        // The answer transaction retains parent and Child physical Session
        // locks. Actor renew/claim/checkpoint takes the same lock before tree,
        // so a lawful same-fence renewal cannot race these byte comparisons.
        if Self::read(&self.directory, "actor-authority.json")? != self.record
            || Self::read(&self.directory, "actor-authority.initialized.json")? != self.marker
        {
            return Err(conflict("Actor authority changed during the answer"));
        }
        Ok(())
    }
}

/// Minted solely by the typed answer CAS with both Session locks held. It
/// permits precisely one result replacement and resolution, never arbitrary
/// history, runtime, provider-state, identity or permission mutation.
#[derive(Clone)]
pub(super) struct ParentQuestionAnswerProof {
    parent: ActorWitness,
    child: ActorWitness,
    parent_pair: CanonicalPair,
    before: CanonicalPair,
    after: CanonicalPair,
    candidate: serde_json::Value,
    question: ParentQuestion,
    resolution: ParentQuestionResolution,
}
impl ParentQuestionAnswerProof {
    pub(super) fn capture(
        parent_directory: PathBuf,
        child_directory: PathBuf,
        original: &Session,
        candidate: &Session,
        question: &ParentQuestion,
    ) -> io::Result<Self> {
        let parent_pair = CanonicalPair::read(&parent_directory)?;
        let parent = parent_pair.session()?;
        let before = CanonicalPair::read(&child_directory)?;
        let durable = before.session()?;
        if serde_json::to_value(&durable).map_err(conflict)?
            != serde_json::to_value(original).map_err(conflict)?
            || parent.id != question.parent.session_id
            || parent.created_at != question.parent.created_at
            || durable.id != question.child.session_id
            || durable.created_at != question.child.created_at
            || !parent_question_checkpoint::parent_request_is_durable(&parent, question)
            || ParentQuestion::for_pending(&parent, &durable).as_ref() != Some(question)
        {
            return Err(conflict(
                "canonical parent request or pending result changed",
            ));
        }
        let resolution = ParentQuestionResolution::from_child(&parent, candidate, &question.id)
            .filter(|resolution| resolution.request == *question)
            .ok_or_else(|| conflict("candidate has no exact answer resolution"))?;
        let ParentQuestionOutcome::Answer { text } = &resolution.outcome else {
            return Err(conflict("answer permit cannot authorize expiration"));
        };
        let mut expected = durable.clone();
        let paired = expected
            .messages
            .iter_mut()
            .find(|message| message.id == question.tool_result_message_id)
            .ok_or_else(|| conflict("paired result missing"))?;
        paired.content = text.clone();
        paired.tool_success = Some(true);
        expected.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            candidate
                .metadata
                .get(PARENT_QUESTION_RESOLUTION_KEY)
                .ok_or_else(|| conflict("answer metadata missing"))?
                .clone(),
        );
        let candidate_value = serde_json::to_value(candidate).map_err(conflict)?;
        if serde_json::to_value(expected).map_err(conflict)? != candidate_value {
            return Err(conflict(
                "answer changed fields outside the exact result delta",
            ));
        }
        let proof = Self {
            parent: ActorWitness::capture(parent_directory, &parent)?,
            child: ActorWitness::capture(child_directory, &durable)?,
            parent_pair,
            before,
            after: CanonicalPair {
                main: compact_main::serialize_main(candidate)?,
                runtime: serde_json::to_vec_pretty(&runtime_sidecar_snapshot(candidate))
                    .map_err(conflict)?,
            },
            candidate: candidate_value,
            question: question.clone(),
            resolution,
        };
        proof.validate_publication()?;
        Ok(proof)
    }

    pub(super) fn validate_candidate(&self, candidate: &Session, full: bool) -> io::Result<()> {
        if !full || serde_json::to_value(candidate).map_err(conflict)? != self.candidate {
            return Err(conflict("answer permit does not authorize this save"));
        }
        self.validate_publication()
    }

    pub(super) fn validate_publication(&self) -> io::Result<()> {
        self.parent.validate()?;
        self.child.validate()?;
        let parent_pair = CanonicalPair::read(&self.parent.directory)?;
        if parent_pair.main != self.parent_pair.main
            || parent_pair.runtime != self.parent_pair.runtime
        {
            return Err(conflict("parent canonical request snapshot changed"));
        }
        let parent = parent_pair.session()?;
        if parent.id != self.question.parent.session_id
            || parent.created_at != self.question.parent.created_at
            || !parent_question_checkpoint::parent_request_is_durable(&parent, &self.question)
        {
            return Err(conflict("parent request is no longer canonical"));
        }
        let child = CanonicalPair::read(&self.child.directory)?;
        let main_after = child.main == self.after.main;
        let runtime_after = child.runtime == self.after.runtime;
        if (!main_after && child.main != self.before.main)
            || (!runtime_after && child.runtime != self.before.runtime)
            || (main_after && !runtime_after)
        {
            return Err(conflict("Child canonical pair changed outside this answer"));
        }
        let child = child.session()?;
        if main_after && runtime_after {
            if ParentQuestionResolution::from_child(&parent, &child, &self.question.id).as_ref()
                != Some(&self.resolution)
            {
                return Err(conflict(
                    "committed answer no longer matches its exact request",
                ));
            }
        } else if Utc::now() >= self.question.deadline
            || ParentQuestion::for_pending(&parent, &child).as_ref() != Some(&self.question)
        {
            return Err(conflict(
                "pending answer expired or changed before replacement",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        ActorActivationClaim, ActorDirectoryPort, FunctionCall, PendingQuestionSource,
        RootActorRuntimeWrite, ToolCall, PARENT_QUESTION_REQUEST_KEY,
    };

    struct Fixture {
        store: Arc<SessionStoreV2>,
        parent: Session,
        child: Session,
        question: ParentQuestion,
        owner: RootActorRuntimeWrite,
        _home: tempfile::TempDir,
    }
    impl Fixture {
        async fn new() -> Self {
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
            let mut parent = Session::new("answer-parent", "model");
            store.save_session(&parent).await.unwrap();
            let mut child = Session::new_child_of("answer-child", &parent, "model", "Child");
            child.messages.push(Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "question-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "AskUserQuestion".into(),
                        arguments: "{}".into(),
                    },
                }]),
            ));
            child.messages.push(Message::tool_result_with_status(
                "question-call",
                "Clarification needed: Which option?",
                true,
            ));
            child.set_pending_question_with_source(
                "question-call".into(),
                "AskUserQuestion".into(),
                "Which option?".into(),
                vec!["A".into(), "B".into()],
                true,
                PendingQuestionSource::DirectParent,
            );
            child.metadata.insert(
                "runtime.suspend_reason".into(),
                "awaiting_clarification".into(),
            );
            let question = ParentQuestion::issue_at(&parent, &child, Utc::now()).unwrap();
            child.metadata.insert(
                PARENT_QUESTION_REQUEST_KEY.into(),
                serde_json::to_string(&question).unwrap(),
            );
            parent.add_message(question.envelope().to_provider_message().unwrap());
            store.save_session(&parent).await.unwrap();
            store.save_session(&child).await.unwrap();
            let mut root_owner = None;
            for session in [&parent, &child] {
                let now = Utc::now();
                let activation = store
                    .claim_activation(&ActorActivationClaim {
                        actor_id: session.id.clone(),
                        run_id: format!("{}-run", session.id),
                        lease_owner: "answer-host".into(),
                        lease_expires_at: now + chrono::Duration::minutes(1),
                        inbox_generation: 0,
                        placement_ref: None,
                        now,
                    })
                    .await
                    .unwrap();
                store
                    .start_activation(&activation.fence(), now)
                    .await
                    .unwrap();
                if session.kind == SessionKind::Root {
                    root_owner = Some(RootActorRuntimeWrite {
                        fence: activation.fence(),
                        created_at: parent.created_at,
                    });
                }
            }
            Self {
                store,
                parent,
                child,
                question,
                owner: root_owner.unwrap(),
                _home: home,
            }
        }
        fn parent_directory(&self) -> PathBuf {
            self.store.sessions_dir.join(&self.parent.id)
        }
        fn child_directory(&self) -> PathBuf {
            self.parent_directory()
                .join("children")
                .join(&self.child.id)
        }
        fn candidate(&self) -> Session {
            let mut candidate = self.child.clone();
            candidate.messages.last_mut().unwrap().content = "A".into();
            let resolution =
                ParentQuestionResolution::answered(&self.question, Utc::now(), "A").unwrap();
            candidate.metadata.insert(
                PARENT_QUESTION_RESOLUTION_KEY.into(),
                serde_json::to_string(&resolution).unwrap(),
            );
            candidate
        }
        fn capture(&self, candidate: &Session) -> io::Result<ParentQuestionAnswerProof> {
            ParentQuestionAnswerProof::capture(
                self.parent_directory(),
                self.child_directory(),
                &self.child,
                candidate,
                &self.question,
            )
        }
    }

    #[tokio::test]
    async fn activated_child_answer_is_single_winner_without_widening_default_saves() {
        for root_fenced in [false, true] {
            let f = Fixture::new().await;
            let (saved, wrote) = f
                .store
                .answer_parent_question_with_root_writer(
                    &f.question,
                    "A",
                    root_fenced.then_some(&f.owner),
                    |_| {},
                )
                .await
                .unwrap()
                .unwrap();
            assert!(wrote);
            assert_eq!(saved.messages.last().unwrap().content, "A");
            assert!(
                ParentQuestionResolution::from_child(&f.parent, &saved, &f.question.id).is_some()
            );
            let mut unchanged = saved.clone();
            unchanged.messages = f.child.messages.clone();
            unchanged.metadata.remove(PARENT_QUESTION_RESOLUTION_KEY);
            assert_eq!(
                serde_json::to_value(unchanged).unwrap(),
                serde_json::to_value(&f.child).unwrap()
            );
            let (replayed, wrote) = f
                .store
                .answer_parent_question(&f.question, "B", |_| {})
                .await
                .unwrap()
                .unwrap();
            assert!(!wrote);
            assert_eq!(replayed.messages.last().unwrap().content, "A");
            let mut forged = saved;
            forged.messages[0].content = "outside the exact parent answer".into();
            assert!(f.store.save_session(&forged).await.is_err());
        }
    }

    #[tokio::test]
    async fn answer_proof_rejects_other_fields_and_runtime_only_saves() {
        let f = Fixture::new().await;
        let candidate = f.candidate();
        let proof = f.capture(&candidate).unwrap();
        assert!(proof.validate_candidate(&candidate, true).is_ok());
        assert!(proof.validate_candidate(&candidate, false).is_err());
        for variant in 0..4 {
            let mut changed = candidate.clone();
            match variant {
                0 => changed.messages[0].content = "rewritten history".into(),
                1 => {
                    changed
                        .metadata
                        .insert("disabled_tools".into(), "[]".into());
                }
                2 => changed.pending_question = None,
                _ => changed.workspace = Some("/changed".into()),
            }
            assert!(f.capture(&changed).is_err());
            assert!(proof.validate_candidate(&changed, true).is_err());
        }
    }

    #[tokio::test]
    async fn malformed_actor_authority_never_mints_answer_permit() {
        for name in ["actor-authority.json", "actor-authority.initialized.json"] {
            let f = Fixture::new().await;
            let main_before = std::fs::read(f.child_directory().join("session.json")).unwrap();
            let runtime_before =
                std::fs::read(f.child_directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
            std::fs::write(f.child_directory().join(name), b"{}").unwrap();
            assert!(f
                .store
                .answer_parent_question(&f.question, "A", |_| {})
                .await
                .is_err());
            assert_eq!(
                std::fs::read(f.child_directory().join("session.json")).unwrap(),
                main_before
            );
            assert_eq!(
                std::fs::read(f.child_directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
                runtime_before
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_replace_rechecks_parent_request_and_child_actor_witness() {
        use super::super::default_actor_context_tests::DefaultWriteHook;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Release(Arc<DefaultWriteHook>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        for tamper_parent in [false, true] {
            let f = Fixture::new().await;
            let before = std::fs::read(f.child_directory().join("session.json")).unwrap();
            let hook = DefaultWriteHook::install(
                &f.store,
                "session.json",
                DurableWritePhase::BeforeReplace,
                false,
            );
            let _release = Release(hook.clone());
            let calls = Arc::new(AtomicUsize::new(0));
            let published = calls.clone();
            let store = f.store.clone();
            let question = f.question.clone();
            let task = tokio::spawn(async move {
                store
                    .answer_parent_question(&question, "A", move |_| {
                        published.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            });
            hook.wait();
            if tamper_parent {
                let mut parent = f.parent.clone();
                parent.messages.clear();
                std::fs::write(
                    f.parent_directory().join("session.json"),
                    compact_main::serialize_main(&parent).unwrap(),
                )
                .unwrap();
            } else {
                std::fs::write(f.child_directory().join("actor-authority.json"), b"{}").unwrap();
            }
            hook.release();
            assert!(tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
                .is_err());
            assert_eq!(
                std::fs::read(f.child_directory().join("session.json")).unwrap(),
                before
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }
}
