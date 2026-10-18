//! Pure lifecycle gate for the proposed native Project-scope channel.
//!
//! A pipe, PID, handshake or launch identifier is not owner consent. Production
//! has no native admission source yet and must pass `None` to `begin`.
//! This module performs no I/O, starts no server, changes no AppState and grants
//! no permission. Only an independently admitted trusted Rust Host may supply a
//! verifier. A wire frame must never construct that verifier.

use std::collections::BTreeSet;

pub(super) const MAX_REQUESTS_PER_LAUNCH: usize = 64;
pub(super) const MAX_FRAME_BYTES: usize = 16 * 1024;
pub(super) const FRAME_PREFIX: &[u8] = b"BAMBOO_SUPERVISOR_HOST_V1 ";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameFault {
    Closed,
    TooLarge,
    InvalidPrefix,
    InvalidUtf8,
    Truncated,
}

/// Bounded byte framing only; the payload still requires strict typed JSON
/// decoding. Feed every byte, including the LF delimiter. A framing failure is
/// terminal and the adapter must disconnect its Session, never retry an apply.
#[derive(Default)]
pub(super) struct FrameBuffer {
    bytes: Vec<u8>,
    closed: bool,
}

impl FrameBuffer {
    fn fail(&mut self, error: FrameFault) -> FrameFault {
        self.bytes.clear();
        self.closed = true;
        error
    }

    pub(super) fn push(&mut self, byte: u8) -> Result<Option<Vec<u8>>, FrameFault> {
        if self.closed {
            return Err(FrameFault::Closed);
        }
        if byte == b'\n' {
            let line = std::mem::take(&mut self.bytes);
            let Some(payload) = line.strip_prefix(FRAME_PREFIX) else {
                return Err(self.fail(FrameFault::InvalidPrefix));
            };
            if std::str::from_utf8(payload).is_err() {
                return Err(self.fail(FrameFault::InvalidUtf8));
            }
            return Ok(Some(payload.to_vec()));
        }
        // Reserve one byte of the limit for the LF delimiter.
        if self.bytes.len() == MAX_FRAME_BYTES - 1 {
            return Err(self.fail(FrameFault::TooLarge));
        }
        self.bytes.push(byte);
        Ok(None)
    }

    pub(super) fn eof(&mut self) -> Result<(), FrameFault> {
        if self.closed {
            return Err(FrameFault::Closed);
        }
        if !self.bytes.is_empty() {
            return Err(self.fail(FrameFault::Truncated));
        }
        self.closed = true;
        Ok(())
    }
}

/// Verification is side-effect free and binds the entire immutable intent plus
/// this launch and request. Implementing this trait is not Host admission.
pub(super) trait ExactOwnerConsent<I> {
    fn verify_exact(&self, launch_id: u128, request_id: u128, intent: &I) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refusal {
    InvalidLaunch,
    HandshakeRequired,
    InvalidRequest,
    Replay,
    Capacity,
    Busy,
    OwnerChannelUnavailable,
    ConsentDenied,
    ReplyMismatch,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Disconnect {
    /// No operation was in flight. This says nothing about historical writes.
    Idle,
    /// An apply was issued; loss of transport does not prove it was cancelled.
    OutcomeUnconfirmed { request_id: u128 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Applying,
    ReplyPending,
}

#[derive(Debug)]
struct Pending<I> {
    request_id: u128,
    intent: I,
    phase: Phase,
}

/// Non-cloneable local permit. Its payload is the exact value verified by the
/// Host, not a later caller-supplied replacement.
#[derive(Debug)]
pub(super) struct Submission<I> {
    launch_id: u128,
    request_id: u128,
    intent: I,
}

impl<I> Submission<I> {
    pub(super) fn intent(&self) -> &I {
        &self.intent
    }
}

#[derive(Debug)]
pub(super) struct Reply<I>(Submission<I>);

pub(super) struct Session<I> {
    launch_id: u128,
    acknowledged: bool,
    seen: BTreeSet<u128>,
    pending: Option<Pending<I>>,
    closed: Option<Disconnect>,
}

impl<I: Clone + Eq> Session<I> {
    pub(super) fn new(launch_id: u128) -> Result<Self, Refusal> {
        if launch_id == 0 {
            return Err(Refusal::InvalidLaunch);
        }
        Ok(Self {
            launch_id,
            acknowledged: false,
            seen: BTreeSet::new(),
            pending: None,
            closed: None,
        })
    }

    fn check_launch(&self, launch_id: u128) -> Result<(), Refusal> {
        if self.closed.is_some() {
            return Err(Refusal::Closed);
        }
        if launch_id != self.launch_id {
            return Err(Refusal::InvalidLaunch);
        }
        Ok(())
    }

    /// Transport readiness only. It never installs or verifies authority.
    pub(super) fn acknowledge(&mut self, launch_id: u128) -> Result<(), Refusal> {
        self.check_launch(launch_id)?;
        if self.acknowledged {
            return Err(Refusal::Replay);
        }
        self.acknowledged = true;
        Ok(())
    }

    pub(super) fn transport_ready(&self) -> bool {
        self.acknowledged && self.closed.is_none()
    }

    pub(super) fn begin(
        &mut self,
        launch_id: u128,
        request_id: u128,
        intent: I,
        authority: Option<&dyn ExactOwnerConsent<I>>,
    ) -> Result<Submission<I>, Refusal> {
        self.check_launch(launch_id)?;
        if !self.acknowledged {
            return Err(Refusal::HandshakeRequired);
        }
        if request_id == 0 {
            return Err(Refusal::InvalidRequest);
        }
        if self.seen.contains(&request_id) {
            return Err(Refusal::Replay);
        }
        if self.seen.len() == MAX_REQUESTS_PER_LAUNCH {
            return Err(Refusal::Capacity);
        }
        // Every structurally valid request is spent before verification or any
        // future await/CAS. Refusal never permits automatic replay of its ID.
        self.seen.insert(request_id);
        if self.pending.is_some() {
            return Err(Refusal::Busy);
        }
        let Some(authority) = authority else {
            return Err(Refusal::OwnerChannelUnavailable);
        };
        if !authority.verify_exact(launch_id, request_id, &intent) {
            return Err(Refusal::ConsentDenied);
        }
        self.pending = Some(Pending {
            request_id,
            intent: intent.clone(),
            phase: Phase::Applying,
        });
        Ok(Submission {
            launch_id,
            request_id,
            intent,
        })
    }

    /// Called only after the canonical use case returns. The operation stays
    /// in flight until its reply has actually been written to the owned pipe.
    pub(super) fn reply_ready(&mut self, permit: Submission<I>) -> Result<Reply<I>, Refusal> {
        self.check_launch(permit.launch_id)?;
        let Some(pending) = self.pending.as_mut() else {
            return Err(Refusal::ReplyMismatch);
        };
        if pending.request_id != permit.request_id
            || pending.intent != permit.intent
            || pending.phase != Phase::Applying
        {
            return Err(Refusal::ReplyMismatch);
        }
        pending.phase = Phase::ReplyPending;
        Ok(Reply(permit))
    }

    pub(super) fn reply_flushed(&mut self, reply: Reply<I>) -> Result<(), Refusal> {
        self.check_launch(reply.0.launch_id)?;
        let Some(pending) = self.pending.as_ref() else {
            return Err(Refusal::ReplyMismatch);
        };
        if pending.request_id != reply.0.request_id
            || pending.intent != reply.0.intent
            || pending.phase != Phase::ReplyPending
        {
            return Err(Refusal::ReplyMismatch);
        }
        self.pending = None;
        Ok(())
    }

    /// Closing is sticky. Repeated cleanup can never turn an uncertain apply
    /// into a claim of cancellation or success. A new process needs a new
    /// launch and new explicit consent; no consent or ID store is persisted.
    pub(super) fn disconnect(&mut self) -> Disconnect {
        if let Some(outcome) = self.closed {
            return outcome;
        }
        let outcome = match self.pending.take() {
            Some(pending) => Disconnect::OutcomeUnconfirmed {
                request_id: pending.request_id,
            },
            None => Disconnect::Idle,
        };
        self.closed = Some(outcome);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &[u8]) -> Vec<u8> {
        [FRAME_PREFIX, payload, b"\n"].concat()
    }

    #[test]
    fn fragmented_and_coalesced_frames_keep_exact_payloads() {
        let mut reader = FrameBuffer::default();
        let all = [
            frame(br#"{"kind":"hello_ack"}"#),
            frame(br#"{"kind":"apply"}"#),
        ]
        .concat();
        let mut observed = Vec::new();
        for chunk in all.chunks(3) {
            for byte in chunk {
                if let Some(payload) = reader.push(*byte).unwrap() {
                    observed.push(payload);
                }
            }
        }
        assert_eq!(
            observed,
            vec![
                br#"{"kind":"hello_ack"}"#.to_vec(),
                br#"{"kind":"apply"}"#.to_vec()
            ]
        );
        assert_eq!(reader.eof(), Ok(()));
    }

    #[test]
    fn exact_frame_byte_limit_includes_prefix_and_newline() {
        let payload = vec![b' '; MAX_FRAME_BYTES - FRAME_PREFIX.len() - 1];
        let mut reader = FrameBuffer::default();
        let mut completed = None;
        for byte in frame(&payload) {
            completed = reader.push(byte).unwrap().or(completed);
        }
        assert_eq!(completed, Some(payload));
    }

    #[test]
    fn oversized_frame_fails_closed_before_dispatch() {
        let mut reader = FrameBuffer::default();
        for _ in 0..MAX_FRAME_BYTES - 1 {
            assert_eq!(reader.push(b'x'), Ok(None));
        }
        assert_eq!(reader.push(b'x'), Err(FrameFault::TooLarge));
        assert!(reader.bytes.is_empty());
        assert_eq!(reader.push(b'\n'), Err(FrameFault::Closed));
    }

    #[test]
    fn unframed_logs_or_invalid_utf8_are_not_control_messages() {
        for (line, expected) in [
            (b"ordinary log\n".to_vec(), FrameFault::InvalidPrefix),
            (frame(&[0xff]), FrameFault::InvalidUtf8),
        ] {
            let mut reader = FrameBuffer::default();
            let mut error = None;
            for byte in line {
                if let Err(failure) = reader.push(byte) {
                    error = Some(failure);
                    break;
                }
            }
            assert_eq!(error, Some(expected));
            assert_eq!(reader.push(b'x'), Err(FrameFault::Closed));
        }
    }

    #[test]
    fn partial_frame_at_eof_cannot_become_a_request() {
        let mut reader = FrameBuffer::default();
        for byte in FRAME_PREFIX {
            reader.push(*byte).unwrap();
        }
        assert_eq!(reader.eof(), Err(FrameFault::Truncated));
        assert_eq!(reader.push(b'\n'), Err(FrameFault::Closed));
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Intent {
        supervisor: &'static str,
        incarnation: u128,
        revision: u64,
        projects: Vec<&'static str>,
    }

    fn intent() -> Intent {
        Intent {
            supervisor: "bamboo-default-supervisor",
            incarnation: 9,
            revision: 4,
            projects: vec!["project-a", "project-b"],
        }
    }

    // Mock consent only. This is neither a platform identity verifier nor a
    // production way to admit a parent process.
    struct MockConsent {
        launch: u128,
        request: u128,
        exact: Intent,
    }

    impl ExactOwnerConsent<Intent> for MockConsent {
        fn verify_exact(&self, launch: u128, request: u128, observed: &Intent) -> bool {
            self.launch == launch && self.request == request && self.exact == *observed
        }
    }

    fn consent(request: u128) -> MockConsent {
        MockConsent {
            launch: 11,
            request,
            exact: intent(),
        }
    }

    fn ready() -> Session<Intent> {
        let mut session = Session::new(11).unwrap();
        session.acknowledge(11).unwrap();
        session
    }

    #[test]
    fn handshake_never_supplies_owner_authority() {
        let mut session = ready();
        assert!(session.transport_ready());
        assert_eq!(
            session.begin(11, 1, intent(), None).unwrap_err(),
            Refusal::OwnerChannelUnavailable
        );
        assert_eq!(session.disconnect(), Disconnect::Idle);
    }

    #[test]
    fn no_handshake_or_invalid_identifiers_never_admit_an_apply() {
        assert!(matches!(
            Session::<Intent>::new(0),
            Err(Refusal::InvalidLaunch)
        ));
        let mut session = Session::new(11).unwrap();
        assert_eq!(
            session
                .begin(11, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::HandshakeRequired
        );
        assert_eq!(session.acknowledge(12), Err(Refusal::InvalidLaunch));
        session.acknowledge(11).unwrap();
        assert_eq!(session.acknowledge(11), Err(Refusal::Replay));
        assert_eq!(
            session
                .begin(11, 0, intent(), Some(&consent(0)))
                .unwrap_err(),
            Refusal::InvalidRequest
        );
    }

    #[test]
    fn mock_consent_binds_every_intent_field_and_request_identity() {
        let original = intent();
        let mut changes = Vec::new();
        let mut changed = original.clone();
        changed.supervisor = "ordinary-root";
        changes.push(changed);
        let mut changed = original.clone();
        changed.incarnation += 1;
        changes.push(changed);
        let mut changed = original.clone();
        changed.revision += 1;
        changes.push(changed);
        let mut changed = original.clone();
        changed.projects.pop();
        changes.push(changed);
        let mut changed = original.clone();
        changed.projects.push("project-c");
        changes.push(changed);
        changes.push(original);
        for (index, changed) in changes.into_iter().enumerate() {
            let request = (index + 1) as u128;
            let proof = MockConsent {
                request: if index == 5 { 99 } else { request },
                ..consent(request)
            };
            let mut session = ready();
            assert_eq!(
                session
                    .begin(11, request, changed, Some(&proof))
                    .unwrap_err(),
                Refusal::ConsentDenied
            );
            assert_eq!(session.disconnect(), Disconnect::Idle);
        }
    }

    #[test]
    fn accepted_payload_is_immutable_and_a_second_submission_is_rejected() {
        let mut session = ready();
        let mut caller_value = intent();
        let permit = session
            .begin(11, 1, caller_value.clone(), Some(&consent(1)))
            .unwrap();
        caller_value.projects.clear();
        assert_eq!(permit.intent(), &intent());
        assert_eq!(
            session
                .begin(11, 2, intent(), Some(&consent(2)))
                .unwrap_err(),
            Refusal::Busy
        );
        let reply = session.reply_ready(permit).unwrap();
        session.reply_flushed(reply).unwrap();
        assert_eq!(
            session
                .begin(11, 1, caller_value, Some(&consent(1)))
                .unwrap_err(),
            Refusal::Replay
        );
        assert_eq!(
            session
                .begin(11, 2, intent(), Some(&consent(2)))
                .unwrap_err(),
            Refusal::Replay
        );
    }

    #[test]
    fn refusal_spends_request_id_without_admitting_a_write() {
        let mut session = ready();
        assert_eq!(
            session.begin(11, 1, intent(), None).unwrap_err(),
            Refusal::OwnerChannelUnavailable
        );
        assert_eq!(
            session
                .begin(11, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::Replay
        );
        assert_eq!(session.disconnect(), Disconnect::Idle);
    }

    #[test]
    fn disconnect_during_apply_is_sticky_unknown_and_old_permit_cannot_complete() {
        let mut session = ready();
        let permit = session.begin(11, 1, intent(), Some(&consent(1))).unwrap();
        let unknown = Disconnect::OutcomeUnconfirmed { request_id: 1 };
        assert_eq!(session.disconnect(), unknown);
        assert_eq!(session.disconnect(), unknown);
        assert_eq!(session.reply_ready(permit).unwrap_err(), Refusal::Closed);
        assert_eq!(
            session
                .begin(11, 2, intent(), Some(&consent(2)))
                .unwrap_err(),
            Refusal::Closed
        );
    }

    #[test]
    fn loss_after_cas_before_reply_flush_is_still_unknown() {
        let mut session = ready();
        let permit = session.begin(11, 1, intent(), Some(&consent(1))).unwrap();
        let reply = session.reply_ready(permit).unwrap();
        assert_eq!(
            session.disconnect(),
            Disconnect::OutcomeUnconfirmed { request_id: 1 }
        );
        assert_eq!(session.reply_flushed(reply), Err(Refusal::Closed));
    }

    #[test]
    fn a_flushed_reply_releases_only_the_inflight_slot_not_replay_truth() {
        let mut session = ready();
        let permit = session.begin(11, 1, intent(), Some(&consent(1))).unwrap();
        let reply = session.reply_ready(permit).unwrap();
        session.reply_flushed(reply).unwrap();
        assert!(session.begin(11, 2, intent(), Some(&consent(2))).is_ok());
        assert_eq!(
            session
                .begin(11, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::Replay
        );
    }

    #[test]
    fn restart_rejects_old_launch_even_with_a_matching_old_mock_consent() {
        let mut new_process = Session::new(12).unwrap();
        new_process.acknowledge(12).unwrap();
        assert_eq!(
            new_process
                .begin(11, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::InvalidLaunch
        );
        assert_eq!(
            new_process
                .begin(12, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::ConsentDenied
        );
    }

    #[test]
    fn bounded_request_history_never_evicts_an_old_id() {
        let mut session = ready();
        for request in 1..=MAX_REQUESTS_PER_LAUNCH as u128 {
            let permit = session
                .begin(11, request, intent(), Some(&consent(request)))
                .unwrap();
            let reply = session.reply_ready(permit).unwrap();
            session.reply_flushed(reply).unwrap();
        }
        assert_eq!(
            session
                .begin(11, 65, intent(), Some(&consent(65)))
                .unwrap_err(),
            Refusal::Capacity
        );
        assert_eq!(
            session
                .begin(11, 1, intent(), Some(&consent(1)))
                .unwrap_err(),
            Refusal::Replay
        );
    }
}
