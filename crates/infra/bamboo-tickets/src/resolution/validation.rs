use super::*;
use crate::service::validate_authority;

pub(super) fn user(
    authority: &Authority,
    snapshot: &Snapshot,
    record: &HumanIngressRecord,
) -> Result<()> {
    validate_authority(authority, snapshot)?;
    authority.user()?;
    match authority.principal() {
        Principal::User { user_id } if user_id == &record.user_id => Ok(()),
        _ => Err(Error::ScopeDenied("Human ingress subject differs".into())),
    }
}

pub(super) fn group_operation_id(message_id: &str, group_id: &str) -> String {
    format!(
        "message/{}/{}",
        content_hash(message_id.as_bytes()),
        content_hash(group_id.as_bytes())
    )
}

pub(super) fn group_status(error: &Error) -> ResolutionStatus {
    match error {
        Error::RevisionConflict => ResolutionStatus::Stale,
        Error::InvalidTransition(reason) if reason.starts_with("needs_clarification:") => {
            ResolutionStatus::NeedsClarification
        }
        _ => ResolutionStatus::Rejected,
    }
}
fn clarify(reason: &str) -> Error {
    Error::InvalidTransition(format!("needs_clarification: {reason}"))
}

pub(super) fn validate_shape(proposal: &MessageProposal) -> Result<()> {
    if proposal.groups.len() > 16
        || canonical_bytes(proposal)?.len() > 131072
        || proposal
            .groups
            .iter()
            .map(|g| g.operations.len())
            .sum::<usize>()
            > 64
    {
        return Err(Error::ContextBudgetExceeded);
    }
    let mut groups = BTreeSet::new();
    let mut items = BTreeSet::new();
    for group in &proposal.groups {
        if group.group_id.is_empty()
            || group.group_id.len() > 64
            || !groups.insert(&group.group_id)
            || group.item_ids.is_empty()
            || group.item_ids.len() > 16
            || group
                .item_ids
                .iter()
                .any(|id| id.is_empty() || id.len() > 64 || !items.insert(id))
            || group.source_quote.len() > 32768
            || group
                .clarification
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 2048)
        {
            return Err(Error::InvalidTransition(
                "invalid semantic group/item bounds".into(),
            ));
        }
    }
    Ok(())
}

fn reference(snapshot: &Snapshot, target: &TicketReference) -> Result<()> {
    let TicketReference::Existing {
        id,
        record_revision,
        contract_revision,
        generation,
    } = target
    else {
        return Ok(());
    };
    if snapshot.tickets.get(id).is_none_or(|t| {
        t.record_revision != *record_revision
            || t.contract_revision != *contract_revision
            || t.generation != *generation
    }) {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
fn request<'a>(snapshot: &'a Snapshot, target: &RequestReference) -> Result<&'a PendingRequest> {
    let q = snapshot
        .requests
        .get(&target.request_id)
        .ok_or(Error::RevisionConflict)?;
    if q.work_id != target.work_id
        || q.assignment_id != target.assignment_id
        || q.generation != target.generation
        || q.contract_revision != target.contract_revision
        || q.prompt_revision != target.prompt_revision
        || q.status != RequestStatus::Open
    {
        return Err(Error::RevisionConflict);
    }
    let work = snapshot
        .tickets
        .get(&q.work_id)
        .ok_or(Error::RevisionConflict)?;
    if work.generation != q.generation || work.contract_revision != q.contract_revision {
        return Err(Error::RevisionConflict);
    }
    Ok(q)
}

fn conditional_text(text: &str) -> bool {
    [
        "如果", "假如", "假设", "例如", "比如", "前提", "条件", "除非", "只有", "只要", "一旦",
        "等到", "仅当", "之后", "以后",
    ]
    .iter()
    .any(|marker| text.contains(marker))
        || text.split(|c: char| !c.is_alphabetic()).any(|word| {
            matches!(
                word,
                "if" | "when"
                    | "unless"
                    | "once"
                    | "until"
                    | "assuming"
                    | "provided"
                    | "before"
                    | "after"
                    | "conditional"
                    | "contingent"
            )
        })
}

fn human_sentence<'a>(record: &'a HumanIngressRecord, quote: &str) -> Option<&'a str> {
    // A comma may separate a condition or denial from its action. The model
    // cannot discard either by quoting only the affirmative fragment. A
    // separate sentence/semicolon action retains its own approval evidence.
    let mut matches = record
        .text
        .split(['；', ';', '。', '\n'])
        .filter(|sentence| {
            sentence
                .split(['，', ','])
                .any(|part| part.trim() == quote.trim())
        });
    let sentence = matches.next()?;
    matches.next().is_none().then_some(sentence)
}

fn unconditional_human_context(record: &HumanIngressRecord, quote: &str) -> bool {
    human_sentence(record, quote).is_some_and(|sentence| {
        let lower = sentence.to_lowercase();
        !conditional_text(&lower) && !refused_human_text(&lower)
    })
}

fn refused_human_text(text: &str) -> bool {
    [
        "不", "未", "暂", "如果", "假如", "假设", "例如", "比如", "之前", "?", "？", "don't",
        "don’t", "can't", "can’t", "hold off", "拒绝", "取消", "撤销", "别", "勿", "但", "\"", "“",
        "「", "`", ">",
    ]
    .iter()
    .any(|marker| text.contains(marker))
        || text.split(|c: char| !c.is_alphabetic()).any(|word| {
            matches!(
                word,
                "no" | "not"
                    | "never"
                    | "reject"
                    | "decline"
                    | "but"
                    | "however"
                    | "except"
                    | "maybe"
                    | "wait"
                    | "example"
            )
        })
}

fn explicit_amount_matches(text: &str, amount: Option<&str>) -> bool {
    let markers: Vec<_> = text
        .match_indices("金额")
        .map(|(offset, marker)| offset + marker.len())
        .chain(text.match_indices("amount").filter_map(|(offset, marker)| {
            let before = text[..offset].chars().next_back();
            let after = text[offset + marker.len()..].chars().next();
            (before.is_none_or(|c| !c.is_alphabetic()) && after.is_none_or(|c| !c.is_alphabetic()))
                .then_some(offset + marker.len())
        }))
        .collect();
    if markers.is_empty() {
        return true;
    }
    let Some(amount) = amount else {
        return false;
    };
    // Amount is an opaque action field, including its currency/unit. Accept
    // only one complete, exact stated value; uncertain formatting or multiple
    // values require clarification rather than substring/numeric guessing.
    markers.len() == 1
        && text[markers[0]..]
            .trim_start_matches(|c: char| c.is_whitespace() || matches!(c, ':' | '：' | '='))
            .trim_end_matches(['!', '！'])
            .trim()
            == amount.trim().to_lowercase()
}

fn exact_name(text: &str, name: &str) -> bool {
    let reference_char = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-');
    text.match_indices(name).any(|(offset, _)| {
        let before = &text[..offset];
        let after = &text[offset + name.len()..];
        let left = before
            .chars()
            .next_back()
            .is_none_or(|c| !reference_char(c))
            || [
                "批准",
                "同意",
                "拒绝",
                "不批准",
                "不要批准",
                "不同意",
                "禁止",
                "确认验收",
                "验收通过",
                "接受交付",
                "接受成果",
            ]
            .contains(&before.trim());
        left && after.chars().next().is_none_or(|c| !reference_char(c))
    })
}

fn names_work(snapshot: &Snapshot, quote: &str, work: &Ticket) -> bool {
    if exact_name(quote, &work.id) {
        return true;
    }
    let title = &work.contract.title;
    snapshot
        .tickets
        .values()
        .filter(|other| &other.contract.title == title)
        .count()
        == 1
        && exact_name(quote, title)
        && !snapshot.tickets.values().any(|other| {
            other.id != work.id
                && other.contract.title.contains(title)
                && quote.contains(&other.contract.title)
        })
}

fn approval_text(
    snapshot: &Snapshot,
    record: &HumanIngressRecord,
    quote: &str,
    q: &PendingRequest,
    approve: bool,
) -> Result<()> {
    let work = &snapshot.tickets[&q.work_id];
    let unique_approval = snapshot
        .requests
        .values()
        .filter(|other| {
            other.work_id == q.work_id
                && other.generation == q.generation
                && other.contract_revision == q.contract_revision
                && other.status == RequestStatus::Open
                && matches!(other.kind, RequestKind::Approval { .. })
        })
        .count()
        == 1;
    if !(exact_name(quote, &q.id) || (unique_approval && names_work(snapshot, quote, work))) {
        return Err(clarify(
            "approval must identify one exact current Work/request",
        ));
    }
    // A quote cannot hide a preceding negation/example. Approval proof uses a
    // complete Human clause, never a substring extracted from external data.
    if !record
        .text
        .split(['，', ',', '；', ';', '。', '\n'])
        .any(|part| part.trim() == quote.trim())
    {
        return Err(clarify(
            "approval evidence must be a complete current Human clause",
        ));
    }
    let lower = quote.trim().to_lowercase();
    let positive = ["批准", "同意", "approve ", "i approve "]
        .iter()
        .any(|p| lower.starts_with(p));
    let negative = [
        "拒绝",
        "不批准",
        "不要批准",
        "不同意",
        "禁止",
        "deny ",
        "reject ",
    ]
    .iter()
    .any(|p| lower.starts_with(p));
    if approve {
        if !positive
            || !unconditional_human_context(record, quote)
            || conditional_text(&lower)
            || [
                "?",
                "？",
                "如果",
                "假如",
                "假设",
                "比如",
                "例如",
                "之前",
                "不要",
                "不能",
                "不批准",
                "不同意",
                "不发送",
                "not ",
                "don't ",
            ]
            .iter()
            .any(|s| lower.contains(s))
        {
            return Err(clarify(
                "ambiguous, conditional or negative text cannot approve",
            ));
        }
        if [
            "改金额",
            "金额改",
            "改为",
            "改成",
            "调整金额",
            "change amount",
        ]
        .iter()
        .any(|s| record.text.to_lowercase().contains(s))
        {
            return Err(clarify(
                "a changed action/amount requires a fresh approval request",
            ));
        }
        if let RequestKind::Approval { action, .. } = &q.kind {
            if human_sentence(record, quote).is_none_or(|sentence| {
                !explicit_amount_matches(&sentence.to_lowercase(), action.amount.as_deref())
            }) {
                return Err(clarify(
                    "explicit Human amount must exactly match the approved action",
                ));
            }
        }
    } else if !negative {
        return Err(clarify("denial must explicitly identify the denied action"));
    }
    Ok(())
}

fn answer_text(
    snapshot: &Snapshot,
    record: &HumanIngressRecord,
    quote: &str,
    q: &PendingRequest,
    answer: &str,
) -> Result<()> {
    let work = &snapshot.tickets[&q.work_id];
    let unique_question = snapshot
        .requests
        .values()
        .filter(|other| {
            other.work_id == q.work_id
                && other.generation == q.generation
                && other.contract_revision == q.contract_revision
                && other.status == RequestStatus::Open
                && other.kind == RequestKind::Question
        })
        .count()
        == 1;
    // User authority requires a verbatim answer in a complete, explicitly
    // addressed Human sentence. Pending Worker prompts are not answer evidence.
    // Work names are usable only for one current question; otherwise the Human
    // must identify the exact request. Ambiguous natural language clarifies.
    let explicit = std::iter::once(q.id.as_str())
        .chain(unique_question.then_some(work.id.as_str()))
        .chain(
            (unique_question
                && work.contract.title == work.contract.title.trim()
                && !snapshot.tickets.contains_key(&work.contract.title)
                && !snapshot.requests.contains_key(&work.contract.title)
                && snapshot
                    .tickets
                    .values()
                    .filter(|other| other.contract.title.trim() == work.contract.title)
                    .count()
                    == 1
                && !snapshot.tickets.values().any(|other| {
                    other.id != work.id
                        && other.contract.title.contains(&work.contract.title)
                        && quote.contains(&other.contract.title)
                }))
            .then_some(work.contract.title.as_str()),
        )
        .any(|name| {
            quote.trim() == format!("{name} 的答案是{answer}")
                || quote.trim() == format!("{name}使用{answer}")
                || quote.trim() == format!("{name} 使用{answer}")
                || quote.trim() == format!("Answer {name}: {answer}")
                || quote.trim() == format!("answer {name}: {answer}")
        });
    if answer.trim().is_empty()
        || answer != answer.trim()
        || !explicit
        || human_sentence(record, quote).is_none_or(|sentence| sentence.trim() != quote.trim())
    {
        return Err(clarify(
            "question answers need an exact, unambiguous named Human sentence and verbatim answer",
        ));
    }
    Ok(())
}

fn acceptance_text(
    snapshot: &Snapshot,
    record: &HumanIngressRecord,
    quote: &str,
    target: &TicketReference,
    submission: &str,
) -> Result<()> {
    let work = snapshot
        .tickets
        .get(target.id())
        .ok_or(Error::RevisionConflict)?;
    if !work.contract.user_acceptance_required {
        return Ok(());
    }
    let title = &work.contract.title;
    let named = names_work(snapshot, quote, work) || exact_name(quote, submission);
    let complete = record
        .text
        .split(['，', ',', '；', ';', '。', '\n'])
        .any(|part| part.trim() == quote.trim());
    let lower = quote.trim().to_lowercase();
    let affirmative = |clause: &str| {
        [
            "验收通过",
            "确认验收",
            "接受交付",
            "接受成果",
            "accept ",
            "i accept ",
        ]
        .iter()
        .any(|s| clause.starts_with(s))
    };
    let explicit = affirmative(&lower)
        || [work.id.as_str(), title.as_str(), submission]
            .iter()
            .any(|name| {
                lower
                    .strip_prefix(&name.to_lowercase())
                    .is_some_and(|rest| affirmative(rest.trim_start_matches([' ', ':', '：'])))
            });
    let refused = refused_human_text(&lower);
    if !named
        || !complete
        || !explicit
        || refused
        || conditional_text(&lower)
        || !unconditional_human_context(record, quote)
    {
        return Err(clarify("user-required acceptance needs an explicit complete current Human clause identifying the Work/submission"));
    }
    Ok(())
}

pub(super) fn validate_group(
    snapshot: &Snapshot,
    record: &HumanIngressRecord,
    group: &SemanticGroup,
) -> Result<()> {
    if let Some(reason) = &group.clarification {
        return Err(clarify(reason));
    }
    if group.operations.is_empty()
        || group.source_quote.trim().is_empty()
        || !record.text.contains(&group.source_quote)
    {
        return Err(clarify("operations require exact current Human text"));
    }
    // The saved quote must still occur verbatim in the current Human text.
    // A final existing clause delimiter belongs to that quote, while the
    // existing clause validators compare the sentence content after splitting.
    // Remove only one final delimiter, never another sentence or its context.
    let quote = group.source_quote.trim();
    let clause = quote
        .strip_suffix(|c| matches!(c, '。' | '；' | ';' | '，' | ','))
        .unwrap_or(quote)
        .trim_end();
    let mut temporary = BTreeSet::new();
    for op in &group.operations {
        for target in operation_references(op) {
            reference(snapshot, target)?;
            if let TicketReference::Temporary { id } = target {
                if !temporary.contains(id) {
                    return Err(clarify(
                        "temporary references must belong to this atomic group",
                    ));
                }
            }
        }
        match op {
            SemanticOperation::Create {
                temp_id,
                kind,
                contract,
                ..
            } => {
                if !contract.user_acceptance_required {
                    return Err(Error::ScopeDenied(
                        "model contracts must require explicit User acceptance".into(),
                    ));
                }
                if *kind == TicketKind::Step
                    || temp_id.is_empty()
                    || !temporary.insert(temp_id.clone())
                {
                    return Err(Error::InvalidTransition(
                        "duplicate temporary ID or private Step creation".into(),
                    ));
                }
            }
            SemanticOperation::Steer { contract, .. } if !contract.user_acceptance_required => {
                return Err(Error::ScopeDenied(
                    "model contracts must require explicit User acceptance".into(),
                ));
            }
            SemanticOperation::Answer { target, answer } => {
                let q = request(snapshot, target)?;
                if q.kind != RequestKind::Question {
                    return Err(Error::ScopeDenied(
                        "a question answer cannot approve an action".into(),
                    ));
                }
                answer_text(snapshot, record, clause, q, answer)?;
            }
            SemanticOperation::DecideApproval {
                target,
                fingerprint,
                approve,
            } => {
                let q = request(snapshot, target)?;
                if !matches!(&q.kind, RequestKind::Approval {fingerprint:current,..} if current.as_str() == fingerprint.as_str())
                {
                    return Err(Error::RevisionConflict);
                }
                approval_text(snapshot, record, clause, q, *approve)?;
            }
            SemanticOperation::Accept {
                target,
                submission_id,
                ..
            } => {
                acceptance_text(snapshot, record, &group.source_quote, target, submission_id)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn validate_history(snapshot: &Snapshot) -> Result<()> {
    let mut ingress = BTreeSet::new();
    for (id, resolution) in &snapshot.resolutions {
        if id != &resolution.message_id || !ingress.insert(resolution.ingress_seq) {
            return Err(Error::InvalidTransition(
                "resolution identity/sequence mismatch".into(),
            ));
        }
        let Some(human) = &resolution.ingress else {
            continue;
        };
        if human.user_id.is_empty()
            || human.source_ingress_seq == 0
            || resolution.message_hash != content_hash(&canonical_bytes(human)?)
            || resolution
                .basis
                .as_ref()
                .is_some_and(|b| b.commit.len() != 64)
            || resolution.updated_seq.is_none_or(|s| s > snapshot.seq)
        {
            return Err(Error::InvalidTransition(
                "Human resolution hash/basis mismatch".into(),
            ));
        }
        if let Some(proposal) = &resolution.proposal {
            if resolution.basis.is_none() {
                return Err(Error::InvalidTransition(
                    "proposal lacks fixed basis".into(),
                ));
            }
            validate_shape(proposal)?;
            if resolution.proposal_hash.as_deref()
                != Some(content_hash(&canonical_bytes(proposal)?).as_str())
                || proposal.groups.len() != resolution.groups.len()
            {
                return Err(Error::InvalidTransition(
                    "saved proposal hash/group mismatch".into(),
                ));
            }
            for (proposed, group) in proposal.groups.iter().zip(&resolution.groups) {
                if group.operation_id != group_operation_id(id, &proposed.group_id)
                    || group.item_ids != proposed.item_ids
                {
                    return Err(Error::InvalidTransition(
                        "saved semantic group identity changed".into(),
                    ));
                }
                if group.status == ResolutionStatus::Committed {
                    let receipt = group.receipt.as_ref().ok_or_else(|| {
                        Error::InvalidTransition("committed group lacks receipt".into())
                    })?;
                    if snapshot.receipts.get(&group.operation_id) != Some(receipt)
                        || receipt.principal != format!("user:{}", human.user_id)
                    {
                        return Err(Error::InvalidTransition(
                            "semantic receipt subject mismatch".into(),
                        ));
                    }
                } else if group.receipt.is_some() {
                    return Err(Error::InvalidTransition(
                        "uncommitted group has success receipt".into(),
                    ));
                }
            }
        } else if resolution.proposal_hash.is_some() || !resolution.groups.is_empty() {
            return Err(Error::InvalidTransition(
                "unsaved proposal has group outcomes".into(),
            ));
        }
    }
    Ok(())
}
