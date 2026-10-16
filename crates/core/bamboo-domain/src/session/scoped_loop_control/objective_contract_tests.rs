//! The unpublished remote contract is intentionally narrower than self control.
//! These are stateless wire checks, not runtime/host authority grants.
use super::*;

fn binding() -> ScopedLoopControlBinding {
    ScopedLoopControlBinding {
        grant_id: Uuid::from_u128(1),
        supervisor: SupervisorReference {
            session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: Uuid::from_u128(2),
        },
        supervisor_tool_authority_revision: 0,
        target_session_id: "remote-objective-target".into(),
        target_created_at: Utc::now(),
        target_metadata_version: 0,
        target_project_id: "remote-objective-project".parse().unwrap(),
        link_revision: 1,
        owner_run_id: "remote-objective-owner".into(),
        execution_id: "remote-objective-execution".into(),
        allowed_actions: [
            LoopControlAction::Status,
            LoopControlAction::Start,
            LoopControlAction::Update,
            LoopControlAction::Stop,
        ]
        .into_iter()
        .collect(),
        max_iterations: 3,
        max_total_rounds: 12,
    }
}

fn start(objective: &str) -> LoopControlRequest {
    LoopControlRequest::Start {
        operation_id: Uuid::from_u128(3).to_string(),
        expected_generation: None,
        expected_revision: 0,
        objective: objective.into(),
        max_iterations: 3,
        max_total_rounds: 12,
    }
}

fn update(objective: Option<String>) -> LoopControlRequest {
    LoopControlRequest::Update {
        operation_id: Uuid::from_u128(4).to_string(),
        generation: Uuid::from_u128(5).to_string(),
        expected_revision: 0,
        objective,
        max_iterations: None,
        max_total_rounds: Some(10),
    }
}

#[test]
fn scoped_objective_scalar_limit_explicitly_preserves_the_self_byte_contract() {
    let binding = binding();
    binding.validate().unwrap();
    for (label, objective, self_valid, remote_valid) in [
        ("ASCII 4096", "x".repeat(4096), true, true),
        ("CJK 4096", "界".repeat(4096), true, true),
        ("emoji 4096 exact 16 KiB", "😀".repeat(4096), true, true),
        ("mixed 4096", "aé界😀".repeat(1024), true, true),
        ("FEFF is content", "\u{feff}".repeat(4096), true, true),
        (
            "NEL around content",
            format!("{}x", "\u{0085}".repeat(4095)),
            true,
            true,
        ),
        (
            "raw spaces count",
            format!("{}x", " ".repeat(4096)),
            true,
            false,
        ),
        ("ASCII 4097", "x".repeat(4097), true, false),
        ("old candidate ASCII 16 KiB", "x".repeat(16384), true, false),
        ("old candidate CJK 5000", "界".repeat(5000), true, false),
        (
            "old candidate mixed exact 16 KiB",
            format!("{}abcd", "aé界😀".repeat(1638)),
            true,
            false,
        ),
        ("reported CJK 5500", "界".repeat(5500), false, false),
        ("emoji 4097", "😀".repeat(4097), false, false),
        ("NEL only", "\u{0085}".repeat(4096), false, false),
    ] {
        for command in [start(&objective), update(Some(objective.clone()))] {
            let before = serde_json::to_value(&command).unwrap();
            assert_eq!(command.validate().is_ok(), self_valid, "{label}: self DTO");
            assert_eq!(
                binding.authorize(&command).is_ok(),
                remote_valid,
                "{label}/{}: remote contract",
                command.action()
            );
            assert_eq!(
                serde_json::to_value(command).unwrap(),
                before,
                "validation must not trim, normalize or rewrite objective text"
            );
        }
    }
    let bound_only = update(None);
    let value = serde_json::to_value(&bound_only).unwrap();
    assert!(value["objective"].is_null());
    binding.authorize(&bound_only).unwrap();
    binding.authorize(&LoopControlRequest::Status).unwrap();
}

#[test]
fn typed_remote_carrier_cannot_bypass_the_scalar_limit() {
    let input = ScopedLoopControlInput {
        binding: binding(),
        command: start("seed"),
    };
    let envelope = input.envelope().unwrap();
    for (objective, valid) in [
        ("😀".repeat(4096), true),
        ("x".repeat(4097), false),
        ("界".repeat(5000), false),
    ] {
        // The stable operation/input identity does not hash objective text.
        // Mutating only typed data isolates carrier parsing from Tool/Messenger.
        let mut carrier = envelope.clone();
        let SessionMessageBody::RuntimeInstruction(instruction) = &mut carrier.body else {
            unreachable!()
        };
        instruction.data.as_mut().unwrap()["command"]["objective"] = serde_json::json!(&objective);
        let parsed = scoped_loop_control_input(&carrier);
        assert_eq!(
            parsed.is_ok(),
            valid,
            "{} scalars",
            objective.chars().count()
        );
        if valid {
            let parsed = parsed.unwrap().unwrap();
            assert_eq!(parsed.command, start(&objective));
            assert_eq!(parsed.envelope().unwrap().id, envelope.id);
        }
    }
}
