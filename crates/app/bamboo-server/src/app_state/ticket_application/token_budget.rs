//! Explicit owner setup/readback of the original Ticket token ledger.
use super::*;
use bamboo_tickets::{ExecutionTokenUsage, WorkTokenConfiguration, WorkTokenLimits};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTokenBudgetSetup {
    pub binding: ScopeBinding,
    pub authority_epoch: u64,
    /// The requested decision is configuration expected_revision + 1.
    pub expected_revision: u64,
    pub limits: WorkTokenLimits,
}

fn owner(principal: &Principal) -> Result<()> {
    if matches!(principal, Principal::User { user_id } if user_id == "host-owner") {
        Ok(())
    } else {
        Err(Error::ScopeDenied("verified owner access required".into()))
    }
}

impl TicketApplication {
    /// Bounded current limits/counters and at most one historical decision.
    /// No scope initialization, clock rollover, execution grant or model call.
    pub async fn work_token_budget_status(
        &self,
        principal: Principal,
        configuration_revision: Option<u64>,
    ) -> Result<Value> {
        owner(&principal)?;
        let (service, _) = self.authority(principal).await?;
        let (commit, snapshot) = service.published()?;
        let budget = snapshot.execution_budget.as_ref();
        let requested = configuration_revision
            .map(|revision| {
                budget
                    .and_then(|budget| budget.configurations.get(&revision))
                    .cloned()
                    .ok_or_else(|| Error::InvalidTransition("budget decision not found".into()))
            })
            .transpose()?;
        let accounting = budget.map(|budget| {
            let pending = budget
                .reservations
                .values()
                .filter(|r| r.usage.is_none())
                .count();
            let unknown = budget
                .reservations
                .values()
                .filter(|r| r.usage == Some(ExecutionTokenUsage::Unknown))
                .count();
            let over_cap = budget
                .reservations
                .values()
                .filter(|r| r.over_cap_usage.is_some())
                .count();
            json!({
                "total_charged":budget.total_charged,
                "daily_charged":budget.daily_charged,
                "accounting_utc_day":budget.current_utc_day,
                "last_reservation_ms":budget.last_reservation_ms,
                "accounting_halted":over_cap > 0,
                "work_charged":budget.work_charged,
                "reservations":{
                    "total":budget.reservations.len(),"pending_usage":pending,
                    "unknown_usage":unknown,
                    "known_usage":budget.reservations.len() - pending - unknown,
                    "over_cap_usage":over_cap,
                },
            })
        });
        Ok(json!({
            "binding":snapshot.binding,
            "observed":{"commit":commit,"seq":snapshot.seq,"authority_epoch":snapshot.authority_epoch},
            "configured":budget.is_some(),
            "budget_revision":budget.map_or(0, |budget| budget.revision),
            "current_configuration":budget.and_then(|budget| budget.configurations.get(&budget.revision)),
            "requested_configuration":requested,"accounting":accounting,
        }))
    }

    pub async fn configure_work_token_budget(
        &self,
        principal: Principal,
        request: WorkTokenBudgetSetup,
    ) -> Result<WorkTokenConfiguration> {
        owner(&principal)?;
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        if service.published()?.1.binding != request.binding {
            return Err(Error::ScopeDenied("scope binding changed".into()));
        }
        let receipt = service.configure_work_token_budget_at_epoch(
            &authority,
            request.authority_epoch,
            request.expected_revision,
            request.limits,
        )?;
        self.wake_ready_work();
        Ok(receipt)
    }
}
