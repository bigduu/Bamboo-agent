//! Planning-only host adapter. Runtime dispatch and stop are deliberately absent.
use super::*;

impl TicketApplication {
    pub async fn sprint_command(
        &self,
        principal: Principal,
        command: &SprintCommand,
    ) -> Result<SprintReceipt> {
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        // Preserve exact committed retries even after Project metadata changes.
        // The canonical service still validates subject, request hash and binding.
        if !service
            .published()?
            .1
            .receipts
            .contains_key(&command.operation_id)
        {
            self.validate_project_context(&command.project_id, None, false)?;
        }
        service.execute_sprint(&authority, command)
    }
}
