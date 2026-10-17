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
            let store = bamboo_projects::ProjectStore::open(self.root.clone())
                .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
            let project = store
                .get(&command.project_id)
                .map_err(|e| Error::InvalidTransition(e.to_string()))?;
            if project.status != bamboo_domain::ProjectStatus::Active {
                return Err(Error::InvalidTransition("Project is archived".into()));
            }
        }
        service.execute_sprint(&authority, command)
    }
}
