use super::*;

async fn existing(data: &Path) -> anyhow::Result<(TicketService, Authority)> {
    let source = read_source(data, DEFAULT_SUPERVISOR_SESSION_ID).await?;
    let root = scope_root(data, &source.session)?;
    ensure!(
        root.join("HEAD").is_file(),
        "existing Ticket authority required"
    );
    // The lifetime OS lock rejects a live Host. Run stop proof remains the
    // existing trusted Runtime's durable fact, never a CLI boolean or PID guess.
    let service = offline_service(data, &source.session)?;
    let binding = service.published()?.1.binding;
    Ok((service, owner(binding)))
}

pub(super) async fn plan(
    data: &Path,
    assignment: &str,
    effect: &str,
    operation: &str,
    evidence: &str,
) -> anyhow::Result<Value> {
    let (service, authority) = existing(data).await?;
    Ok(serde_json::to_value(service.file_reconciliation_plan(
        &authority, assignment, effect, operation, evidence,
    )?)?)
}

pub(super) async fn commit(data: &Path, path: &Path) -> anyhow::Result<Value> {
    ensure!(
        fs::metadata(path)?.len() <= 32 * 1024,
        "file request exceeds budget"
    );
    let bytes = fs::read(path)?;
    ensure!(bytes.len() <= 32 * 1024, "file request exceeds budget");
    let request: FileReconcileRequest = serde_json::from_slice(&bytes)?;
    let (service, authority) = existing(data).await?;
    Ok(serde_json::to_value(
        service.reconcile_file_effect(&authority, &request)?,
    )?)
}
