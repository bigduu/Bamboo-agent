use std::panic::{catch_unwind, AssertUnwindSafe};

use super::*;

impl SqliteMetricsStorage {
    pub(super) async fn apply_mutations(
        &self,
        mutations: Vec<MetricsMutation>,
    ) -> MetricsResult<Vec<MetricsResult<()>>> {
        let mut results = Vec::with_capacity(mutations.len());
        let mut mutations = mutations.into_iter();
        loop {
            let segment = mutations
                .by_ref()
                .take(MAX_METRICS_BATCH_SIZE)
                .collect::<Vec<_>>();
            if segment.is_empty() {
                return Ok(results);
            }
            let path = self.db_path.clone();
            let completed = tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                batch_probe::task_started(&path);
                apply_segment(&path, segment)
            })
            .await
            .map_err(|error| MetricsError::Task(error.to_string()))?;
            results.extend(completed);
        }
    }
}

fn apply_segment(path: &Path, mutations: Vec<MetricsMutation>) -> Vec<MetricsResult<()>> {
    let mut results = Vec::with_capacity(mutations.len());
    let mut connection = None;
    for mutation in mutations {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            if connection.is_none() {
                connection = Some(open_connection(path)?);
            }
            let connection = connection.as_ref().expect("connection just opened");
            writes::apply_mutation_on_connection(connection, mutation)?;
            if !connection.is_autocommit() {
                return Err(MetricsError::Task(
                    "metrics command left a transaction open".into(),
                ));
            }
            Ok(())
        }));
        let result = match outcome {
            Ok(result) => result,
            Err(payload) => {
                let detail = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic");
                Err(MetricsError::Task(format!(
                    "metrics command panicked: {}",
                    detail.chars().take(512).collect::<String>()
                )))
            }
        };
        if result.is_err() {
            // A dropped connection rolls back anything left by a panic or
            // failed rollback. Reopen for the next item, without replaying any
            // command or letting it join the failed item's transaction.
            drop(connection.take());
        }
        results.push(result);
    }
    results
}
