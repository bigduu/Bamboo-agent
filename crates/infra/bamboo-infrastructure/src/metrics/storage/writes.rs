use super::*;

pub(super) fn execute_cached<P: rusqlite::Params>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> rusqlite::Result<usize> {
    let mut statement = connection.prepare_cached(sql)?;
    #[cfg(test)]
    batch_probe::cached_statement(
        connection,
        statement.get_status(rusqlite::StatementStatus::Run),
    );
    statement.execute(params)
}

fn query_row_cached<
    T,
    P: rusqlite::Params,
    F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
>(
    connection: &Connection,
    sql: &str,
    params: P,
    map: F,
) -> rusqlite::Result<T> {
    let mut statement = connection.prepare_cached(sql)?;
    #[cfg(test)]
    batch_probe::cached_statement(
        connection,
        statement.get_status(rusqlite::StatementStatus::Run),
    );
    statement.query_row(params, map)
}

pub(super) fn apply_mutation_on_connection(
    connection: &Connection,
    mutation: MetricsMutation,
) -> MetricsResult<()> {
    let result = match mutation {
        MetricsMutation::SessionStarted {
            session_id,
            model,
            started_at,
        } => {
            let started_at = format_timestamp(started_at);

            execute_cached(
                connection,
                r#"
                INSERT INTO session_metrics (
                    session_id, model, started_at, status, updated_at
                ) VALUES (?1, ?2, ?3, 'running', ?3)
                ON CONFLICT(session_id) DO UPDATE SET
                    model = excluded.model,
                    started_at = CASE
                        WHEN session_metrics.started_at <= excluded.started_at THEN session_metrics.started_at
                        ELSE excluded.started_at
                    END,
                    completed_at = NULL,
                    status = 'running',
                    updated_at = excluded.updated_at
                "#,
                params![session_id, model, started_at],
            )?;
            Ok(())
        }
        MetricsMutation::SessionMessageCount {
            session_id,
            message_count,
            updated_at,
        } => {
            let updated_at = format_timestamp(updated_at);

            execute_cached(connection,
                "UPDATE session_metrics SET message_count = ?1, updated_at = ?2 WHERE session_id = ?3",
                params![i64::from(message_count), updated_at, session_id],
            )?;
            Ok(())
        }
        MetricsMutation::SessionCompleted {
            session_id,
            status,
            completed_at,
        } => {
            let completed_at_str = format_timestamp(completed_at);

            with_immediate_transaction(connection, || {
                refresh_session_aggregates(connection, &session_id, completed_at)?;
                execute_cached(connection,
                    "UPDATE session_metrics SET status = ?1, completed_at = ?2, updated_at = ?2 WHERE session_id = ?3",
                    params![status.as_str(), completed_at_str, session_id],
                )?;
                Ok(())
            })
        }
        MetricsMutation::RoundStarted {
            round_id,
            session_id,
            model,
            started_at,
        } => {
            let started_at_str = format_timestamp(started_at);

            with_immediate_transaction(connection, || {
                execute_cached(
                    connection,
                    r#"
                    INSERT INTO round_metrics (
                        round_id, session_id, model, started_at, status
                    ) VALUES (?1, ?2, ?3, ?4, 'running')
                    ON CONFLICT(round_id) DO NOTHING
                    "#,
                    params![round_id, session_id, model, started_at_str],
                )?;
                refresh_session_aggregates(connection, &session_id, started_at)?;
                Ok(())
            })
        }
        MetricsMutation::RoundCompleted {
            round_id,
            completed_at,
            status,
            usage,
            prompt_cached_tool_outputs,
            prompt_cached_tool_tokens_saved,
            error,
        } => {
            let completed_at_str = format_timestamp(completed_at);
            // SQLite INTEGER is signed 64-bit. Normalize before conversion so
            // extreme provider values saturate instead of wrapping negative.
            let usage = usage.clamped_for_durable_metrics();
            let prompt_tokens = durable_token_to_i64(usage.prompt_tokens);
            let completion_tokens = durable_token_to_i64(usage.completion_tokens);
            let total_tokens = durable_token_to_i64(usage.total_tokens);

            with_immediate_transaction(connection, || {
                #[cfg(test)]
                signal_complete_round_transaction_entered(&round_id);

                let session_id: String = query_row_cached(
                    connection,
                    "SELECT session_id FROM round_metrics WHERE round_id = ?1",
                    params![round_id],
                    |row| row.get(0),
                )?;

                execute_cached(
                    connection,
                    r#"
                    UPDATE round_metrics
                    SET completed_at = ?1,
                        status = ?2,
                        prompt_tokens = ?3,
                        completion_tokens = ?4,
                        total_tokens = ?5,
                        prompt_cached_tool_outputs = ?6,
                        prompt_cached_tool_tokens_saved = ?7,
                        -- `RoundCompleted` may be replayed. Replace its prompt-
                        -- cache contribution while preserving tokens recorded by
                        -- separate compression events, rather than adding the
                        -- same completion payload again.
                        tokens_saved = MAX(
                            COALESCE(tokens_saved, 0) - COALESCE(prompt_cached_tool_tokens_saved, 0),
                            0
                        ) + ?8,
                        error = ?9
                    WHERE round_id = ?10
                    "#,
                    params![
                        completed_at_str,
                        status.as_str(),
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                        i64::from(prompt_cached_tool_outputs),
                        i64::from(prompt_cached_tool_tokens_saved),
                        i64::from(prompt_cached_tool_tokens_saved),
                        error,
                        round_id,
                    ],
                )?;

                #[cfg(test)]
                batch_probe::after_round_update(connection, &round_id);
                refresh_session_aggregates(connection, &session_id, completed_at)?;
                Ok(())
            })
        }
        MetricsMutation::ToolStarted {
            tool_call_id,
            round_id,
            session_id,
            tool_name,
            started_at,
        } => {
            let started_at_str = format_timestamp(started_at);

            execute_cached(
                connection,
                r#"
                INSERT INTO tool_call_metrics (
                    tool_call_id, round_id, session_id, tool_name, started_at
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(tool_call_id) DO UPDATE SET
                    round_id = excluded.round_id,
                    session_id = excluded.session_id,
                    tool_name = excluded.tool_name,
                    started_at = excluded.started_at
                "#,
                params![
                    tool_call_id,
                    round_id,
                    session_id,
                    tool_name,
                    started_at_str
                ],
            )?;
            Ok(())
        }
        MetricsMutation::ToolCompleted {
            tool_call_id,
            completion,
        } => {
            let completed_at = format_timestamp(completion.completed_at);
            let success = if completion.success { 1_i64 } else { 0_i64 };
            let error = completion.error;

            with_immediate_transaction(connection, || {
                let session_id: String = query_row_cached(
                    connection,
                    "SELECT session_id FROM tool_call_metrics WHERE tool_call_id = ?1",
                    params![tool_call_id],
                    |row| row.get(0),
                )?;

                execute_cached(connection,
                    "UPDATE tool_call_metrics SET completed_at = ?1, success = ?2, error = ?3 WHERE tool_call_id = ?4",
                    params![completed_at, success, error, tool_call_id],
                )?;

                refresh_session_aggregates(connection, &session_id, completion.completed_at)?;
                Ok(())
            })
        }
        MetricsMutation::ExecuteSyncMismatch {
            reason,
            occurred_at,
        } => {
            let mismatch_date = occurred_at.date_naive().to_string();
            let updated_at = format_timestamp(occurred_at);

            execute_cached(
                connection,
                r#"
                INSERT INTO execute_sync_mismatch_metrics (reason, mismatch_date, count, updated_at)
                VALUES (?1, ?2, 1, ?3)
                ON CONFLICT(reason, mismatch_date) DO UPDATE SET
                    count = count + 1,
                    updated_at = excluded.updated_at
                "#,
                params![reason, mismatch_date, updated_at],
            )?;
            Ok(())
        }
        MetricsMutation::ForwardStarted {
            forward_id,
            endpoint,
            model,
            is_stream,
            started_at,
        } => {
            let is_stream_int = if is_stream { 1_i64 } else { 0_i64 };
            let started_at_str = format_timestamp(started_at);

            execute_cached(
                connection,
                r#"
                INSERT INTO forward_request_metrics (
                    forward_id, endpoint, model, is_stream, started_at, status, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?5)
                ON CONFLICT(forward_id) DO UPDATE SET
                    endpoint = excluded.endpoint,
                    model = excluded.model,
                    is_stream = excluded.is_stream,
                    started_at = excluded.started_at,
                    completed_at = NULL,
                    status_code = NULL,
                    status = 'pending',
                    prompt_tokens = NULL,
                    completion_tokens = NULL,
                    total_tokens = NULL,
                    cache_creation_input_tokens = NULL,
                    cache_read_input_tokens = NULL,
                    cache_write_input_tokens = NULL,
                    reasoning_output_tokens = NULL,
                    error = NULL,
                    updated_at = excluded.updated_at
                "#,
                params![forward_id, endpoint, model, is_stream_int, started_at_str],
            )?;
            Ok(())
        }
        MetricsMutation::ForwardCompleted {
            forward_id,
            completed_at,
            status_code,
            status,
            usage,
            token_details,
            error,
        } => {
            let completed_at_str = format_timestamp(completed_at);
            let status_code_int = status_code.map(|s| s as i64);
            let (prompt, completion, total) = match usage {
                Some(u) => (
                    Some(u.prompt_tokens as i64),
                    Some(u.completion_tokens as i64),
                    Some(u.total_tokens as i64),
                ),
                None => (None, None, None),
            };
            let token_details = token_details.unwrap_or_default();
            let cache_creation = token_details
                .cache_creation_input_tokens
                .map(|value| value as i64);
            let cache_read = token_details
                .cache_read_input_tokens
                .map(|value| value as i64);
            let cache_write = token_details
                .cache_write_input_tokens
                .map(|value| value as i64);
            let reasoning_output = token_details
                .reasoning_output_tokens
                .map(|value| value as i64);

            execute_cached(
                connection,
                r#"
                UPDATE forward_request_metrics
                SET completed_at = ?1,
                    status_code = ?2,
                    status = ?3,
                    prompt_tokens = ?4,
                    completion_tokens = ?5,
                    total_tokens = ?6,
                    cache_creation_input_tokens = ?7,
                    cache_read_input_tokens = ?8,
                    cache_write_input_tokens = ?9,
                    reasoning_output_tokens = ?10,
                    error = ?11,
                    updated_at = ?1
                WHERE forward_id = ?12
                "#,
                params![
                    completed_at_str,
                    status_code_int,
                    status.as_str(),
                    prompt,
                    completion,
                    total,
                    cache_creation,
                    cache_read,
                    cache_write,
                    reasoning_output,
                    error,
                    forward_id,
                ],
            )?;
            Ok(())
        }
    };
    #[cfg(test)]
    if result.is_ok() {
        batch_probe::after_item(connection);
    }
    result
}
