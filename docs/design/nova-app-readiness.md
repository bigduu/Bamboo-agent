# Nova App setup and observed readiness

`GET /api/v1/plugins/nova/app` retains the verified setup contract from #1919.
It additionally reports `start.available/reason` and `observation`:

- `observed_at`: server UTC timestamp. A start request must use this observation
  within 60 seconds; future timestamps and changed/incompatible App versions fail.
- `app_runtime` and `permissions`: `not_verified`. The setup CLI checks files;
  this endpoint does not probe an App PID, request TCC, or perform desktop work.
- `mcp.server_id/state/tool_count`: a manager snapshot for the freshly rechecked
  package-owned desktop binding, or `not_observed` if no runtime observation exists.
  `configuration_applied` remains `not_verified`: a saved desired row or an older
  client connection is not proof that the current configuration was adopted.

`desktop_readiness` remains `not_tested`. MCP readiness is transport evidence,
and never certifies desktop permissions or the usability of an operation.

`POST /api/v1/plugins/nova/app/start` is a separate explicit native action behind
the existing authenticated plugin route. Its exact body is:

```json
{"observed_at":"2026-10-15T02:00:00Z","app_version":"0.3.1"}
```

The timestamp/version are optimistic stale-observation conditions, not new
authorization. The host independently revalidates its reviewed owned CLI,
publisher policy, actual App status, compatible version and finite Nova.app path
under the existing plugin operation lock. No command, URL, path, install mode or
permission flag is accepted from the request. Only the fixed native macOS adapter
can open that producer-verified App. No manifest gains postinstall execution.

On success, HTTP 202 returns `action: start_requested` and a fresh `setup` report.
It does not claim the App is running, grant permissions, enable Chrome, reconnect
MCP, create a data root or change configuration. The UI must show the separate
manual permission and explicit reconnect steps and refresh observation afterwards.
HTTP 409 uses finite reasons such as `observation_expired`, `app_changed`,
`app_version_mismatch`, `invalid_app_path`, `start_failed` or `start_timeout`.
Refresh status before retrying; preserve the failed operation's result.

Queued cancellation performs no action. Once an explicit action starts, its
bounded operation and fresh status retain the existing ownership/cleanup lock
even if the client disconnects. Existing Apps, data and configuration are not
replaced. Installing, upgrading and adopting existing Apps remain their own
contracts; this slice reuses #1919 and does not add an updater or service manager.
