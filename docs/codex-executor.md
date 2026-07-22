# Codex executor

`subagents.executor = "codex"` runs one official Codex CLI process per child
activation through `codex exec --json`. Bamboo passes the assignment through
stdin, consumes bounded JSONL events, and captures the final answer separately
with `--output-last-message`.

The minimum supported version is **Codex CLI 0.144.0**. Bamboo verifies the
installed version and the required `exec --json`, stdin prompt,
`--output-last-message`, and `exec resume --json` surfaces before a worker
starts. The implementation and live Bamboo-provider test are verified against
0.144.5. Codex 0.144 removed the custom-provider Chat Completions wire, so
`codex_wire_api` only accepts `"responses"`.

## Authentication and billing modes

`codex_auth_mode` has four values. Unset defaults to `"bamboo"`; inheriting a
personal Codex login is deliberately opt-in.

| Mode | `CODEX_HOME` and credential | Provider and billing | Secret boundary |
|---|---|---|---|
| `inherit` | Leaves `CODEX_HOME` unset, so Codex uses the invoking user's `~/.codex/config.toml` and `auth.json`. | The user's configured provider; a ChatGPT login uses that user's subscription. | Inherits the user's full Codex configuration, so use only when that ambient authority is intended. |
| `api_key` | Uses `<child-state>/codex-home`; `OPENAI_API_KEY` must be named explicitly in `codex_forward_env`. | OpenAI API billing for that key. | Bamboo never forwards `OPENAI_API_KEY` implicitly. The key is process-environment only. |
| `custom` | Uses the isolated home and a generated `model_providers.custom` entry. The key is selected by `codex_provider_key_ref`. | The configured third-party/proxy provider and its billing policy. | `config.toml` contains only `env_key = "BAMBOO_CODEX_PROVIDER_KEY"`; the referenced key is injected into that environment variable and never written to disk. |
| `bamboo` | Uses the isolated home and a generated `model_providers.bamboo` entry pointing at the parent loopback `/openai/v1` surface. | The parent Bamboo provider/routing configuration; recommended default. | The parent mints a fresh `bcx1_` token for each activation, binds it to the child session and Responses/models paths, and revokes it on every exit path. No upstream provider key reaches Codex. |

Because `bamboo` deliberately targets the parent through `127.0.0.1`, it
requires local actor placement. A remote worker must use `custom` with an
explicit provider URL reachable from that worker; Bamboo rejects the ambiguous
loopback combination before issuing a run token.

### Examples

Recommended parent-routed mode (also the default when the mode is absent):

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "bamboo",
    "codex_model": "gpt-5.4"
  }
}
```

Use the logged-in user's subscription and personal Codex configuration:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "inherit"
  }
}
```

Isolated OpenAI API-key billing requires the explicit forwarding opt-in:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "api_key",
    "codex_forward_env": ["OPENAI_API_KEY"]
  }
}
```

Custom provider credentials use an existing Bamboo credential reference:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "custom",
    "codex_base_url": "https://provider.example/v1",
    "codex_wire_api": "responses",
    "codex_provider_key_ref": "provider.openai.api_key"
  }
}
```

`codex_base_url` must be an absolute HTTP(S) URL without embedded credentials,
query parameters, or a fragment. It and `codex_provider_key_ref` are valid only
in `custom` mode. `OPENAI_API_KEY` is valid in `codex_forward_env` only in
`api_key` mode. Unknown modes and wire protocols fail settings validation.

## Isolation and environment policy

Every child starts after `env_clear()`. Bamboo restores only `HOME`, `PATH`,
`SHELL`, `TERM`, `LANG`, `LC_*`, `TMPDIR`, `USER`, and `LOGNAME`, followed by
validated `codex_forward_env` names. `CODEX_*` variables and
`BAMBOO_CODEX_PROVIDER_KEY` cannot be supplied through that escape hatch, so a
parent/nested Codex sentinel cannot leak into the child and callers cannot
replace Bamboo's managed provider credential.

For `api_key`, `custom`, and `bamboo`, Bamboo creates
`<child-state>/codex-home` with mode `0700`, removes any stale `auth.json`, and
writes a minimal `config.toml` with mode `0600`. Generated provider config names
the environment variable but never includes a key or run token. The executor
also passes `--ignore-rules`; `inherit` intentionally does neither and leaves
`CODEX_HOME` unset.

The process owns its own process group. Cancellation sends SIGTERM to the
whole group, waits five seconds, then escalates to SIGKILL, so the npm launcher
and its native Codex child cannot be orphaned independently.

## Bamboo-as-provider token and observability

The parent actor runner mints a token at the activation boundary, not when a
warm worker is provisioned. The token travels only in `RunSpec.secrets`, whose
debug representation is redacted. A drop guard revokes it after success,
error, cancellation, dispatch failure, or retry exhaustion.

The HTTP gate recognizes `bcx1_` credentials before the ordinary loopback
bypass. A valid token may call only `/openai/v1/responses` and
`/openai/v1/models`; revoked, expired, or out-of-scope credentials return 401
even from `127.0.0.1`. The bound child session becomes the upstream
`LLMRequestOptions.session_id`, while the normal OpenAI-compat forward metrics
record the request and outcome in the parent.

## Verification

Run the deterministic executor and security tests normally with `cargo test`.
Two real-machine tests are ignored in routine CI because they require an
installed external binary:

```sh
# User-login/inherit smoke test
cargo test --test e2e_codex_cli_manual -- --ignored --nocapture

# Live Codex -> Bamboo Responses path, metrics, and post-run revocation
cargo test -p bamboo-server \
  live_bamboo_codex_completes_records_metrics_and_rejects_revoked_token \
  --lib -- --ignored --nocapture
```
