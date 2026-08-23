# Real russh transport fixture

The repository-level runner starts this fixture and executes the ignored
`russh_live` integration test:

```sh
scripts/run-russh-live.sh
```

The runner requires a working Docker daemon, `ssh-keygen`, and Cargo. It creates
an ephemeral Ed25519 client key, builds the digest-pinned Alpine fixture, binds
the SSH port to a random loopback port, waits for the container health check,
and always removes the container, image tag, and key directory on exit.

The container generates a fresh Ed25519 host key at startup. Only public-key
authentication for the unprivileged `deploy` user is enabled; password, root,
agent, X11, and local forwarding are disabled. Remote forwarding and
`internal-sftp` remain enabled because they are the production contracts under
test. No repository secret or external SSH service is used.

For an already-running SSH server, invoke the ignored test directly with either
`RUSSH_KEY_PATH` (recommended) or `RUSSH_PASS`:

```sh
RUSSH_HOST=127.0.0.1 \
RUSSH_PORT=2222 \
RUSSH_USER=deploy \
RUSSH_KEY_PATH=/path/to/test_ed25519 \
cargo test --locked -p bamboo-broker --test russh_live \
  russh_deploys_through_reverse_tunnel -- --exact --ignored --nocapture
```

The Rust test has a 60-second contract timeout. The protected Linux `Test` CI
job additionally bounds the complete fixture build and execution to ten
minutes, so startup or cleanup regressions fail closed instead of silently
skipping.
