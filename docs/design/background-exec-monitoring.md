# Quiet exec commands and background monitoring

An ordinary `exec_command` remains pull based: it yields an owner-scoped handle,
and `write_stdin` retrieves incremental output and the real terminal result.
When repeated empty polling reaches the existing trusted quiet-poll boundary in
a Human-controlled Root, a capable Host can hand that exact command to background
monitoring before spending another diagnostic model request.

The runner reserves the existing capture, then commits an ordinary assistant
handoff notice and the existing `WaitingForBashState` together. The suspension
keeps the normal composer recovery and Human steering path available. It creates
no pending question, second job store, process, permission grant, or command retry.
If durable persistence or a capable completion hook is unavailable, the current
finite diagnostic policy still applies.

The monitor waits for the same drained terminal capture as `write_stdin` without
provider requests. It takes a bounded snapshot without advancing the pull cursor.
Completion is admitted to SessionInbox as `background_exec_completed` before the
corresponding wait is cleared. SessionMessenger delivers it at the next safe
boundary or reserves a successor execution. Clearing a completed exec wait uses
the latest locked Session so an active Human conversation keeps its messages and
runtime status. Exact retries use the same completion identity. Normal exec
completion never invokes legacy Bash PostToolUse hooks a second time.

The existing shell-wait backstop covers completion during suspension finalization
and re-arms after Host restart. It checks the captured handles rather than the
legacy Bash-only registry listing. Losing a live handle or reaching the wait
ceiling is an unknown outcome; the recovery notice requests a real status check
with `write_stdin` and does not authorize replaying the command. Host restart does
not reconstruct an OS process. Unread terminal output retains its existing
five-minute expiry; the completion envelope remains in the durable inbox.
