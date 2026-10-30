import { setTimeout as delay } from 'node:timers/promises'

const fields = ['session_id', 'created_at', 'kind', 'root_session_id', 'parent_session_id',
  'project_id', 'authority_sha256', 'ancestry_sha256', 'metadata_version']

/** An uncertain write is never promoted to an acknowledgement by the harness. */
export function clearOutcome(state) {
  if (!state || state.busy) return 'waiting'
  if (state.error === 'unknown') return 'reject'
  if (state.confirmed) return 'confirmed'
  if (state.error === 'readback_failed') return 'refresh'
  return 'reject'
}

/** Wait for the UI store to apply the actual Host observation, not just receive HTTP. */
export async function waitForManagedBinding(read, expected, { timeoutMs = 15000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs
  do {
    const binding = await read()
    const target = binding?.selection?.target
    if (binding?.status === 'active' && target && fields.every(field => target[field] === expected[field])) return binding
    if (Date.now() >= deadline) break
    await delay(intervalMs)
  } while (true)
  throw new Error('The UI did not apply the expected managed binding before the deadline')
}
