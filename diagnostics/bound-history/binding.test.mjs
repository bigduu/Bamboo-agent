import assert from 'node:assert/strict'
import { test } from 'node:test'
import { clearOutcome, waitForManagedBinding } from './binding.mjs'

const target = { session_id: 'synthetic', created_at: 'birth', kind: 'root', root_session_id: 'synthetic',
  parent_session_id: null, project_id: null, authority_sha256: 'authority', ancestry_sha256: 'ancestry', metadata_version: 2 }
const binding = (patch = {}, status = 'active') => ({ status, selection: { target: { ...target, ...patch } } })

test('waits until the new observation is applied, not merely until the old session is selected', async () => {
  let reads = 0
  const applied = await waitForManagedBinding(async () => ++reads < 3 ? binding({ metadata_version: 1 }) : binding(),
    target, { timeoutMs: 100, intervalMs: 1 })
  assert.equal(reads, 3)
  assert.equal(applied.selection.target.metadata_version, 2)
})
test('does not accept a matching revision for a different identity', async () => {
  await assert.rejects(waitForManagedBinding(async () => binding({ authority_sha256: 'replacement' }), target,
    { timeoutMs: 10, intervalMs: 1 }), /did not apply/)
})
test('does not accept a stale binding even if its target fields match', async () => {
  await assert.rejects(waitForManagedBinding(async () => binding({}, 'stale'), target,
    { timeoutMs: 10, intervalMs: 1 }), /did not apply/)
})
test('only an acknowledged readback failure permits a read-only refresh', () => {
  assert.equal(clearOutcome({ busy: false, error: 'readback_failed' }), 'refresh')
  assert.equal(clearOutcome({ busy: false, error: 'unknown', readbackConfirmed: true }), 'reject')
  assert.equal(clearOutcome({ busy: false, confirmed: true }), 'confirmed')
  assert.equal(clearOutcome({ busy: true }), 'waiting')
})
