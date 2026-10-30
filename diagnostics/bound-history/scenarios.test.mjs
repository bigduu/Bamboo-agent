import assert from 'node:assert/strict'
import { test } from 'node:test'
import { runIndependentScenarios } from './scenarios.mjs'

test('a failed confirmation does not suppress the separate unknown-outcome scenario', async () => {
  const visited = []
  const failures = await runIndependentScenarios([['success', false], ['unknown', true]], async (name, value) => {
    visited.push([name, value])
    if (name === 'success') throw new Error('synthetic timeout')
  })
  assert.deepEqual(visited, [['success', false], ['unknown', true]])
  assert.equal(failures.length, 1)
  assert.equal(failures[0].label, 'success')
  assert.match(failures[0].error, /synthetic timeout/)
})
