const assert = require('node:assert/strict')

// The owner-requested release dispatch reuses a real completed CI snapshot.
// Changes after that snapshot are restricted to this publication controller.
const POLICY_PATHS = Object.freeze([
  '.github/workflows/ci.yml', '.github/workflows/publish-crate.yml',
  'scripts/ci-policy.test.cjs', 'scripts/crate-release.cjs', 'scripts/crate-release.test.cjs',
  'scripts/sidecar-release.cjs', 'scripts/sidecar-release.test.cjs',
  'scripts/release-source.cjs', 'scripts/release-source.test.cjs',
])
const REQUIRED = Object.freeze(['Validation Source', 'Promotion Source', 'Test', 'E2E Tests', 'Lint',
  'Build (ubuntu-latest)', 'Build (macos-latest)', 'Build (windows-latest)'])
const SHA = /^[0-9a-f]{40}$/

function validateDispatch(event, validation, sourceRevision, sourceTree) {
  assert.ok(event.inputs?.complete_release === 'true' || event.inputs?.complete_release === true)
  assert.equal(event.inputs.expected_source_sha, sourceRevision)
  assert.match(sourceRevision, SHA)
  assert.match(sourceTree, SHA)
  assert.equal(validation?.kind, 'bamboo-release-ci-reuse/v1')
  assert.equal(validation.sourceRevision, sourceRevision)
  assert.equal(validation.sourceTree, sourceTree)
  const run = validation.ciRun
  assert.equal(String(run?.id), event.inputs.accepted_ci_run)
  assert.equal(run.name, 'CI')
  assert.equal(run.path, '.github/workflows/ci.yml')
  assert.equal(run.status, 'completed')
  assert.equal(run.conclusion, 'success')
  assert.equal(run.event, 'pull_request')
  assert.equal(run.repository?.full_name, 'bigduu/Bamboo-agent')
  assert.equal(run.head_repository?.full_name, 'bigduu/Bamboo-agent')
  assert.match(run.head_branch, /^release\/[0-9a-f]{40}$/)
  assert.match(run.head_sha, SHA)
  assert.equal(run.html_url, `https://github.com/bigduu/Bamboo-agent/actions/runs/${run.id}`)
  assert.match(validation.validatedRevision, SHA)
  assert.match(validation.validatedTree, SHA)
  assert.deepEqual(validation.requiredChecks, REQUIRED)
  assert.ok(Array.isArray(validation.policyOnlyPaths))
  assert.equal(new Set(validation.policyOnlyPaths).size, validation.policyOnlyPaths.length)
  assert.ok(validation.policyOnlyPaths.every(name => POLICY_PATHS.includes(name)),
    'Runtime changes cannot reuse earlier CI')
  if (!validation.policyOnlyPaths.length) assert.equal(sourceTree, validation.validatedTree)
  // Distinct HEAD-derived metadata is recorded, never claimed byte-identical.
  assert.equal(validation.productTreeVerified, true)
  return run
}

module.exports = { POLICY_PATHS, REQUIRED, validateDispatch }
