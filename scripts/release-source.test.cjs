const assert = require('node:assert/strict')
const { test } = require('node:test')
const { validateDispatch, POLICY_PATHS, REQUIRED } = require('./release-source.cjs')
const { validateSource } = require('./crate-release.cjs')
const sha = 'a'.repeat(40), tree = 'b'.repeat(40)
function fixture() {
  const event = { inputs: { complete_release: 'true', expected_source_sha: sha, accepted_ci_run: '123' } }
  const validation = { kind: 'bamboo-release-ci-reuse/v1', sourceRevision: sha, sourceTree: tree,
    validatedRevision: 'c'.repeat(40), validatedTree: tree, requiredChecks: [...REQUIRED],
    policyOnlyPaths: [...POLICY_PATHS], productTreeVerified: true,
    ciRun: { id: 123, name: 'CI', path: '.github/workflows/ci.yml', status: 'completed', conclusion: 'success',
      event: 'pull_request', head_sha: 'd'.repeat(40), head_branch: `release/${'e'.repeat(40)}`,
      repository: { full_name: 'bigduu/Bamboo-agent' }, head_repository: { full_name: 'bigduu/Bamboo-agent' },
      html_url: 'https://github.com/bigduu/Bamboo-agent/actions/runs/123' } }
  return { event, validation }
}
test('explicit complete release consumes actual successful CI and records its distinct source', () => {
  const { event, validation } = fixture()
  validation.validatedTree = 'f'.repeat(40)
  assert.equal(validateDispatch(event, validation, sha, tree), validation.ciRun)
  assert.equal(validateSource({ eventName: 'workflow_dispatch', event, sourceRevision: sha,
    sourceTree: tree, repository: 'bigduu/Bamboo-agent', validation }), true)
})
test('cancelled, failed, foreign, mismatched or incomplete CI cannot authorize publication', () => {
  for (const mutate of [
    x => { x.validation.ciRun.conclusion = 'cancelled' },
    x => { x.validation.ciRun.conclusion = 'failure' },
    x => { x.validation.ciRun.status = 'in_progress' },
    x => { x.validation.ciRun.repository.full_name = 'other/repository' },
    x => { x.validation.ciRun.head_repository.full_name = 'other/repository' },
    x => { x.validation.ciRun.path = '.github/workflows/other.yml' },
    x => { x.validation.ciRun.event = 'workflow_dispatch' },
    x => { x.validation.ciRun.head_branch = 'dev' },
    x => { x.validation.ciRun.html_url = 'https://example.invalid/123' },
    x => { x.event.inputs.accepted_ci_run = '124' },
    x => { x.event.inputs.complete_release = 'false' },
    x => { x.event.inputs.expected_source_sha = 'f'.repeat(40) },
    x => { x.validation.requiredChecks.pop() },
    x => { x.validation.productTreeVerified = false },
    x => { x.validation.sourceTree = 'f'.repeat(40) },
  ]) {
    const x = fixture(); mutate(x)
    assert.throws(() => validateDispatch(x.event, x.validation, sha, tree))
  }
})
test('earlier CI cannot cover runtime, lockfile, Rust manifest or renamed-source changes', () => {
  for (const name of ['Cargo.toml', 'Cargo.lock', 'src/main.rs', 'crates/app/bamboo-server/src/lib.rs',
    'scripts/../src/main.rs', 'scripts/sidecar-build.cjs']) {
    const { event, validation } = fixture(); validation.policyOnlyPaths = [name]
    assert.throws(() => validateDispatch(event, validation, sha, tree))
  }
  const { event, validation } = fixture(); validation.policyOnlyPaths = []
  validation.validatedTree = 'f'.repeat(40)
  assert.throws(() => validateDispatch(event, validation, sha, tree))
})
