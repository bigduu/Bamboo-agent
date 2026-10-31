const assert = require('node:assert/strict')
const { test } = require('node:test')
const { spawnSync } = require('node:child_process')
const { validateDispatch, REQUIRED } = require('./release-source.cjs')
const { validateSource } = require('./crate-release.cjs')
// Retained workflow_run compatibility, not the active frozen-Dev publisher.
// The hash-checked fixture is the complete raw f0eb67c3 publisher. Its loader
// bytes also match original test source116c1ec4; preserve the v2/v4 cases below.
const historicalWorkflow = require('./tests/main-authority-bootstrap-fixture.cjs').acceptedDevData().publisher
const sha = 'a'.repeat(40), tree = 'b'.repeat(40)
function fixture() {
  const event = { inputs: { complete_release: 'true', expected_source_sha: sha, accepted_ci_run: '123' } }
  const validation = { kind: 'bamboo-release-ci-reuse/v1', sourceRevision: sha, sourceTree: tree,
    validatedRevision: 'c'.repeat(40), validatedTree: tree, requiredChecks: [...REQUIRED],
    policyOnlyPaths: [], productTreeVerified: true,
    ciRun: { id: 123, name: 'CI', path: '.github/workflows/ci.yml', status: 'completed', conclusion: 'success',
      event: 'pull_request', head_sha: sha, head_branch: `release/${'e'.repeat(40)}`,
      repository: { full_name: 'bigduu/Bamboo-agent' }, head_repository: { full_name: 'bigduu/Bamboo-agent' },
      html_url: 'https://github.com/bigduu/Bamboo-agent/actions/runs/123' } }
  validation.mainPromotion = {
    mainProtected: true, sourceInMain: true, reviewedBeforeMerge: true,
    mergeRevision: '1'.repeat(40), mergeTree: tree, mergeParents: ['8'.repeat(40), sha],
    pullRequest: { number: 7, html_url: 'https://github.com/bigduu/Bamboo-agent/pull/7', state: 'closed',
      merged: true, draft: false, baseRef: 'main', headRef: validation.ciRun.head_branch,
      headRevision: sha, mergedAt: '2026-10-19T00:00:00Z' },
    reviewSummary: { url: 'https://github.com/bigduu/Bamboo-agent/pull/7#issuecomment-99',
      actor: { type: 'Bot', login: 'chatgpt-codex-connector[bot]' },
      state: Object.fromEntries(['code_review', 'security_review'].map(kind => [kind,
        { commit_sha: sha, status: 'completed', has_any_findings: false, has_unresolved_findings: false }])) },
  }
  return { event, validation }
}
test('historical dispatch validation is not authority to start a new publication', () => {
  const { event, validation } = fixture()
  assert.equal(validateDispatch(event, validation, sha, tree), validation.ciRun)
  assert.throws(() => validateSource({ eventName: 'workflow_dispatch', event, sourceRevision: sha,
    sourceTree: tree, repository: 'bigduu/Bamboo-agent', validation }), /Unsupported release trigger/)
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
    x => { x.validation.validatedTree = 'f'.repeat(40) },
    x => { x.validation.ciRun.head_sha = 'f'.repeat(40) },
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

function automaticBootstrap() {
  const workflow = historicalWorkflow
  const step = workflow.split('      - name: Authorize the exact source before checkout\n')[1].split('\n      - ')[0]
  const match = step.match(/python3 - <<'([A-Z]+)'\n([\s\S]*?)\n          \1/)
  assert.ok(match, 'Select the named authority step, never an unrelated later Python version step')
  return match[2].split('\n').map(line => line.replace(/^ {10}/, '')).join('\n')
}
async function runActualPreflight(t, scenario) {
  return require('./tests/main-authority-bootstrap-fixture.cjs').verify(t, automaticBootstrap(), 'v4', 'publication', {
    exercise: ({run, base}) => {
      const environment = {}, change = input => {
        const delivered = input.event.workflow_run, server = input.routes['actions/runs/111']
        const set = (key, value) => { delivered[key] = value; server[key] = value }
        if (scenario.startsWith('manual')) {
          input.event = {sender: {login: 'bigduu'}, inputs: {complete_release: 'true', dry_run: 'false', expected_source_sha: 'e'.repeat(40), accepted_ci_run: '123'}}
          environment.GITHUB_EVENT_NAME = 'workflow_dispatch'
        } else if (scenario === 'failed' || scenario === 'cancelled') set('conclusion', scenario === 'failed' ? 'failure' : 'cancelled')
        else if (scenario === 'release_pr') set('event', 'pull_request')
        else if (scenario === 'non_main') set('head_branch', 'dev')
        else if (scenario === 'other_workflow') set('name', 'Unrelated CI')
        else if (scenario === 'foreign_repository') { delivered.repository.full_name = 'other/repository'; server.repository.full_name = 'other/repository' }
        else if (scenario === 'fork') { delivered.head_repository.full_name = 'other/repository'; server.head_repository.full_name = 'other/repository' }
        else if (scenario === 'unprotected_main') input.routes['branches/main'].protected = false
        else if (scenario === 'unprotected_dev') input.routes['branches/dev'].protected = false
        else if (scenario === 'untrusted_ref') environment.GITHUB_REF = 'refs/heads/topic'
        else if (scenario === 'unprotected_workflow_ref') environment.GITHUB_REF_PROTECTED = 'false'
        else if (scenario === 'unrelated_source' || scenario === 'unrelated_controller') input.routes['branches/dev'].commit.sha = base
        else throw new Error('Unknown authority fixture scenario: ' + scenario)
      }
      return run(change, environment)
    },
  })
}

test('historical complete automatic publication has one route and never accepts the experimental manual authority', () => {
  const workflow = historicalWorkflow
  const automatic = workflow.split('  automatic:\n')[1]
  const condition = automatic.match(/^    if: >-\n([\s\S]*?)\n    runs-on:/m)[1]
  assert.match(condition, /github\.event_name == 'workflow_run'/)
  assert.doesNotMatch(condition, /workflow_dispatch|inputs\./)
  assert.doesNotMatch(workflow.split('\npermissions:')[0], /complete_release|accepted_ci_run/)
  assert.doesNotMatch(workflow, /accepted_ci_run/)
  assert.doesNotMatch(workflow.split('\npermissions:')[0], /SOURCE_VALIDATION/)
  assert.doesNotMatch(workflow, /SOURCE_VALIDATION: \$\{\{ (?:inputs|github\.event\.inputs)\./)
  assert.match(workflow, /SOURCE_VALIDATION: \$\{\{ needs\.automatic\.outputs\.source_validation \}\}/)
  assert.ok(workflow.includes("if: github.event_name == 'workflow_dispatch' && !inputs.dry_run && !fromJSON(toJSON(github.event.inputs)).complete_release"))
  assert.ok(workflow.includes("if: github.event_name == 'workflow_dispatch' && inputs.dry_run && !fromJSON(toJSON(github.event.inputs)).complete_release"))
  // Existing guarded crate-only manual/dry-run modes do not call the complete publisher.
  assert.match(workflow.split('  automatic:\n')[0], /  manual:\n/)
  assert.match(workflow.split('  automatic:\n')[0], /  dry-run:\n/)
})

test('historical automatic preflight selects the successful main merge, never the separate release head', async (t) => {
  const source = automaticBootstrap()
  assert.doesNotMatch(source, /GITHUB_ENV/, 'Source authorization must not write environment state')
  // Use the existing real Git merge/CI transport fixture for the historical dev
  // controller contract, including advanced dev and exact main output checks.
  await require('./tests/main-authority-bootstrap-fixture.cjs').verify(t, source, 'v2')
})

test('historical manual complete dispatch stops after the pinned loader, before CI proof or authority output', async t => {
  for (const scenario of ['manual_release_head', 'manual_changed_base']) {
    const result = await runActualPreflight(t, scenario)
    assert.notEqual(result.status, 0)
    assert.match(result.stderr, /AssertionError/)
    assert.match(result.calls, /^contents\/scripts\/main-release-admission\.py\?ref=[0-9a-f]{40}\n$/,
      'Only the pinned read-only verifier lookup may occur; no CI/release authority is queried')
    assert.equal(result.output, '')
  }
})

test('historical automatic preflight preserves CI, protected history and workflow-ref rejection boundaries', async t => {
  for (const scenario of ['failed', 'cancelled', 'release_pr', 'non_main', 'other_workflow',
    'foreign_repository', 'fork', 'unprotected_main', 'unprotected_dev', 'untrusted_ref',
    'unprotected_workflow_ref', 'unrelated_source', 'unrelated_controller']) {
    const result = await runActualPreflight(t, scenario)
    if (scenario === 'unprotected_main') {
      assert.equal(result.status, 0, result.stderr)
      assert.match(result.output, /^revision=/)
    } else {
      assert.notEqual(result.status, 0, `${scenario} must not authorize publication`)
      assert.match(result.stderr, /AssertionError/, result.stderr)
      assert.equal(result.output, '')
    }
  }
})

test('receipt validation rejects absent or inconsistent protected main acceptance evidence', () => {
  for (const mutate of [
    v => { delete v.mainPromotion },
    v => { v.mainPromotion.mainProtected = false },
    v => { v.mainPromotion.reviewedBeforeMerge = false },
    v => { v.mainPromotion.sourceInMain = false },
    v => { v.mainPromotion.mergeTree = '9'.repeat(40) },
    v => { v.mainPromotion.mergeParents = ['8'.repeat(40)] },
    v => { v.mainPromotion.pullRequest.merged = false },
    v => { v.mainPromotion.pullRequest.draft = true },
    v => { v.mainPromotion.pullRequest.baseRef = 'dev' },
    v => { v.mainPromotion.pullRequest.headRevision = '9'.repeat(40) },
    v => { v.mainPromotion.pullRequest.mergedAt = null },
    v => { v.mainPromotion.reviewSummary.actor.type = 'User' },
    v => { v.mainPromotion.reviewSummary.state.code_review.status = 'running' },
    v => { v.mainPromotion.reviewSummary.state.security_review.has_unresolved_findings = true },
    v => { v.mainPromotion.reviewSummary.state.code_review.commit_sha = '9'.repeat(40) },
  ]) {
    const { event, validation } = fixture()
    mutate(validation)
    assert.throws(() => validateDispatch(event, validation, sha, tree))
  }
})
