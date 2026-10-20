const assert = require('node:assert/strict')
const { test } = require('node:test')
const fs = require('node:fs')
const { spawnSync } = require('node:child_process')
const { validateDispatch, REQUIRED } = require('./release-source.cjs')
const { validateSource } = require('./crate-release.cjs')
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

function runActualPreflight(scenario) {
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const source = workflow.split('  automatic:\n')[1].match(/python3 - <<'PY'\n([\s\S]*?)\n          PY/)[1]
  return spawnSync('python3', ['-B', '-c', String.raw`
import json, os, subprocess, sys, tempfile, textwrap
from pathlib import Path
request = json.load(sys.stdin)
scenario = request['scenario']
repo = 'bigduu/Bamboo-agent'
source, controller, tip, release_head = [c * 40 for c in 'adfe']
run = {'name': 'CI', 'event': 'push', 'conclusion': 'success', 'head_branch': 'main',
       'head_sha': source, 'repository': {'full_name': repo}, 'head_repository': {'full_name': repo}}
for name, key, value in [('failed', 'conclusion', 'failure'), ('cancelled', 'conclusion', 'cancelled'),
                         ('release_pr', 'event', 'pull_request'), ('non_main', 'head_branch', 'dev'),
                         ('other_workflow', 'name', 'Unrelated CI')]:
    if scenario == name: run[key] = value
if scenario == 'foreign_repository': run['repository']['full_name'] = 'other/repository'
if scenario == 'fork': run['head_repository']['full_name'] = 'other/repository'
api_calls = []
def transport(argv, *, text):
    assert text is True and argv[:2] == ['gh', 'api'] and len(argv) == 3
    route = argv[2].removeprefix('repos/' + repo + '/')
    api_calls.append(route)
    if route in ('branches/dev', 'branches/main'):
        name = route.split('/')[1]
        return json.dumps({'name': name, 'protected': scenario != 'unprotected_' + name,
                           'commit': {'sha': tip}})
    if route.startswith('compare/'):
        earlier, later = route.removeprefix('compare/').split('...')
        bad = (scenario == 'unrelated_source' and earlier == source) or (
               scenario == 'unrelated_controller' and earlier == controller)
        return json.dumps({'status': 'diverged' if bad else 'ahead',
                           'merge_base_commit': {'sha': '0' * 40 if bad else earlier}})
    raise RuntimeError('Unexpected API route: ' + route)
subprocess.check_output = transport
with tempfile.TemporaryDirectory(prefix='bamboo-main-publication-preflight-') as directory:
    root = Path(directory)
    event = {'workflow_run': run}
    event_name = 'workflow_run'
    if scenario.startswith('manual'):
        event_name = 'workflow_dispatch'
        event = {'sender': {'login': 'bigduu'}, 'inputs': {'complete_release': 'true', 'dry_run': 'false',
                 'expected_source_sha': release_head, 'accepted_ci_run': '123'}}
    (root / 'event').write_text(json.dumps(event))
    ref = 'refs/heads/main' if scenario != 'untrusted_ref' else 'refs/heads/topic'
    os.environ.update(GITHUB_REPOSITORY=repo, GITHUB_EVENT_NAME=event_name, GITHUB_ACTOR='bigduu',
        GITHUB_TRIGGERING_ACTOR='bigduu', GITHUB_EVENT_PATH=str(root / 'event'), GITHUB_WORKFLOW_SHA=controller,
        GITHUB_SHA=controller, GITHUB_REF=ref,
        GITHUB_REF_PROTECTED='false' if scenario == 'unprotected_workflow_ref' else 'true',
        GITHUB_WORKFLOW_REF=repo + '/.github/workflows/publish-crate.yml@' + ref,
        GITHUB_OUTPUT=str(root / 'output'), GITHUB_ENV=str(root / 'env'))
    try:
        exec(textwrap.dedent(request['source']), {})
    except BaseException:
        print(json.dumps({'apiCalls': api_calls, 'outputCreated': (root / 'output').exists()}))
        raise
    print(json.dumps({'output': (root / 'output').read_text(), 'envCreated': (root / 'env').exists(),
                      'apiCalls': api_calls}))
`], { input: JSON.stringify({ source, scenario }), encoding: 'utf8',
    env: { PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1' } })
}

test('complete automatic publication has one route and never accepts the experimental manual authority', () => {
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const automatic = workflow.split('  automatic:\n')[1]
  const condition = automatic.match(/^    if: >-\n([\s\S]*?)\n    runs-on:/m)[1]
  assert.match(condition, /github\.event_name == 'workflow_run'/)
  assert.doesNotMatch(condition, /workflow_dispatch|inputs\./)
  assert.doesNotMatch(workflow.split('\npermissions:')[0], /complete_release|accepted_ci_run/)
  assert.doesNotMatch(workflow, /accepted_ci_run|SOURCE_VALIDATION/)
  assert.ok(workflow.includes("if: github.event_name == 'workflow_dispatch' && !inputs.dry_run && !fromJSON(toJSON(github.event.inputs)).complete_release"))
  assert.ok(workflow.includes("if: github.event_name == 'workflow_dispatch' && inputs.dry_run && !fromJSON(toJSON(github.event.inputs)).complete_release"))
  // Existing guarded crate-only manual/dry-run modes do not call the complete publisher.
  assert.match(workflow.split('  automatic:\n')[0], /  manual:\n/)
  assert.match(workflow.split('  automatic:\n')[0], /  dry-run:\n/)
})

test('actual automatic preflight selects the successful main merge, never the separate release head', () => {
  const result = runActualPreflight('accepted_after_main_advances')
  assert.equal(result.status, 0, result.stderr)
  const emitted = JSON.parse(result.stdout.trim().split('\n').at(-1))
  assert.equal(emitted.output, `revision=${sha}\n`)
  assert.equal(emitted.envCreated, false)
  assert.ok(emitted.apiCalls.includes(`compare/${sha}...${'f'.repeat(40)}`))
  assert.ok(!emitted.output.includes('e'.repeat(40)), 'Do not create a second release-head publication identity')
})

test('actual manual complete dispatch fails before API or output even with owner and accepted-CI inputs', () => {
  for (const scenario of ['manual_release_head', 'manual_changed_base']) {
    const result = runActualPreflight(scenario)
    assert.notEqual(result.status, 0)
    assert.match(result.stderr, /AssertionError/)
    const emitted = JSON.parse(result.stdout.trim().split('\n').at(-1))
    assert.deepEqual(emitted.apiCalls, [], 'No promotion/review summary can become alternate publication authority')
    assert.equal(emitted.outputCreated, false)
  }
})

test('actual automatic preflight preserves CI, protected history and workflow-ref rejection boundaries', () => {
  for (const scenario of ['failed', 'cancelled', 'release_pr', 'non_main', 'other_workflow',
    'foreign_repository', 'fork', 'unprotected_main', 'unprotected_dev', 'untrusted_ref',
    'unprotected_workflow_ref', 'unrelated_source', 'unrelated_controller']) {
    const result = runActualPreflight(scenario)
    assert.notEqual(result.status, 0, `${scenario} must not authorize publication`)
    assert.match(result.stderr, /AssertionError/, result.stderr)
    assert.equal(JSON.parse(result.stdout.trim().split('\n').at(-1)).outputCreated, false)
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
