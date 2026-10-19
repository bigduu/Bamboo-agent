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
    mainProtected: true, sourceInMain: true,
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
test('explicit complete release consumes actual successful CI and records its distinct source', () => {
  const { event, validation } = fixture()
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
  const automatic = workflow.split('  automatic:\n')[1]
  const source = automatic.match(/python3 - <<'PY'\n([\s\S]*?)\n          PY/)[1]
  return spawnSync('python3', ['-B', '-c', String.raw`
import json, os, re, subprocess, sys, tempfile, textwrap
from pathlib import Path
request = json.load(sys.stdin)
scenario = request['scenario']
source, tree, synthetic, controller, cut, dev, merge = [c * 40 for c in 'abcdef1']
if scenario == 'accepted_later_main':
    controller = '9' * 40
repo = 'bigduu/Bamboo-agent'
branch = 'release/' + cut
pr = {'number': 7, 'html_url': 'https://github.com/' + repo + '/pull/7', 'state': 'closed',
      'merged': True, 'draft': False, 'merged_at': '2026-10-19T00:00:00Z', 'merge_commit_sha': merge,
      'head': {'sha': source, 'ref': branch, 'repo': {'full_name': repo}},
      'base': {'ref': 'main', 'repo': {'full_name': repo}}}
if scenario in ('unmerged', 'unmerged_but_on_main'):
    pr.update(state='open', merged=False, merged_at=None)
if scenario == 'wrong_promotion_head':
    pr['head']['sha'] = '9' * 40
if scenario == 'wrong_promotion_base':
    pr['base']['ref'] = 'dev'
reviews = {k: {'commit_sha': source, 'status': 'completed', 'has_any_findings': False,
               'has_unresolved_findings': False} for k in ('code_review', 'security_review')}
if scenario == 'review_pending':
    reviews['code_review']['status'] = 'running'
if scenario == 'review_unresolved':
    reviews['security_review']['has_unresolved_findings'] = True
required = ['Validation Source', 'Promotion Source', 'Test', 'E2E Tests', 'Lint',
            'Build (ubuntu-latest)', 'Build (macos-latest)', 'Build (windows-latest)']
def api(route):
    if route == 'branches/main':
        return {'protected': scenario != 'unprotected_main', 'commit': {'sha': controller}}
    if route == 'branches/dev':
        return {'protected': True, 'commit': {'sha': dev}}
    if route == 'actions/runs/123':
        return {'id': 123, 'name': 'CI', 'path': '.github/workflows/ci.yml', 'status': 'completed',
                'conclusion': 'success', 'event': 'pull_request', 'head_sha': source, 'head_branch': branch,
                'repository': {'full_name': repo}, 'head_repository': {'full_name': repo},
                'html_url': 'https://github.com/' + repo + '/actions/runs/123'}
    if route == 'git/ref/heads/' + branch:
        return {'object': {'type': 'commit', 'sha': source}}
    if route.startswith('compare/'):
        earlier, later = route.removeprefix('compare/').split('...')
        bad = (scenario == 'unmerged' and (earlier, later) == (source, controller)) or (
            scenario == 'merge_not_on_main' and (earlier, later) == (merge, controller))
        return {'status': 'diverged' if bad else ('identical' if earlier == later else 'ahead'),
                'merge_base_commit': {'sha': '0' * 40 if bad else earlier}}
    if route == 'actions/runs/123/jobs?per_page=100':
        return {'jobs': [{'id': 42 + i, 'name': name, 'status': 'completed', 'conclusion': 'success',
                 'steps': [{'name': 'Require the exact clean source after full validation', 'conclusion': 'success'}]}
                 for i, name in enumerate(required)]}
    if route.startswith('git/commits/'):
        revision = route.removeprefix('git/commits/')
        parents = [{'sha': '8' * 40}, {'sha': source}]
        if scenario == 'not_a_normal_merge' and revision == merge:
            parents = [{'sha': '8' * 40}]
        return {'sha': revision, 'tree': {'sha': '9' * 40 if scenario == 'changed_merge_tree' and revision == merge else tree},
                'parents': parents}
    if route.startswith('pulls?state=closed&base=main&head=bigduu:'):
        return [pr]
    if route == 'pulls/7':
        return pr
    if route == 'issues/7/comments?per_page=100':
        return [{'id': 99, 'html_url': 'https://github.com/' + repo + '/pull/7#issuecomment-99',
                 'user': {'login': 'bigduu' if scenario == 'forged_review' else 'chatgpt-codex-connector[bot]',
                          'type': 'User' if scenario == 'forged_review' else 'Bot'},
                 'body': '<!-- codex-review-state:v1 ' + json.dumps(reviews) + ' -->'}]
    raise RuntimeError('Unexpected fixture API route: ' + route)
def transport(argv, *, text):
    assert text is True and argv[:2] == ['gh', 'api'] and len(argv) == 3
    prefix = 'repos/' + repo + '/'
    assert argv[2].startswith(prefix)
    route = argv[2].removeprefix(prefix)
    if re.fullmatch(r'actions/jobs/[0-9]+/logs', route):
        return 'Source: ' + synthetic + ' tree: ' + tree + '\n'
    return json.dumps(api(route))
subprocess.check_output = transport
with tempfile.TemporaryDirectory(prefix='bamboo-release-main-preflight-') as directory:
    root = Path(directory)
    event = {'sender': {'login': 'bigduu'}, 'inputs': {'complete_release': 'true', 'dry_run': 'false',
             'expected_source_sha': source, 'accepted_ci_run': '123'}}
    (root / 'event').write_text(json.dumps(event))
    os.environ.update(GITHUB_REPOSITORY=repo, GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_ACTOR='bigduu',
        GITHUB_TRIGGERING_ACTOR='bigduu', GITHUB_EVENT_PATH=str(root / 'event'), GITHUB_WORKFLOW_SHA=controller,
        GITHUB_SHA=controller, GITHUB_REF='refs/heads/main',
        GITHUB_WORKFLOW_REF=repo + '/.github/workflows/publish-crate.yml@refs/heads/main',
        GITHUB_OUTPUT=str(root / 'output'), GITHUB_ENV=str(root / 'env'))
    try:
        exec(textwrap.dedent(request['source']), {})
    except SystemExit as result:
        if result.code:
            raise
    print(json.dumps({'output': (root / 'output').read_text(), 'env': (root / 'env').read_text()}))
`], { input: JSON.stringify({ source, scenario }), encoding: 'utf8',
    env: { PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1' } })
}

test('actual complete-release fixture can publish the original source after normal main promotion', () => {
  const result = runActualPreflight('accepted')
  assert.equal(result.status, 0, result.stderr)
  assert.match(result.stdout, new RegExp(`revision=${sha}`))
  const emitted = JSON.parse(result.stdout.trim().split('\n').at(-1))
  assert.ok(emitted.env.startsWith('SOURCE_VALIDATION='))
  const validation = JSON.parse(emitted.env.trim().slice('SOURCE_VALIDATION='.length))
  assert.equal(validateDispatch(fixture().event, validation, sha, tree).id, 123)
  assert.equal(validation.mainPromotion.mergeRevision, '1'.repeat(40))
})

test('actual complete-release preflight rejects green release CI before protected main promotion', () => {
  const result = runActualPreflight('unmerged')
  assert.notEqual(result.status, 0, 'An unmerged release with green CI must not emit publication authority')
  assert.match(result.stderr, /AssertionError/)
})

test('actual preflight rejects incomplete, foreign, unreviewed or bypassed main promotion', () => {
  for (const scenario of ['unmerged_but_on_main', 'unprotected_main', 'wrong_promotion_head',
    'wrong_promotion_base', 'review_pending', 'review_unresolved', 'forged_review',
    'merge_not_on_main', 'not_a_normal_merge', 'changed_merge_tree']) {
    const result = runActualPreflight(scenario)
    assert.notEqual(result.status, 0, `${scenario} must not authorize publication`)
    assert.match(result.stderr, /AssertionError/, result.stderr)
  }
})

test('receipt validation rejects absent or inconsistent protected main acceptance evidence', () => {
  for (const mutate of [
    v => { delete v.mainPromotion },
    v => { v.mainPromotion.mainProtected = false },
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

test('actual complete-release recovery retains immutable validation after main advances', () => {
  function validation(scenario) {
    const result = runActualPreflight(scenario)
    assert.equal(result.status, 0, result.stderr)
    const emitted = JSON.parse(result.stdout.trim().split('\n').at(-1))
    return JSON.parse(emitted.env.trim().slice('SOURCE_VALIDATION='.length))
  }
  const reserved = validation('accepted')
  const recovery = validation('accepted_later_main')
  // Sidecar recovery checks equality with the authenticated reservation validation.
  // Both invocations must still prove source/merge ancestry in their current main.
  assert.deepEqual(recovery, reserved)
  assert.equal(validateDispatch(fixture().event, recovery, sha, tree).id, 123)
})
