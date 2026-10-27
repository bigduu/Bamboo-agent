const assert = require('node:assert/strict')

// Historical dispatch receipt shape retained for inspecting signed evidence.
// Current publication uses a protected Dev controller and an exact feature-tested frozen source.
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
  assert.equal(sourceRevision, run.head_sha, 'Publish the original validated source')
  assert.equal(run.html_url, `https://github.com/bigduu/Bamboo-agent/actions/runs/${run.id}`)
  assert.match(validation.validatedRevision, SHA)
  assert.match(validation.validatedTree, SHA)
  assert.deepEqual(validation.requiredChecks, REQUIRED)
  assert.ok(Array.isArray(validation.policyOnlyPaths))
  assert.deepEqual(validation.policyOnlyPaths, [], 'No changed product source may reuse earlier CI')
  assert.equal(sourceTree, validation.validatedTree, 'Require the exact accepted CI tree')
  const promotion = validation.mainPromotion
  assert.equal(promotion?.mainProtected, true, 'Require protected main promotion before publication')
  assert.equal(promotion.sourceInMain, true)
  assert.equal(promotion.reviewedBeforeMerge, true, 'Require review completion before normal main promotion')
  assert.match(promotion.mergeRevision, SHA)
  assert.equal(promotion.mergeTree, sourceTree)
  assert.equal(promotion.mergeParents?.length, 2)
  for (const parent of promotion.mergeParents) assert.match(parent, SHA)
  assert.ok(promotion.mergeParents.includes(sourceRevision))
  const pr = promotion.pullRequest
  assert.ok(Number.isSafeInteger(pr?.number) && pr.number > 0)
  assert.equal(pr.html_url, `https://github.com/bigduu/Bamboo-agent/pull/${pr.number}`)
  assert.equal(pr.state, 'closed')
  assert.equal(pr.merged, true)
  assert.equal(pr.draft, false)
  assert.equal(pr.baseRef, 'main')
  assert.equal(pr.headRef, run.head_branch)
  assert.equal(pr.headRevision, sourceRevision)
  assert.ok(typeof pr.mergedAt === 'string' && Number.isFinite(Date.parse(pr.mergedAt)))
  const summary = promotion.reviewSummary
  // The preflight logs the actual comment URL; stable recovery binds its verified result.
  assert.equal(summary?.actor?.type, 'Bot')
  assert.ok(['chatgpt-codex-connector[bot]', 'chatgpt-codex-connector'].includes(summary.actor.login))
  for (const kind of ['code_review', 'security_review']) {
    const review = summary.state[kind]
    assert.equal(review?.commit_sha, sourceRevision)
    assert.equal(review.status, 'completed')
    assert.equal(review.has_any_findings, false)
    assert.equal(review.has_unresolved_findings, false)
  }
  // Distinct HEAD-derived metadata is recorded, never claimed byte-identical.
  assert.equal(validation.productTreeVerified, true)
  return run
}

module.exports = { REQUIRED, validateDispatch }


const REPOSITORY = 'bigduu/Bamboo-agent'
const FEATURE_CHECKS = ['Validation Source', 'Test', 'E2E Tests', 'Feature Unit and E2E']
function githubRead(route) {
  return JSON.parse(require('./crate-release.cjs').command(['gh', 'api', '--hostname', 'github.com',
    'repos/' + REPOSITORY + '/' + route], { encoding: 'utf8', timeout: 30000, maxBuffer: 8 * 1024 * 1024 }))
}
function sourceEvent(env) {
  return JSON.parse(require('node:fs').readFileSync(env.GITHUB_EVENT_PATH, 'utf8'))
}
function frozenInputs(env, event) {
  const source = event.inputs?.expected_source_sha
  assert.equal(env.GITHUB_REPOSITORY, REPOSITORY)
  assert.equal(env.GITHUB_EVENT_NAME, 'workflow_dispatch')
  assert.match(source || '', SHA)
  assert.match(env.GITHUB_WORKFLOW_SHA || '', SHA)
  assert.equal(env.GITHUB_SHA, env.GITHUB_WORKFLOW_SHA)
  assert.equal(env.GITHUB_REF, 'refs/heads/dev')
  assert.equal(env.GITHUB_REF_PROTECTED, 'true')
  assert.equal(env.GITHUB_WORKFLOW_REF, REPOSITORY + '/.github/workflows/publish-crate.yml@' + env.GITHUB_REF)
  for (const name of ['feature_pr', 'accepted_ci_run']) assert.match(String(event.inputs[name] || ''), /^[1-9][0-9]*$/)
  return source
}
function frozenGuard(validation, env = process.env, read = githubRead, event = sourceEvent(env)) {
  const source = frozenInputs(env, event)
  assert.equal(validation.sourceRevision, source)
  const dev = read('branches/dev'), ref = read('git/ref/heads/release/' + source)
  assert.equal(dev.name, 'dev'); assert.equal(dev.protected, true)
  assert.equal(ref.ref, 'refs/heads/release/' + source)
  assert.equal(ref.object.type, 'commit'); assert.equal(ref.object.sha, source)
  for (const revision of new Set([source, env.GITHUB_WORKFLOW_SHA])) {
    const comparison = read('compare/' + revision + '...' + dev.commit.sha)
    assert.equal(comparison.base_commit.sha, revision)
    assert.equal(comparison.merge_base_commit.sha, revision)
    assert.ok(['ahead', 'identical'].includes(comparison.status), 'Source or controller left protected Dev history')
  }
  const pr = read('pulls/' + event.inputs.feature_pr), run = read('actions/runs/' + event.inputs.accepted_ci_run)
  assert.equal(pr.number, Number(event.inputs.feature_pr)); assert.equal(run.id, Number(event.inputs.accepted_ci_run))
  assert.equal(pr.state, 'closed'); assert.equal(pr.merged, true); assert.equal(pr.draft, false)
  assert.equal(pr.base.ref, 'dev'); assert.equal(pr.merge_commit_sha, source)
  assert.equal(pr.base.repo.full_name, REPOSITORY); assert.equal(pr.head.repo.full_name, REPOSITORY)
  assert.equal(run.status, 'completed'); assert.equal(run.conclusion, 'success')
  assert.equal(run.event, 'pull_request'); assert.equal(run.name, 'CI'); assert.equal(run.path, '.github/workflows/ci.yml')
  assert.equal(run.head_sha, pr.head.sha); assert.equal(run.head_branch, pr.head.ref)
  assert.equal(run.repository.full_name, REPOSITORY); assert.equal(run.head_repository.full_name, REPOSITORY)
  if (validation.ciRun) for (const name of ['id', 'run_attempt', 'head_sha', 'workflow_id', 'check_suite_id']) {
    assert.equal(run[name], validation.ciRun[name], 'Accepted CI identity changed: ' + name)
  }
  return { pr, run }
}
function authorizeFrozenRelease(env = process.env, read = githubRead, log = id =>
  require('./crate-release.cjs').command(['gh', 'api', '--hostname', 'github.com', '--allow-escape-sequences',
    'repos/' + REPOSITORY + '/actions/jobs/' + id + '/logs'], { encoding: 'utf8', timeout: 30000, maxBuffer: 1024 * 1024 })) {
  const event = sourceEvent(env), source = frozenInputs(env, event)
  const { pr, run } = frozenGuard({ sourceRevision: source }, env, read, event)
  assert.equal(run.workflow_id, read('actions/workflows/ci.yml').id)
  const suite = read('check-suites/' + run.check_suite_id)
  assert.equal(suite.app.id, 15368); assert.equal(suite.head_sha, pr.head.sha)
  assert.equal(suite.status, 'completed'); assert.equal(suite.conclusion, 'success')
  const jobs = [], seen = new Set(); let total
  for (let page = 1; page <= 20; page++) {
    const data = read('actions/runs/' + run.id + '/jobs?filter=latest&per_page=100&page=' + page)
    total ??= data.total_count; assert.equal(data.total_count, total); assert.ok(total <= 2000)
    for (const job of data.jobs) { assert.ok(!seen.has(job.id)); seen.add(job.id); jobs.push(job) }
    if (jobs.length === total) break
    assert.equal(data.jobs.length, 100, 'Incomplete feature jobs')
  }
  assert.equal(jobs.length, total)
  const selected = FEATURE_CHECKS.map(name => {
    const found = jobs.filter(job => job.name === name); assert.equal(found.length, 1, name)
    const job = found[0]; assert.equal(job.status, 'completed'); assert.equal(job.conclusion, 'success')
    assert.ok(Date.parse(job.completed_at) < Date.parse(pr.merged_at), 'Feature checks must precede Dev admission')
    return job
  })
  const snapshots = [...log(selected[0].id).matchAll(/Event source: ([0-9a-f]{40}); full validation: false/g)]
  assert.equal(snapshots.length, 1, 'Missing exact tested merge from original Validation Source log')
  const merged = read('git/commits/' + source), tested = read('git/commits/' + snapshots[0][1])
  assert.equal(merged.sha, source); assert.equal(tested.sha, snapshots[0][1])
  assert.equal(merged.parents.length, 2); assert.equal(merged.parents[1].sha, pr.head.sha)
  assert.deepEqual(tested.parents.map(parent => parent.sha), merged.parents.map(parent => parent.sha))
  assert.equal(tested.tree.sha, merged.tree.sha, 'Release source differs from the feature-tested product')
  const ciRun = Object.fromEntries(['id', 'run_attempt', 'workflow_id', 'check_suite_id', 'name', 'event', 'head_sha',
    'head_branch', 'status', 'conclusion', 'html_url'].map(name => [name, run[name]]))
  ciRun.repository = { full_name: REPOSITORY }; ciRun.head_repository = { full_name: REPOSITORY }
  assert.equal(ciRun.html_url, 'https://github.com/' + REPOSITORY + '/actions/runs/' + run.id)
  const validation = { schemaVersion: 1, kind: 'bamboo-frozen-dev-release/v1', repository: REPOSITORY,
    sourceRevision: source, sourceTree: merged.tree.sha, cutRevision: source, releaseHead: source,
    baseRevision: merged.parents[0].sha, featurePr: pr.number, validatedRevision: tested.sha, ciRun }
  frozenGuard(validation, env, read, event)
  return validation
}
// The immutable product source must contain the completed cutover too: promoting
// an older tested Dev cut would restore Main builds or the old tag Docker build.
function assertFrozenSourceWorkflows(read = file => require('node:fs').readFileSync(file, 'utf8')) {
  for (const [file, marker, required] of [
    ['.github/workflows/ci.yml', '# BAMBOO_FEATURE_RELEASE_ROUTE_V1', ['pull_request', 'workflow_dispatch']],
    ['.github/workflows/docker-publish.yml', '# BAMBOO_RELEASE_IMAGE_CONSUMER_V1', ['workflow_run', 'workflow_dispatch']],
    ['.github/workflows/publish-crate.yml', '# BAMBOO_FROZEN_DEV_PUBLISHER_V1', ['workflow_dispatch']],
  ]) {
    const text = read(file); assert.ok(Buffer.byteLength(text) <= 256 * 1024)
    const lines = text.replace(/\r\n/g, '\n').split('\n')
    assert.equal(lines.filter(line => line === marker).length, 1, 'Source lacks the reviewed cutover: ' + file)
    const root = line => line.length && !/^\s|^#/.test(line)
    const declarations = lines.filter(root)
    assert.ok(declarations.every(line => /^[a-z][a-z_-]*:/.test(line)), 'Noncanonical workflow declaration: ' + file)
    assert.deepEqual(declarations.filter(line => /^on:/.test(line)), ['on:'], 'Require one block event declaration: ' + file)
    const start = lines.indexOf('on:') + 1
    const next = lines.findIndex((line, index) => index >= start && root(line))
    const rows = lines.slice(start, next < 0 ? undefined : next).filter(line => /^  \S/.test(line) && !/^  #/.test(line))
    const events = rows.map(line => {
      const match = /^  ([a-z_]+):(?:\s*#.*)?$/.exec(line)
      assert.ok(match, 'Noncanonical workflow event: ' + file)
      return match[1]
    })
    assert.deepEqual(events.sort(), [...required].sort(), 'Source retains an old or unknown trigger: ' + file)
  }
}
module.exports = { ...module.exports, FEATURE_CHECKS, authorizeFrozenRelease, frozenGuard, assertFrozenSourceWorkflows }
if (require.main === module) {
  if (process.argv[2] === 'source-contract') {
    const source = frozenInputs(process.env, sourceEvent(process.env))
    assert.equal(require('./crate-release.cjs').command(['git', 'rev-parse', 'HEAD']).trim(), source)
    assertFrozenSourceWorkflows()
  } else {
    assert.equal(process.argv[2], undefined)
    const validation = authorizeFrozenRelease()
    require('node:fs').appendFileSync(process.env.GITHUB_OUTPUT,
      'revision=' + validation.sourceRevision + '\nvalidation=' + JSON.stringify(validation) + '\n')
  }
}
