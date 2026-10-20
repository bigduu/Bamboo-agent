// Run the production inline bootstrap unchanged. Only GitHub's read transport
// is replaced; ancestry uses an actual disposable Git merge graph.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const copy = (value) => JSON.parse(JSON.stringify(value))
const FULL = ['Validation Source', 'Test', 'E2E Tests', 'Lint',
  'MSRV (Rust 1.95)', 'Security Audit', 'cargo-deny (licenses/bans/advisories/sources)',
  'Documentation', 'TLS Fixture (macos-latest)', 'TLS Fixture (windows-latest)',
  'ToolEvent Recorder E2E (ubuntu-latest)', 'ToolEvent Recorder E2E (macos-latest)',
  'ToolEvent Recorder E2E (windows-latest)']

const PLATFORMS = ['Build (ubuntu-latest)', 'Build (macos-latest)', 'Build (windows-latest)']

async function verify(t, bootstrap) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-main-authority-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: directory, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  const commit = (name) => {
    fs.writeFileSync(path.join(directory, name), name)
    git('add', name)
    git('-c', 'user.name=Authority fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', name)
    return git('rev-parse', 'HEAD')
  }
  const base = commit('old-main')
  const cut = commit('accepted-cut')
  const head = commit('original-f')
  const controller = commit('accepted-controller')
  const advanced = commit('normal-dev-advance')
  git('checkout', '--detach', base)
  git('-c', 'user.name=Authority fixture', '-c', 'user.email=fixture@example.invalid',
    'merge', '--no-ff', '-qm', 'normal release merge', head)
  const source = git('rev-parse', 'HEAD')
  git('checkout', '--detach', base)
  const foreign = commit('unaccepted-feature')
  const repository = 'bigduu/Bamboo-agent'
  const repo = { id: 1164779771, full_name: repository }
  const release = `release/${cut}`
  const ci = (id, event, sha, ref) => ({ id, run_attempt: 1, name: 'CI', path: '.github/workflows/ci.yml',
    event, head_sha: sha, head_branch: ref, status: 'completed', conclusion: 'success',
    repository: repo, head_repository: repo })
  const mainCI = ci(111, 'push', source, 'main')
  const pull = { number: 42, state: 'closed', merged: true, draft: false, merge_commit_sha: source,
    merged_at: '2026-10-20T03:00:00Z', base: { ref: 'main', sha: base, repo }, head: { ref: release, sha: head, repo } }
  const pre = { ...ci(222, 'pull_request', head, release), pull_requests: [pull] }
  const jobs = (id, promotion) => ({ jobs: [...FULL, ...(promotion ? [...PLATFORMS, 'Promotion Source'] : [])].map(name => ({
    name, run_id: id, run_attempt: 1, status: 'completed', conclusion: 'success', completed_at: '2026-10-20T02:59:00Z' })) })
  const commitObject = (sha) => ({ sha, tree: { sha: git('rev-parse', `${sha}^{tree}`) },
    parents: git('show', '-s', '--format=%P', sha).split(' ').map(sha => ({ sha })) })
  const routes = {
    'branches/main': { name: 'main', protected: false, commit: { sha: source } },
    'branches/dev': { name: 'dev', protected: true, commit: { sha: controller } },
    'actions/runs/111': mainCI,
    'actions/runs/222': pre,
    'actions/runs/111/jobs?filter=latest&per_page=100&page=1': jobs(111, false),
    'actions/runs/222/jobs?filter=latest&per_page=100&page=1': jobs(222, true),
    [`commits/${source}/pulls?per_page=100&page=1`]: [pull],
    'pulls/42': pull,
    [`git/commits/${source}`]: commitObject(source),
    [`git/commits/${head}`]: commitObject(head),
    [`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`]: { workflow_runs: [pre] },
  }
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  fs.writeFileSync(path.join(bin, 'gh'), `#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
route = sys.argv[-1]
assert route.startswith('repos/bigduu/Bamboo-agent/')
assert all(flag not in sys.argv for flag in ['-X', '--method', '--input', '-f', '-F'])
route = route.removeprefix('repos/bigduu/Bamboo-agent/')
calls = pathlib.Path(os.environ['FIXTURE_CALLS'])
previous = calls.read_text().splitlines()
with calls.open('a') as stream: stream.write(route + '\\n')
if os.environ.get('FIXTURE_API_FAILURE') == 'true': sys.exit(1)
rows = json.loads(pathlib.Path(os.environ['FIXTURE_ROUTES']).read_text())
if route.startswith('compare/'):
    source, target = route.removeprefix('compare/').split('...')
    base = subprocess.check_output(['git', 'merge-base', source, target], cwd=os.environ['FIXTURE_REPOSITORY'], text=True).strip()
    print(json.dumps({'base_commit': {'sha': source}, 'merge_base_commit': {'sha': base},
      'status': 'identical' if source == target else 'ahead' if base == source else 'diverged'}))
else:
    assert route in rows, 'Unexpected route: ' + route
    value = rows[route]
    if isinstance(value, dict) and 'sequence' in value:
        value = value['sequence'][min(previous.count(route), len(value['sequence']) - 1)]
    print(json.dumps(value))
`, { mode: 0o755 })
  const calls = path.join(directory, 'calls')
  const eventFile = path.join(directory, 'event.json')
  const routeFile = path.join(directory, 'routes.json')
  const output = path.join(directory, 'output')
  function run(change = () => {}, environment = {}) {
    const input = { routes: copy(routes), event: { workflow_run: copy(mainCI) } }
    change(input)
    fs.writeFileSync(routeFile, JSON.stringify(input.routes))
    fs.writeFileSync(eventFile, JSON.stringify(input.event))
    fs.writeFileSync(calls, '')
    fs.writeFileSync(output, '')
    const result = spawnSync('python3', ['-c', bootstrap], { encoding: 'utf8', timeout: 30000, env: {
      PATH: `${bin}:${process.env.PATH}`, HOME: directory, LANG: 'C.UTF-8', LC_ALL: 'C.UTF-8',
      GITHUB_REPOSITORY: repository, GITHUB_EVENT_NAME: 'workflow_run', GITHUB_EVENT_PATH: eventFile,
      GITHUB_SHA: controller, GITHUB_WORKFLOW_SHA: controller, GITHUB_REF: 'refs/heads/dev',
      GITHUB_REF_PROTECTED: 'true', GITHUB_WORKFLOW_REF: `${repository}/.github/workflows/publish-crate.yml@refs/heads/dev`,
      GITHUB_OUTPUT: output, GH_TOKEN: 'dummy-read-only-metadata-token',
      FIXTURE_CALLS: calls, FIXTURE_ROUTES: routeFile, FIXTURE_REPOSITORY: directory, ...environment } })
    return { ...result, output: fs.readFileSync(output, 'utf8'), calls: fs.readFileSync(calls, 'utf8') }
  }
  for (const [name, change] of [
    ['unprotected main with complete normal merge evidence', () => {}],
    ['main push legitimately skips platform builds already checked before merge', x => {
      x.routes['actions/runs/111/jobs?filter=latest&per_page=100&page=1'].jobs.push(...PLATFORMS.map(name => ({ name, run_id: 111, status: 'completed', conclusion: 'skipped' })))
    }],
    ['protected main does not change the evidence requirement', x => { x.routes['branches/main'].protected = true }],
    ['normal dev advancement and deleted release ref need no historical ref lookup', x => {
      x.routes['branches/dev'] = { sequence: [routes['branches/dev'], { ...routes['branches/dev'], commit: { sha: advanced } }] }
    }],
    ['failed-only CI rerun retains earlier successful job attempts', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs[0].run_attempt = 2
    }],
  ]) await t.test(name, () => {
    const result = run(change)
    assert.equal(result.status, 0, result.stderr)
    assert.equal(result.output, `revision=${source}\n`)
  })
  const negatives = [
    ['no source PR, even when main happens to be protected', x => {
      x.routes['branches/main'].protected = true; x.routes[`commits/${source}/pulls?per_page=100&page=1`] = []
    }],
    ['unmerged PR', x => { x.routes['pulls/42'].merged = false }],
    ['ambiguous PR association', x => { x.routes[`commits/${source}/pulls?per_page=100&page=1`].push(copy(pull)) }],
    ['foreign head repo', x => { x.routes['pulls/42'].head.repo.full_name = 'foreign/fork' }],
    ['non-release head', x => { x.routes['pulls/42'].head.ref = 'feature/topic' }],
    ['wrong base parent', x => { x.routes['pulls/42'].base.sha = cut }],
    ['squash or direct main commit', x => { x.routes[`git/commits/${source}`].parents.pop() }],
    ['extra main tree changes', x => { x.routes[`git/commits/${source}`].tree.sha = 'f'.repeat(40) }],
    ['unaccepted original F', x => { x.routes['branches/dev'].commit.sha = base }],
    ['cut outside original F history', x => { x.routes['pulls/42'].head.ref = `release/${foreign}` }],
    ['unprotected dev', x => { x.routes['branches/dev'].protected = false }],
    ['main moved before authorization', x => { x.routes['branches/main'].commit.sha = foreign }],
    ['main moved during authorization', x => { x.routes['branches/main'] = { sequence: [routes['branches/main'], { ...routes['branches/main'], commit: { sha: foreign } }] } }],
    ['dev rewrites during proof', x => { x.routes['branches/dev'] = { sequence: [routes['branches/dev'], { ...routes['branches/dev'], commit: { sha: base } }] } }],
    ['missing pre-merge CI', x => { x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs = [] }],
    ['stale PR CI base', x => { x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs[0].pull_requests[0].base.sha = cut }],
    ['server main CI failed', x => { x.routes['actions/runs/111'].conclusion = 'failure' }],
    ['stale delivered main CI attempt', x => { x.routes['actions/runs/111'].run_attempt = 2 }],
    ['wrong CI workflow path', x => { x.routes['actions/runs/111'].path = '.github/workflows/other.yml' }],
    ['foreign main CI source repo', x => { x.routes['actions/runs/111'].head_repository.full_name = 'foreign/repo' }],
    ['pre-merge CI completes too late', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs[0].completed_at = '2026-10-20T03:01:00Z' }],
    ['same-second CI completion is not proven before merge', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs[0].completed_at = pull.merged_at }],
    ['main CI attempt changes during proof', x => { x.routes['actions/runs/111'] = { sequence: [mainCI, { ...mainCI, run_attempt: 2 }] } }],
    ['pre-merge CI attempt changes during proof', x => { x.routes['actions/runs/222'] = { sequence: [pre, { ...pre, run_attempt: 2 }] } }],
    ['Promotion Source skipped', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs.at(-1).conclusion = 'skipped' }],
    ['newer exact-input red must not fall back to old green', x => {
      const red = { ...copy(pre), id: 333, conclusion: 'failure' }
      x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs.unshift(red)
      x.routes['actions/runs/333'] = red
    }],
  ]
  for (const name of FULL) negatives.push([`main CI cannot skip ${name}`, x => {
    x.routes['actions/runs/111/jobs?filter=latest&per_page=100&page=1'].jobs.find(job => job.name === name).conclusion = 'skipped'
  }])
  for (const name of PLATFORMS) negatives.push([`pre-merge CI cannot skip ${name}`, x => {
    x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs.find(job => job.name === name).conclusion = 'skipped'
  }])
  for (const [name, change] of negatives) await t.test(name, () => {
    const result = run(change)
    assert.notEqual(result.status, 0, `${name}: ${result.stdout}`)
    assert.equal(result.output, '', 'No untrusted source may reach checkout/credentials/reservation/upload')
  })
  for (const [name, environment] of [
    ['manual complete dispatch stays forbidden', { GITHUB_EVENT_NAME: 'workflow_dispatch' }],
    ['wrong workflow controller ref', { GITHUB_REF: 'refs/heads/main' }],
    ['unprotected workflow controller', { GITHUB_REF_PROTECTED: 'false' }],
    ['controller outside protected dev history', { GITHUB_WORKFLOW_SHA: foreign }],
    ['API transport fails closed', { FIXTURE_API_FAILURE: 'true' }],
  ]) await t.test(name, () => {
    const result = run(undefined, environment)
    assert.notEqual(result.status, 0)
    assert.equal(result.output, '')
  })
}
module.exports = { verify }
