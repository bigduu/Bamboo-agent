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

async function verify(t, bootstrap, producer = 'v1') {
  assert.match(producer, /^v[12]$/)
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-main-authority-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: directory, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  // Frozen producer bytes, not an active workflow or a caller-authored receipt.
  const definition = fs.readFileSync(path.join(__dirname, `fixtures/main-authority-ci-${producer}.yml`))
  fs.mkdirSync(path.join(directory, '.github/workflows'), { recursive: true })
  fs.writeFileSync(path.join(directory, '.github/workflows/ci.yml'), definition)
  git('add', '.github/workflows/ci.yml')
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
  git('-c', 'user.name=Authority fixture', '-c', 'user.email=fixture@example.invalid',
    'merge', '--no-ff', '-qm', 'historical GitHub test snapshot', head)
  const snapshot = git('rev-parse', 'HEAD')
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
  const pre = { ...ci(222, 'pull_request', head, release), workflow_id: 237621118,
    check_suite_id: 444, check_suite_node_id: 'CS_fixture', pull_requests: [pull] }
  const steps = ['Set up job', 'Bind full validation to the actual event snapshot', 'Complete job']
    .map((name, i) => ({ name, number: i + 1, status: 'completed', conclusion: 'success' }))
  const jobs = (id, promotion) => ({ jobs: [...FULL, ...(promotion ? [...PLATFORMS, 'Promotion Source'] : [])].map((name, i) => ({
    id: id * 100 + i, name, run_id: id, run_attempt: 1, head_sha: head, head_branch: release,
    steps, status: 'completed', conclusion: 'success', completed_at: '2026-10-20T02:59:00Z' })) })
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
    [`git/commits/${snapshot}`]: commitObject(snapshot),
    [`contents/.github/workflows/ci.yml?ref=${head}`]: { encoding: 'base64', content: definition.toString('base64') },
    [`contents/.github/workflows/ci.yml?ref=${snapshot}`]: { encoding: 'base64', content: definition.toString('base64') },
    'actions/runs/222/attempts/1/jobs?per_page=100&page=1': jobs(222, true),
    'actions/jobs/22200': jobs(222, true).jobs[0],
    'actions/jobs/22200/logs': { raw: `2026-10-20T02:58:00Z \x1b[36;1mSet up runner\x1b[0m\n2026-10-20T02:58:00.0000000Z Event source: ${snapshot}; full validation: true\n` },
    graphql: { data: { node: { databaseId: 444, repository: { databaseId: repo.id, nameWithOwner: repository },
      app: { databaseId: 15368, slug: 'github-actions' }, commit: { oid: head },
      workflowRun: { databaseId: pre.id, event: 'pull_request', runAttempt: 1,
        workflow: { databaseId: pre.workflow_id, name: 'CI' },
        file: { path: '.github/workflows/ci.yml', repositoryName: repository,
          repositoryFileUrl: `https://github.com/${repository}/blob/${head}/.github/workflows/ci.yml` } } } } },
    [`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`]: { workflow_runs: [pre] },
  }
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  fs.writeFileSync(path.join(bin, 'gh'), `#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
if 'graphql' in sys.argv:
    # Only this fixed read query and the server-recorded suite ID are accepted.
    assert sys.argv[:5] == ['' + sys.argv[0], 'api', '--hostname', 'github.com', 'graphql']
    query = sys.argv[sys.argv.index('-f') + 1]
    assert query.startswith('query=query($suite:ID!){node(id:$suite){... on CheckSuite{')
    assert 'mutation' not in query and query.count('workflowRun') == 1
    assert sys.argv[-2:] == ['-f', 'suite=CS_fixture']
    route = 'graphql'
else:
    route = sys.argv[-1]
    assert route.startswith('repos/bigduu/Bamboo-agent/')
    assert all(flag not in sys.argv for flag in ['-X', '--method', '--input', '-f', '-F'])
    route = route.removeprefix('repos/bigduu/Bamboo-agent/')
    if route.endswith('/logs'):
        assert '--allow-escape-sequences' in sys.argv, 'Official runner logs contain ANSI bytes'
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
    print(value['raw'] if isinstance(value, dict) and 'raw' in value else json.dumps(value), end='\\n')
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
  const empty = x => {
    x.routes['actions/runs/222'].pull_requests = []
    x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs[0].pull_requests = []
  }
  for (const [name, change] of [
    ['empty association uses authenticated historical source job', empty],
    ['empty association retains source job from its exact earlier attempt', x => {
      empty(x)
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
    }],
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
  const suite = x => x.routes.graphql.data.node
  const sourceJob = x => x.routes['actions/runs/222/jobs?filter=latest&per_page=100&page=1'].jobs[0]
  const raw = x => x.routes['actions/jobs/22200/logs']
  for (const [name, change] of [
    ['missing association field is not empty', x => { delete x.routes['actions/runs/222'].pull_requests }],
    ['nonempty conflicting association cannot use a log', x => { x.routes['actions/runs/222'].pull_requests = [{ ...copy(pull), base: { ...pull.base, sha: cut } }] }],
    ['foreign suite repository', x => { suite(x).repository.nameWithOwner = 'foreign/fork' }],
    ['wrong suite repo id', x => { suite(x).repository.databaseId++ }],
    ['wrong suite id', x => { suite(x).databaseId++ }],
    ['non Actions producer', x => { suite(x).app.slug = 'untrusted-app' }],
    ['forged Actions app name with wrong app id', x => { suite(x).app.databaseId++ }],
    ['wrong suite commit', x => { suite(x).commit.oid = foreign }],
    ['wrong official run', x => { suite(x).workflowRun.databaseId++ }],
    ['wrong official event', x => { suite(x).workflowRun.event = 'workflow_dispatch' }],
    ['wrong official attempt', x => { suite(x).workflowRun.runAttempt++ }],
    ['wrong official workflow', x => { suite(x).workflowRun.workflow.databaseId++ }],
    ['wrong executed workflow file', x => { suite(x).workflowRun.file.path = '.github/workflows/forged.yml' }],
    ['foreign executed file repo', x => { suite(x).workflowRun.file.repositoryName = 'foreign/fork' }],
    ['unrelated definition ref', x => { suite(x).workflowRun.file.repositoryFileUrl = `https://github.com/${repository}/blob/${foreign}/.github/workflows/ci.yml` }],
    ['unreviewed producer even with a convincing log', x => { x.routes[`contents/.github/workflows/ci.yml?ref=${head}`].content = Buffer.from('fake producer').toString('base64') }],
    ['wrong historical snapshot workflow bytes', x => { x.routes[`contents/.github/workflows/ci.yml?ref=${snapshot}`].content = Buffer.from('fake producer').toString('base64') }],
    ['GraphQL errors', x => { x.routes.graphql.errors = [{ message: 'unavailable' }] }],
    ['missing source attempt', x => { delete sourceJob(x).run_attempt }],
    ['source job from a future attempt', x => { sourceJob(x).run_attempt = 2 }],
    ['job absent from its attempt', x => { x.routes['actions/runs/222/attempts/1/jobs?per_page=100&page=1'].jobs.shift() }],
    ['duplicate source job in attempt', x => { x.routes['actions/runs/222/attempts/1/jobs?per_page=100&page=1'].jobs.push(copy(sourceJob(x))) }],
    ['wrong job identity', x => { x.routes['actions/jobs/22200'].run_id++ }],
    ['wrong job head', x => { sourceJob(x).head_sha = foreign }],
    ['unexpected source step', x => { sourceJob(x).steps.push({ name: 'Run untrusted checkout' }) }],
    ['skipped source step', x => { sourceJob(x).steps[1].conclusion = 'skipped' }],
    ['failed source job', x => { sourceJob(x).conclusion = 'failure' }],
    ['skipped source job', x => { sourceJob(x).conclusion = 'skipped' }],
    ['missing record', x => { raw(x).raw = '' }],
    ['quoted script is not an admission', x => { raw(x).raw = `2026-10-20T02:58:00Z echo 'Event source: ${snapshot}; full validation: true'\n` }],
    ['duplicate records', x => { raw(x).raw += raw(x).raw }],
    ['conflicting records', x => { raw(x).raw += `2026-10-20T02:58:01Z Event source: ${foreign}; full validation: true\n` }],
    ['non-full record', x => { raw(x).raw = raw(x).raw.replace('true', 'false') }],
    ['malformed record', x => { raw(x).raw = raw(x).raw.replace(snapshot, 'bad-sha') }],
    ['oversized log', x => { raw(x).raw += 'x'.repeat(262145) }],
    ['missing immutable snapshot', x => { delete x.routes[`git/commits/${snapshot}`] }],
    ['wrong immutable object identity', x => { x.routes[`git/commits/${snapshot}`].sha = foreign }],
    ['snapshot wrong head', x => { x.routes[`git/commits/${snapshot}`].parents[1].sha = foreign }],
    ['snapshot wrong base', x => { x.routes[`git/commits/${snapshot}`].parents[0].sha = cut }],
    ['snapshot reversed parents', x => { x.routes[`git/commits/${snapshot}`].parents.reverse() }],
    ['snapshot extra parent', x => { x.routes[`git/commits/${snapshot}`].parents.push({ sha: cut }) }],
    ['snapshot wrong tree', x => { x.routes[`git/commits/${snapshot}`].tree.sha = git('rev-parse', `${foreign}^{tree}`) }],
    ['suite changes during proof', x => { x.routes['actions/runs/222'] = { sequence: [copy(x.routes['actions/runs/222']), { ...copy(x.routes['actions/runs/222']), check_suite_id: 999 }] } }],
    ['association becomes conflicting during proof', x => { x.routes['actions/runs/222'] = { sequence: [copy(x.routes['actions/runs/222']), { ...copy(x.routes['actions/runs/222']), pull_requests: [{ ...copy(pull), base: { ...pull.base, sha: cut } }] }] } }],
    ['newer empty red cannot fall back to older green', x => {
      const red = { ...copy(pre), id: 333, pull_requests: [], conclusion: 'failure' }
      x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs.unshift(red)
      x.routes['actions/runs/333'] = red
    }],
  ]) negatives.push([`empty association rejects ${name}`, x => { empty(x); change(x) }])
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
