// Run the production loader and pinned verifier unchanged. Only GitHub's read
// transport is replaced; ancestry uses an actual disposable Git merge graph.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { createHash } = require('node:crypto')
const copy = (value) => JSON.parse(JSON.stringify(value))
const FULL = ['Validation Source', 'Test', 'E2E Tests', 'Lint',
  'MSRV (Rust 1.95)', 'Security Audit', 'cargo-deny (licenses/bans/advisories/sources)',
  'Documentation', 'TLS Fixture (macos-latest)', 'TLS Fixture (windows-latest)',
  'ToolEvent Recorder E2E (ubuntu-latest)', 'ToolEvent Recorder E2E (macos-latest)',
  'ToolEvent Recorder E2E (windows-latest)']

const PLATFORMS = ['Build (ubuntu-latest)', 'Build (macos-latest)', 'Build (windows-latest)']

async function verify(t, bootstrap, producer = 'v1', mode = 'publication', options = {}) {
  assert.match(producer, /^v(?:[1234567]|4-main-3488)$/)
  assert.ok(['publication', 'main'].includes(mode))
  const current = producer === 'v7', grouped = producer === 'v3'
  const serverPartitioned = current || producer === 'v6'
  const actualMain = producer === 'v4-main-3488'
  const legacyCrates = producer === 'v4' || actualMain
  const crates = legacyCrates || producer === 'v5' || serverPartitioned, light = grouped || crates
  const platformLabel = serverPartitioned || producer === 'v5' ? 'Platform Tests' : 'Build'
  const platforms = PLATFORMS.map(name => name.replace('Build', platformLabel))
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-main-authority-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: directory, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
    return result.stdout.trim()
  }
  git('init', '-q')
  // Frozen producer bytes, not an active workflow or a caller-authored receipt.
  const definition = fs.readFileSync(current ? '.github/workflows/ci.yml'
    : path.join(__dirname, `fixtures/main-authority-ci-${producer}.yml`))
  const helper = fs.readFileSync('scripts/main-release-admission.py')
  const helperDigest = createHash('sha256').update(helper).digest('hex')
  const producerHelper = grouped || (crates && !current)
    ? fs.readFileSync(path.join(__dirname, `fixtures/main-authority-admission-${producer}.py`)) : helper
  const producerHelperDigest = createHash('sha256').update(producerHelper).digest('hex')
  const packagesByLabel = {}, blocksByLabel = {}
  for (const [label, id] of [['Test', 'test-crates'], ['E2E', 'e2e-crates']]) {
    const job = crates ? definition.toString().split(`  ${id}:\n`)[1].split(/\n  [a-z0-9-]+:/)[0] : ''
    packagesByLabel[label] = [...job.matchAll(/^          - ([a-z0-9-]+)$/gm)].map(match => match[1])
    blocksByLabel[label] = serverPartitioned ? [...job.matchAll(/^            server_block: ([a-z-]+)$/gm)].map(match => match[1]) : []
  }
  const cratePackages = packagesByLabel.Test
  const crateNames = ['Test SSH', ...['Test', 'E2E'].flatMap(label => [
    ...packagesByLabel[label].map(crate => `${label} (${crate})`),
    ...blocksByLabel[label].map(block => `${label} (bamboo-server/${block})`)])]
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
  const mainCI = { ...ci(111, 'push', source, 'main'), workflow_id: 237621118,
    check_suite_id: 555, check_suite_node_id: 'CS_main' }
  const pull = { number: 42, state: 'closed', merged: true, draft: false, merge_commit_sha: source,
    merged_at: '2026-10-20T03:00:00Z', base: { ref: 'main', sha: base, repo }, head: { ref: release, sha: head, repo } }
  const pre = { ...ci(222, 'pull_request', head, release), workflow_id: 237621118,
    check_suite_id: 444, check_suite_node_id: 'CS_fixture', pull_requests: [pull] }
  const steps = ['Set up job', 'Bind full validation to the actual event snapshot', 'Complete job']
    .map((name, i) => ({ name, number: i + 1, status: 'completed', conclusion: 'success' }))
  const plans = [
    {profile: 'default', label: 'Test', inventory: 2, doctests: 1, packages: ['bamboo-config'], blocks: [
      {id: 'bamboo-config-lib-0', package: 'bamboo-config', kind: 'libtest', count: 2, ignored: 0, timeoutSeconds: 1200},
      {id: 'bamboo-config-doc-0', package: 'bamboo-config', kind: 'doctest', count: 1, ignored: 0, timeoutSeconds: 1200}]},
    {profile: 'all-features', label: 'E2E', inventory: 3, doctests: 0, packages: ['bamboo-engine'], blocks: [
      {id: 'bamboo-engine-lib-0', package: 'bamboo-engine', kind: 'libtest', count: 3, ignored: 0, timeoutSeconds: 1200}]},
    {profile: 'ssh-live', label: 'Test SSH', inventory: 1, doctests: 0, packages: ['bamboo-broker'], blocks: [
      {id: 'bamboo-broker-russh-0', package: 'bamboo-broker', kind: 'ssh', count: 1, ignored: 0, timeoutSeconds: 1200}]},
  ].map((p, index) => ({...p, schema: 'bamboo-ci-test-plan/v1', sha: snapshot,
    tree: git('rev-parse', `${source}^{tree}`), producerAttempt: 1, planId: 900 + index,
    planKey: createHash('sha256').update(p.profile).digest('hex')}))
  if (options.inventoryFixture) {
    const actual = JSON.parse(require('node:zlib').gunzipSync(fs.readFileSync(options.inventoryFixture)))
    const blocks = actual.historicalBlocks
    Object.assign(plans[1], {inventory: 12397, packages: [...new Set(blocks.map(b => b.package))].sort(),
      blocks: blocks.map(b => ({id: b.id, package: b.package, kind: b.kind, matrixFamily: b.matrixFamily,
        count: b.count, ignored: b.ignored, timeoutSeconds: 1200}))})
  }
  for (const plan of plans) {
    const runtime = plan.blocks.filter(b => b.kind !== 'doctest'), split = Math.ceil(runtime.length / 2)
    runtime.forEach((b, i) => { b.matrixFamily = i < split ? 1 : 2 })
    plan.blocks.filter(b => b.kind === 'doctest').forEach(b => { b.matrixFamily = 1 })
  }
  const blockName = (plan, block) => plan.profile === 'ssh-live' ? 'Test SSH'
    : `${plan.label} ${block.kind === 'doctest' ? 'Doctest' : 'Block' + (block.matrixFamily === 2 ? ' 2' : '')} (${block.id})`
  const emptyNames = plans.filter(p => p.profile !== 'ssh-live' && !p.blocks.some(b => b.kind !== 'doctest' && b.matrixFamily === 2))
    .map(p => p.label + ' Block 2 (${{ matrix.id }})')
  const groupedNames = [...plans.flatMap(p => [p.label + ' Build', ...p.blocks.map(b => blockName(p, b))]), ...emptyNames]
  const mainSkipped = grouped ? ['Test Build', 'E2E Build', 'Test SSH Build', 'Test SSH',
    'Test Block (${{ matrix.id }})', 'E2E Block (${{ matrix.id }})', 'Test Doctest (${{ matrix.id }})',
    'Test Block 2 (${{ matrix.id }})', 'E2E Block 2 (${{ matrix.id }})']
    : ['Test SSH', ...['Test', 'E2E'].map(label => serverPartitioned
      ? label + " (${{ matrix.crate }}${{ ((matrix.server_block && format('/{0}', matrix.server_block)) || '') }})"
      : label + ' (${{ matrix.crate }})')]
  const jobs = (id, promotion) => ({ jobs: (!promotion && light
    ? ['Validation Source', 'Test', 'Main Release Admission', 'Quick Checks', 'Promotion Source',
      'E2E Tests', 'Lint', 'MSRV (Rust 1.95)', 'Security Audit', 'cargo-deny (licenses/bans/advisories/sources)',
      'Documentation', 'TLS Fixture (${{ matrix.os }})', 'ToolEvent Recorder E2E (${{ matrix.os }})', platformLabel + ' (${{ matrix.os }})', ...mainSkipped,
      ...(current ? ['Feature Unit and E2E'] : [])]
    : [...FULL, ...(grouped ? groupedNames : crates ? crateNames : []), ...(current ? ['Feature Unit and E2E'] : []),
      ...(promotion ? [...platforms, 'Promotion Source'] : [])]).map((name, i) => ({
    id: id * 100 + i, name, run_id: id, run_attempt: 1,
    head_sha: promotion ? head : source, head_branch: promotion ? release : 'main',
    steps: crates && promotion && crateNames.includes(name) ? ['Set up job', name === 'Test SSH' ? 'Run SSH fixture'
      : name.startsWith('Test (') ? (name.includes('bamboo-server/') ? 'Test server block' : 'Test crate')
      : (name.includes('bamboo-server/') ? 'Run E2E server block' : 'Run E2E crate tests'), 'Complete job']
      .map((name, i) => ({ name, number: i + 1, status: 'completed', conclusion: 'success' })) : steps,
    status: 'completed', conclusion: name === 'Feature Unit and E2E' || (promotion && grouped && emptyNames.includes(name)) ||
      (!promotion && light && !['Validation Source', 'Test', 'Main Release Admission'].includes(name)) ? 'skipped' : 'success',
    completed_at: '2026-10-20T02:59:00Z' })) })
  const commitObject = (sha) => ({ sha, tree: { sha: git('rev-parse', `${sha}^{tree}`) },
    parents: git('show', '-s', '--format=%P', sha).split(' ').map(sha => ({ sha })) })
  const routes = {
    'branches/main': { name: 'main', protected: false, commit: { sha: source } },
    'branches/dev': { name: 'dev', protected: true, commit: { sha: controller } },
    'actions/runs/111': mainCI,
    'actions/runs/222': pre,
    'actions/runs/111/jobs?filter=latest&per_page=10&page=1': jobs(111, false),
    'actions/runs/222/jobs?filter=latest&per_page=10&page=1': jobs(222, true),
    [`commits/${source}/pulls?per_page=100&page=1`]: [pull],
    'pulls/42': pull,
    [`git/commits/${source}`]: commitObject(source),
    [`git/commits/${head}`]: commitObject(head),
    [`git/commits/${snapshot}`]: commitObject(snapshot),
    [`contents/.github/workflows/ci.yml?ref=${head}`]: { encoding: 'base64', content: definition.toString('base64') },
    [`contents/.github/workflows/ci.yml?ref=${snapshot}`]: { encoding: 'base64', content: definition.toString('base64') },
    [`contents/.github/workflows/ci.yml?ref=${source}`]: { encoding: 'base64', content: definition.toString('base64') },
    ...Object.fromEntries([controller, head, snapshot, source].map(sha => [
      `contents/scripts/main-release-admission.py?ref=${sha}`,
      { encoding: 'base64', content: (sha === controller ? helper : producerHelper).toString('base64') }])),
    'actions/runs/111/attempts/1/jobs?per_page=10&page=1': jobs(111, false),
    'actions/runs/222/attempts/1/jobs?per_page=10&page=1': jobs(222, true),
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
  const recordLog = (marker, record) => `2026-10-20T02:58:00Z ${marker} ${Buffer.from(JSON.stringify(record)).toString('base64')}\n`
  if (grouped) for (const job of jobs(222, true).jobs) {
    let raw = ''
    for (const plan of plans) {
      if (job.name === plan.label + ' Build') raw += recordLog('GROUPED_TEST_PLAN', plan)
      for (const block of plan.blocks) if (job.name === blockName(plan, block)) raw += recordLog('GROUPED_TEST_RESULT', {
        schema: 'bamboo-ci-test-result/v1', planKey: plan.planKey, block: block.id, package: block.package,
        profile: plan.profile, sha: plan.sha, sourceTree: plan.tree, attempt: 1, status: 'passed', exitCode: 0,
        expected: block.count, timeoutSeconds: 1200, testDurationMs: 200,
        counts: {passed: block.count - block.ignored, failed: 0, ignored: block.ignored}})
      if (job.name === (plan.profile === 'all-features' ? 'E2E Tests' : 'Test')) raw += recordLog('GROUPED_TEST_SUMMARY', {
        planKey: plan.planKey, profile: plan.profile, sha: plan.sha, tree: plan.tree,
        inventory: plan.inventory, doctests: plan.doctests, blocks: plan.blocks.length, status: 'passed',
        counts: {passed: plan.inventory + plan.doctests - plan.blocks.reduce((n, b) => n + b.ignored, 0),
          failed: 0, ignored: plan.blocks.reduce((n, b) => n + b.ignored, 0)}})
    }
    if (raw) routes[`actions/jobs/${job.id}/logs`] = {raw}
  }
  routes['graphql/main'] = copy(routes.graphql)
  Object.assign(routes['graphql/main'].data.node, { databaseId: 555, commit: { oid: source },
    workflowRun: { databaseId: 111, event: 'push', runAttempt: 1,
      workflow: { databaseId: mainCI.workflow_id, name: 'CI' },
      file: { path: '.github/workflows/ci.yml', repositoryName: repository,
        repositoryFileUrl: `https://github.com/${repository}/blob/${source}/.github/workflows/ci.yml` } } })
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  fs.writeFileSync(path.join(bin, 'gh'), `#!/usr/bin/env python3
import json, os, pathlib, re, subprocess, sys
if 'graphql' in sys.argv:
    # Only this fixed read query and the server-recorded suite ID are accepted.
    assert sys.argv[:5] == ['' + sys.argv[0], 'api', '--hostname', 'github.com', 'graphql']
    query = sys.argv[sys.argv.index('-f') + 1]
    assert query.startswith('query=query($suite:ID!){node(id:$suite){... on CheckSuite{')
    assert 'mutation' not in query and query.count('workflowRun') == 1
    assert sys.argv[-2] == '-f' and sys.argv[-1] in ['suite=CS_fixture', 'suite=CS_main']
    route = 'graphql/main' if sys.argv[-1] == 'suite=CS_main' else 'graphql'
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
if route == os.environ.get('FIXTURE_API_FAILURE_ROUTE'): sys.exit(1)
rows = json.loads(pathlib.Path(os.environ['FIXTURE_ROUTES']).read_text())
if route.startswith('compare/'):
    source, target = route.removeprefix('compare/').split('...')
    base = subprocess.check_output(['git', 'merge-base', source, target], cwd=os.environ['FIXTURE_REPOSITORY'], text=True).strip()
    print(json.dumps({'base_commit': {'sha': source}, 'merge_base_commit': {'sha': base},
      'status': 'identical' if source == target else 'ahead' if base == source else 'diverged'}))
else:
    page = re.search(r'([?&])per_page=([0-9]+)&page=([0-9]+)$', route)
    canonical = re.sub(r'page=[0-9]+$', 'page=1', route) if page else route
    lookup = canonical if page and canonical in rows and isinstance(rows[canonical], dict) and 'jobs' in rows[canonical] else route
    assert lookup in rows, 'Unexpected route: ' + route
    value = rows[lookup]
    if isinstance(value, dict) and 'sequence' in value:
        value = value['sequence'][min(previous.count(route), len(value['sequence']) - 1)]
    if page and isinstance(value, dict) and 'jobs' in value:
        size = int(page.group(2))
        start = (int(page.group(3)) - 1) * size
        value = dict(value, jobs=value['jobs'][start:start + size])
    print(value['raw'] if isinstance(value, dict) and 'raw' in value else json.dumps(value), end='\\n')
`, { mode: 0o755 })
  const calls = path.join(directory, 'calls')
  const eventFile = path.join(directory, 'event.json')
  const routeFile = path.join(directory, 'routes.json')
  const output = path.join(directory, 'output')
  // Keep the CLI contract assertions in the transport, but execute that shim
  // in-process. Thousands of Python startups would slow the dev policy gate;
  // real Git ancestry still goes through the original subprocess transport.
  const runner = `import contextlib, io, os, pathlib, subprocess, sys
from unittest.mock import patch
real_output = subprocess.check_output
shim = pathlib.Path(os.environ['FIXTURE_GH_SHIM']).read_text()
def output(arguments, **options):
    if arguments[0] != 'gh': return real_output(arguments, **options)
    capture = io.StringIO()
    with patch.object(sys, 'argv', arguments), contextlib.redirect_stdout(capture):
        exec(compile(shim, '<fixture-read-only-gh>', 'exec'), {'__name__': '__fixture__'})
    value = capture.getvalue()
    return value if options.get('text') else value.encode()
with patch('subprocess.check_output', output):
    exec(compile(os.environ['FIXTURE_BOOTSTRAP'], '<production-bootstrap>', 'exec'))
`
  function run(change = () => {}, environment = {}) {
    const input = { routes: copy(routes), event: mode === 'publication' ? { workflow_run: copy(mainCI) }
      : { ref: 'refs/heads/main', after: source, repository: repo } }
    change(input)
    fs.writeFileSync(routeFile, JSON.stringify(input.routes))
    fs.writeFileSync(eventFile, JSON.stringify(input.event))
    fs.writeFileSync(calls, '')
    fs.writeFileSync(output, '')
    const result = spawnSync('python3', ['-c', runner], { encoding: 'utf8', timeout: 30000, env: {
      PATH: `${bin}:${process.env.PATH}`, HOME: directory, LANG: 'C.UTF-8', LC_ALL: 'C.UTF-8',
      GITHUB_REPOSITORY: repository, GITHUB_EVENT_NAME: 'workflow_run', GITHUB_EVENT_PATH: eventFile,
      GITHUB_SHA: controller, GITHUB_WORKFLOW_SHA: controller, GITHUB_REF: 'refs/heads/dev',
      GITHUB_REF_PROTECTED: 'true', GITHUB_WORKFLOW_REF: `${repository}/.github/workflows/publish-crate.yml@refs/heads/dev`,
      GITHUB_OUTPUT: output, GH_TOKEN: 'dummy-read-only-metadata-token',
      FIXTURE_GH_SHIM: path.join(bin, 'gh'), FIXTURE_BOOTSTRAP: bootstrap,
      ADMISSION_MODE: mode, ADMISSION_HELPER_SHA256: helperDigest,
      ...(mode === 'main' ? { GITHUB_EVENT_NAME: 'push', GITHUB_REF: 'refs/heads/main',
        GITHUB_SHA: source, GITHUB_WORKFLOW_SHA: source,
        GITHUB_WORKFLOW_REF: `${repository}/.github/workflows/ci.yml@refs/heads/main`,
        SOURCE_RESULT: 'success', FULL_VALIDATION: 'false' } : {}),
      FIXTURE_CALLS: calls, FIXTURE_ROUTES: routeFile, FIXTURE_REPOSITORY: directory, ...environment } })
    return { ...result, output: fs.readFileSync(output, 'utf8'), calls: fs.readFileSync(calls, 'utf8') }
  }
  if (options.exercise) return options.exercise({run, base, source, head, controller, plans, blockName, cratePackages})
  const empty = x => {
    x.routes['actions/runs/222'].pull_requests = []
    x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs[0].pull_requests = []
  }
  const expandedSkipped = x => {
    const expand = list => list.flatMap(job => {
      if (job.name.includes('${{ matrix.crate }}')) {
        const label = job.name.startsWith('Test ') ? 'Test' : 'E2E'
        return [...packagesByLabel[label], ...blocksByLabel[label].map(block => `bamboo-server/${block}`)].map((crate, index) => ({
          ...copy(job), id: job.id * 100 + index, name: `${label} (${crate})` }))
      }
      if (!job.name.includes('${{ matrix.os }}')) return [job]
      const platforms = job.name.startsWith('TLS Fixture') ? ['macos-latest', 'windows-latest']
        : ['ubuntu-latest', 'macos-latest', 'windows-latest']
      return platforms.map((platform, index) => ({ ...copy(job), id: job.id * 10 + index,
        name: job.name.replace('${{ matrix.os }}', platform) }))
    })
    for (const route of ['actions/runs/111/jobs?filter=latest&per_page=10&page=1',
      'actions/runs/111/attempts/1/jobs?per_page=10&page=1']) x.routes[route].jobs = expand(x.routes[route].jobs)
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
      x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs.push(...platforms.map(name => ({ name, run_id: 111, status: 'completed', conclusion: 'skipped' })))
    }],
    ['protected main does not change the evidence requirement', x => { x.routes['branches/main'].protected = true }],
    ['normal dev advancement and deleted release ref need no historical ref lookup', x => {
      x.routes['branches/dev'] = { sequence: [routes['branches/dev'], { ...routes['branches/dev'], commit: { sha: advanced } }] }
    }],
    ['failed-only CI rerun retains earlier successful job attempts', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs[0].run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = copy(x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'])
      x.routes['actions/jobs/22200'].run_attempt = 2
    }],
    ...(grouped ? [['failed shard alone can rerun while earlier build and shards remain valid', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      const list = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs
      const job = list.find(j => j.name.startsWith('Test Block ('))
      job.run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = {jobs: [copy(job)]}
      const route = `actions/jobs/${job.id}/logs`
      const encoded = x.routes[route].raw.trim().split(' ').at(-1)
      const record = JSON.parse(Buffer.from(encoded, 'base64')); record.attempt = 2
      x.routes[route].raw = recordLog('GROUPED_TEST_RESULT', record)
    }]] : []),
    ...(crates ? [['failed crate alone can rerun while earlier successful crates remain valid', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      const job = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(j => j.name.startsWith('Test ('))
      job.run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = {jobs: [copy(job)]}
    }], ['failed platform alone can rerun while earlier successful jobs remain valid', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      const job = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(j => j.name === platforms[0])
      job.run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = {jobs: [copy(job)]}
    }]] : []),
    ...(serverPartitioned ? [['failed server block alone retains the other native job attempts', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      const job = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(j => j.name === 'Test (bamboo-server/browser)')
      job.run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = {jobs: [copy(job)]}
    }]] : []),
    ...(light && mode === 'publication' ? [['complete expanded skipped matrices remain compatible', expandedSkipped]] : []),
  ].filter(([name]) => !(light && name === 'main push legitimately skips platform builds already checked before merge'))) await t.test(name, () => {
    const result = run(change)
    assert.equal(result.status, 0, result.stderr)
    if (crates) assert.deepEqual(result.calls.trim().split('\n').filter(route => route.endsWith('/logs')),
      ['actions/jobs/22200/logs'], 'Native crate jobs need no grouped result logs or receipts')
    if (serverPartitioned) for (const label of ['Test', 'E2E']) for (const block of blocksByLabel[label]) {
      assert.ok(result.stdout.includes(`${label} (bamboo-server/${block})=`), 'The authenticated job-attempt log includes every server block')
    }
    const output = result.output.trimEnd().split('\n')
    assert.equal(output[0], `revision=${source}`)
    if (light && mode === 'publication') {
      assert.equal(output.length, 2)
      assert.ok(output[1].startsWith('validation='))
      const proof = JSON.parse(output[1].slice('validation='.length))
      assert.equal(proof.kind, 'bamboo-main-release-admission/v1')
      assert.equal(proof.sourceRevision, source)
      assert.equal(proof.sourceTree, git('rev-parse', `${source}^{tree}`))
      assert.equal(proof.ciRun.id, 111)
      assert.equal(proof.ciRun.head_sha, source)
      assert.equal(proof.premerge.runId, 222)
      assert.equal(proof.premerge.snapshotRevision, snapshot)
      assert.equal(proof.releaseHead, head)
      assert.equal(proof.baseRevision, base)
    } else assert.equal(output.length, 1, 'Legacy receipts must retain their original signed shape')
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
    ['pre-merge CI completes too late', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs[0].completed_at = '2026-10-20T03:01:00Z' }],
    ['same-second CI completion is not proven before merge', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs[0].completed_at = pull.merged_at }],
    ['main CI attempt changes during proof', x => { x.routes['actions/runs/111'] = { sequence: [mainCI, { ...mainCI, run_attempt: 2 }] } }],
    ['pre-merge CI attempt changes during proof', x => { x.routes['actions/runs/222'] = { sequence: [pre, { ...pre, run_attempt: 2 }] } }],
    ['Promotion Source skipped', x => { x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.at(-1).conclusion = 'skipped' }],
    ['newer exact-input red must not fall back to old green', x => {
      const red = { ...copy(pre), id: 333, conclusion: 'failure' }
      x.routes[`actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`].workflow_runs.unshift(red)
      x.routes['actions/runs/333'] = red
    }],
    ['newer exact-input run appears during proof', x => {
      const route = `actions/workflows/ci.yml/runs?event=pull_request&head_sha=${head}&per_page=100&page=1`
      const original = copy(x.routes[route])
      x.routes[route] = { sequence: [original,
        { workflow_runs: [{ ...copy(pre), id: 333, conclusion: 'failure' }, copy(pre)] }] }
    }],
    ['full job claims an unexecuted attempt', x => {
      x.routes['actions/runs/222'].run_attempt = 2
      x.routes.graphql.data.node.workflowRun.runAttempt = 2
      x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs[1].run_attempt = 2
      x.routes['actions/runs/222/attempts/2/jobs?per_page=10&page=1'] = { jobs: [] }
    }],
  ]
  for (const name of light ? ['Validation Source', 'Test', 'Main Release Admission'] : FULL) negatives.push([`main CI cannot skip ${name}`, x => {
    x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs.find(job => job.name === name).conclusion = 'skipped'
  }])
  if (grouped) {
    const shard = jobs(222, true).jobs.find(j => j.name.startsWith('Test Block ('))
    const mutateShardRecord = (x, change) => {
      const route = `actions/jobs/${shard.id}/logs`
      const record = JSON.parse(Buffer.from(x.routes[route].raw.trim().split(' ').at(-1), 'base64'))
      change(record); x.routes[route].raw = recordLog('GROUPED_TEST_RESULT', record)
    }
    for (const conclusion of ['skipped', 'failure', 'cancelled']) negatives.push([`aggregate green cannot hide ${conclusion} shard`, x => {
      x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(j => j.id === shard.id).conclusion = conclusion
    }])
    negatives.push(['missing shard cannot hide behind green summaries', x => {
      const list = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs
      list.splice(list.findIndex(j => j.id === shard.id), 1)
    }])
    negatives.push(['different compile input cannot reuse an old green shard', x => mutateShardRecord(x, r => { r.planKey = 'e'.repeat(64) })])
    negatives.push(['shard over twenty minutes cannot be admitted', x => mutateShardRecord(x, r => { r.testDurationMs = 1200001 })])
    negatives.push(['ignored cases cannot hide failed shard cases', x => mutateShardRecord(x, r => { r.counts.ignored = r.expected; r.counts.passed = 0 })])
    negatives.push(['shard has no source-bound count receipt', x => { x.routes[`actions/jobs/${shard.id}/logs`].raw = 'no result\n' }])
  }
  if (crates) {
    const eachCrate = (x, change) => {
      const list = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs
      change(list, list.find(job => job.name.startsWith('Test (')))
    }
    for (const conclusion of ['skipped', 'failure', 'cancelled']) negatives.push([`aggregate green cannot hide ${conclusion} crate`, x => {
      eachCrate(x, (_, job) => { job.conclusion = conclusion })
    }])
    for (const [name, change] of [
      ['missing crate', (list, job) => list.splice(list.indexOf(job), 1)],
      ['duplicate crate', (list, job) => list.push({...copy(job), id: 999999})],
      ['unexpected crate', (list, job) => list.push({...copy(job), id: 999999, name: 'Test (bamboo-foreign)'})],
      ['wrong crate source', (_, job) => { job.head_sha = foreign }],
      ['wrong crate run', (_, job) => { job.run_id++ }],
      ['future crate attempt', (_, job) => { job.run_attempt = 2 }],
      ['late crate completion', (_, job) => { job.completed_at = pull.merged_at }],
      ['missing Cargo step', (_, job) => { job.steps.splice(1, 1) }],
      ['skipped Cargo step', (_, job) => { job.steps[1].conclusion = 'skipped' }],
      ['failed Cargo step', (_, job) => { job.steps[1].conclusion = 'failure' }],
    ]) negatives.push([`${name} cannot hide behind green summaries`, x => eachCrate(x, change)])
    for (const name of ['Test SSH', `E2E (${packagesByLabel.E2E.at(-1)})`]) {
      negatives.push([`full acceptance requires successful ${name}`, x => {
        x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(job => job.name === name).conclusion = 'skipped'
      }])
    }
    if (serverPartitioned) for (const label of ['Test', 'E2E']) for (const block of blocksByLabel[label]) {
      const name = `${label} (bamboo-server/${block})`
      for (const state of ['missing', 'skipped']) negatives.push([`server block ${name} cannot be ${state}`, x => {
        const list = x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs
        const index = list.findIndex(job => job.name === name)
        if (state === 'missing') list.splice(index, 1)
        else list[index].conclusion = state
      }])
    }
    if (serverPartitioned) for (const [name, change] of [
      ['an unsplit server job cannot replace its blocks', list => {
        list.splice(list.findIndex(job => job.name === 'Test (bamboo-server/browser)'), 1)
        list.push({ ...copy(list[1]), name: 'Test (bamboo-server)' })
      }],
      ['server Cargo step cannot be skipped', list => {
        list.find(job => job.name === 'E2E (bamboo-server/integration)').steps[1].conclusion = 'skipped'
      }],
      ['server job must match its original attempt', (_, input) => {
        const list = input.routes['actions/runs/222/attempts/1/jobs?per_page=10&page=1'].jobs
        list.splice(list.findIndex(job => job.name === 'Test (bamboo-server/browser)'), 1)
      }],
    ]) negatives.push([name, x => change(x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs, x)])
  }
  if (light) {
    const changeDefinition = (x, transform, revision = head) => {
      x.routes[`contents/.github/workflows/ci.yml?ref=${revision}`].content =
        Buffer.from(transform(definition.toString())).toString('base64')
    }
    for (const [name, transform] of [
      ['changed required job command', text => text.replace('cargo test --locked --no-fail-fast', 'true')],
      ['changed trigger', text => text.replace('branches: [ dev, main ]', 'branches: [ dev ]')],
      ['changed helper pin', text => text.replace(producerHelperDigest, 'f'.repeat(64))],
      ['duplicate helper pin', text => `${text}\n# ADMISSION_HELPER_SHA256: "${producerHelperDigest}"\n`],
      ['missing helper pin', text => text.replace(`ADMISSION_HELPER_SHA256: "${producerHelperDigest}"`, 'REMOVED_PIN: true')],
    ]) negatives.push([name, x => changeDefinition(x, transform)])
    negatives.push(['changed immutable producer helper bytes', x => {
      x.routes[`contents/scripts/main-release-admission.py?ref=${head}`].content = Buffer.from('raise Exception("foreign helper")\n').toString('base64')
    }])
    negatives.push(['changed controller helper bytes', x => {
      x.routes[`contents/scripts/main-release-admission.py?ref=${mode === 'main' ? source : controller}`].content = Buffer.from('raise Exception("foreign controller")\n').toString('base64')
    }])
    if (crates) {
      const alternate = fs.readFileSync(path.join(__dirname,
        `fixtures/main-authority-admission-${actualMain ? 'v4' : 'v4-main-3488'}.py`))
      const alternateDigest = createHash('sha256').update(alternate).digest('hex')
      if (legacyCrates) {
        negatives.push(['approved sibling pin cannot authenticate unchanged verifier bytes', x =>
          changeDefinition(x, text => text.replace(producerHelperDigest, alternateDigest))])
        negatives.push(['approved pin cannot authenticate sibling verifier bytes', x => {
          x.routes[`contents/scripts/main-release-admission.py?ref=${head}`].content = alternate.toString('base64')
        }])
      }
      const platformHelper = fs.readFileSync(path.join(__dirname, 'fixtures/main-authority-admission-v5.py'))
      const foreignHelpers = current ? [alternate, platformHelper,
        fs.readFileSync(path.join(__dirname, 'fixtures/main-authority-admission-v4.py')),
        fs.readFileSync(path.join(__dirname, 'fixtures/main-authority-admission-v6.py'))]
        : producer === 'v6' ? [helper, alternate, platformHelper]
        : producer === 'v5' ? [helper, alternate,
          fs.readFileSync(path.join(__dirname, 'fixtures/main-authority-admission-v4.py'))] : [helper, platformHelper]
      for (const body of foreignHelpers) negatives.push(['verifier digest cannot cross its authenticated workflow family: ' +
        createHash('sha256').update(body).digest('hex'), x => {
        const digest = createHash('sha256').update(body).digest('hex')
        changeDefinition(x, text => text.replace(producerHelperDigest, digest))
        x.routes[`contents/scripts/main-release-admission.py?ref=${head}`].content = body.toString('base64')
      }])
      if (current) negatives.push(['unpublished prior current pin is not a historical producer', x =>
        changeDefinition(x, text => text.replace(producerHelperDigest,
          'cf8c4f01856f32da8fd5f3d824cbe367a458b8e38d4feac85107b84ab6f864ef'))])
    }
    if (mode === 'publication') {
      negatives.push(['light main admission cannot hide a full test failure', x => {
        x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs.find(job => job.name === 'Lint').conclusion = 'failure'
      }])
      negatives.push(['unknown main definition cannot claim admission', x => changeDefinition(x, text => `${text}\n# foreign main\n`, source)])
      negatives.push(['main admission attempt membership differs', x => {
        x.routes['actions/runs/111/attempts/1/jobs?per_page=10&page=1'].jobs.find(job => job.name === 'Main Release Admission').run_attempt = 9
      }])
      negatives.push(['mixed literal and expanded skipped matrix is ambiguous', x => {
        const jobs = x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs
        jobs.push({ ...copy(jobs.at(-1)), id: 99999, name: platforms[0] })
      }])
      negatives.push(['partial expanded skipped matrix cannot hide a platform', x => {
        expandedSkipped(x)
        const jobs = x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs
        jobs.splice(jobs.findIndex(job => job.name === platforms.at(-1)), 1)
      }])
      if (serverPartitioned) for (const label of ['Test', 'E2E']) {
        negatives.push([`${label} skipped matrix must use the observed jobs API literal`, x => {
          const jobs = x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs
          jobs.find(job => job.name.startsWith(`${label} (`)).name =
            label + " (${{ matrix.crate }}${{ matrix.server_block && format('/{0}', matrix.server_block) || '' }})"
        }])
        negatives.push([`${label} partial expanded skipped matrix cannot hide a server block`, x => {
          expandedSkipped(x)
          const jobs = x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs
          jobs.splice(jobs.findIndex(job => job.name === `${label} (bamboo-server/${blocksByLabel[label][0]})`), 1)
        }])
        negatives.push([`${label} literal and expanded skipped native jobs cannot be mixed`, x => {
          const jobs = x.routes['actions/runs/111/jobs?filter=latest&per_page=10&page=1'].jobs
          jobs.push({ ...copy(jobs.at(-1)), id: 99999, name: `${label} (bamboo-server/${blocksByLabel[label][0]})` })
        }])
      }
    } else {
      for (const [name, change] of [
        ['main push payload changed source', x => { x.event.after = foreign }],
        ['main push payload changed ref', x => { x.event.ref = 'refs/heads/dev' }],
        ['main push payload changed repository', x => { x.event.repository.full_name = 'foreign/repo' }],
      ]) negatives.push([name, change])
    }
  }
  for (const name of platforms) negatives.push([`pre-merge CI cannot skip ${name}`, x => {
    x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs.find(job => job.name === name).conclusion = 'skipped'
  }])
  if (crates) {
    const latest = 'actions/runs/222/jobs?filter=latest&per_page=10&page=1'
    const attempt = 'actions/runs/222/attempts/1/jobs?per_page=10&page=1'
    negatives.push(['platform labels must match the authenticated producer', x => {
      for (const route of [latest, attempt]) for (const job of x.routes[route].jobs) {
        if (platforms.includes(job.name)) job.name = job.name.replace(platformLabel, platformLabel === 'Platform Tests' ? 'Build' : 'Platform Tests')
      }
    }])
    for (const [name, change] of [
      ['missing platform', list => list.splice(list.findIndex(job => job.name === platforms[0]), 1)],
      ['duplicate platform', list => list.push(copy(list.find(job => job.name === platforms[0])))],
      ['failed platform', list => { list.find(job => job.name === platforms[0]).conclusion = 'failure' }],
      ['wrong platform source', list => { list.find(job => job.name === platforms[0]).head_sha = foreign }],
      ['late platform completion', list => { list.find(job => job.name === platforms[0]).completed_at = pull.merged_at }],
    ]) negatives.push([name, x => change(x.routes[latest].jobs)])
    negatives.push(['platform must belong to its original attempt', x => {
      const list = x.routes[attempt].jobs
      list.splice(list.findIndex(job => job.name === platforms[0]), 1)
    }])
  }
  const suite = x => x.routes.graphql.data.node
  const sourceJob = x => x.routes['actions/runs/222/jobs?filter=latest&per_page=10&page=1'].jobs[0]
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
    ['job absent from its attempt', x => { x.routes['actions/runs/222/attempts/1/jobs?per_page=10&page=1'].jobs.shift() }],
    ['duplicate source job in attempt', x => { x.routes['actions/runs/222/attempts/1/jobs?per_page=10&page=1'].jobs.push(copy(sourceJob(x))) }],
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
  for (const [name, change] of negatives.filter(([name]) => !(mode === 'main' &&
    /^(server main CI|stale delivered main CI|wrong CI workflow|foreign main CI|main CI |empty association rejects main)/.test(name)))) await t.test(name, () => {
    const result = run(change)
    assert.notEqual(result.status, 0, `${name}: ${result.stdout}`)
    assert.equal(result.output, '', 'No untrusted source may reach checkout/credentials/reservation/upload')
  })
  for (const [name, environment] of mode === 'publication' ? [
    ['manual complete dispatch stays forbidden', { GITHUB_EVENT_NAME: 'workflow_dispatch' }],
    ['wrong workflow controller ref', { GITHUB_REF: 'refs/heads/main' }],
    ['unprotected workflow controller', { GITHUB_REF_PROTECTED: 'false' }],
    ['controller outside protected dev history', { GITHUB_WORKFLOW_SHA: foreign }],
    ['API transport fails closed', { FIXTURE_API_FAILURE: 'true' }],
  ] : [
    ['wrong main event', { GITHUB_EVENT_NAME: 'workflow_dispatch' }],
    ['wrong main controller ref', { GITHUB_REF: 'refs/heads/dev' }],
    ['main workflow/source mismatch', { GITHUB_WORKFLOW_SHA: controller }],
    ['main source admission failure', { SOURCE_RESULT: 'failure' }],
    ['main cannot claim full mode', { FULL_VALIDATION: 'true' }],
    ['wrong main verifier pin', { ADMISSION_HELPER_SHA256: 'f'.repeat(64) }],
    ['API transport fails closed', { FIXTURE_API_FAILURE: 'true' }],
  ]) await t.test(name, () => {
    const result = run(undefined, environment)
    assert.notEqual(result.status, 0)
    assert.equal(result.output, '')
  })
}
module.exports = { verify }
