const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { test } = require('node:test')
const core = require('./crate-release.cjs')
const recovery = require('./crate-release-recover.cjs')
const { TARGET } = recovery
const clone = value => JSON.parse(JSON.stringify(value))
const controller = 'b'.repeat(40)
const key = '11'.repeat(32)
const env = { GITHUB_REPOSITORY: TARGET.repository, GITHUB_EVENT_NAME: 'workflow_dispatch',
  GITHUB_REF: 'refs/heads/dev', GITHUB_REF_TYPE: 'branch', GITHUB_REF_PROTECTED: 'true',
  GITHUB_WORKFLOW_REF: `${TARGET.repository}/.github/workflows/recover-crate-release.yml@refs/heads/dev`,
  GITHUB_SHA: controller, GITHUB_WORKFLOW_SHA: controller, EXPECTED_CONTROLLER_SHA: controller }
const run = { id: TARGET.ciRunId, name: 'CI', path: '.github/workflows/ci.yml', status: 'completed',
  conclusion: 'success', event: 'push', head_branch: 'main', head_sha: TARGET.source,
  repository: {full_name: TARGET.repository}, head_repository: {full_name: TARGET.repository}, html_url: TARGET.ciUrl }

test('existing recovery separates controller/product source and requires the actual successful main CI', () => {
  const requests = []
  const read = route => {
    requests.push(route)
    if (route.startsWith('actions/runs/')) return clone(run)
    if (route.startsWith('branches/')) { const name = route.split('/')[1]; return {name, protected: true, commit: {sha: controller}} }
    if (route.startsWith('compare/')) return {status: 'ahead', merge_base_commit: {sha: route.split('/')[1].split('...')[0]}}
    throw new Error('Unexpected request')
  }
  assert.equal(recovery.authorize(env, read, TARGET.source).head_sha, TARGET.source)
  assert.ok(requests.includes(`actions/runs/${TARGET.ciRunId}`))
  assert.notEqual(controller, TARGET.source)
  for (const [field, value] of [['id', 1], ['name', 'Other'], ['path', '.github/workflows/other.yml'],
    ['status', 'in_progress'], ['conclusion', 'failure'], ['event', 'pull_request'], ['head_branch', 'dev'],
    ['head_sha', controller], ['repository', {full_name: 'fork/repo'}], ['head_repository', {full_name: 'fork/repo'}]]) {
    assert.throws(() => recovery.authorize(env, route => route.startsWith('actions/runs/') ? {...clone(run), [field]: value} : read(route), TARGET.source))
  }
  for (const overrides of [{GITHUB_REF_PROTECTED: 'false'}, {GITHUB_REF: 'refs/heads/feature'},
    {GITHUB_WORKFLOW_SHA: 'c'.repeat(40)}, {GITHUB_SHA: TARGET.source}, {GITHUB_EVENT_NAME: 'workflow_run'}]) {
    assert.throws(() => recovery.authorize({...env, ...overrides}, read, TARGET.source))
  }
  assert.throws(() => recovery.authorize(env, read, controller), /original main CI source/)
  assert.throws(() => recovery.authorize(env, () => { throw new Error('HTTP 403') }, TARGET.source), /HTTP 403/)
  assert.throws(() => recovery.authorize(env, route => route.startsWith('branches/')
    ? {name: route.split('/')[1], protected: false, commit: {sha: controller}} : read(route), TARGET.source))
})

function fixture() {
  const identity = {repository: TARGET.repository, sourceRevision: TARGET.source,
    frontend: {packageName: '@bigduu/lotus-next', packageVersion: '2026.10.12', bundleHash: 'sha256:' + 'c'.repeat(64), lock: {sourceRevision: 'd'.repeat(40)}}}
  const crates = ['bamboo-agent']
  const receipt = core.signReceipt({schemaVersion: 1, version: TARGET.version, identity, crates,
    frontendBytes: {manifestSha256: 'e'.repeat(64), archiveSha256: 'f'.repeat(64)},
    automatic: true, completed: false, ciRun: TARGET.ciUrl, packageChecksums: {}}, key)
  const release = {id: TARGET.releaseId, tag_name: `v${TARGET.version}`, target_commitish: TARGET.source,
    draft: true, prerelease: false, body: core.receiptBody(receipt)}
  const writes = []
  const registry = new Map()
  let uploads = 0
  let completions = 0
  const context = {automatic: true, identity, crates, frontendBytes: receipt.frontendBytes,
    releases: async () => [clone(release)], tags: async () => [TARGET.version], versions: async () => [],
    assertSigningKey: () => {}, readReceipt: async (entry, options) => core.readAuthenticatedReceipt(entry, key, options),
    verifyReceipt: value => core.authenticateReceipt(value, key), tagSource: async () => TARGET.source,
    registryFrontier: async () => [], isAncestor: async () => false, ensureFrontend: async () => {},
    saveReceipt: async (_entry, value) => { core.signReceipt(value, key); release.body = core.receiptBody(value); writes.push('receipt') },
    registry: async name => registry.get(name), package: async () => '9'.repeat(64),
    cargoPublish: async name => { uploads++; registry.set(name, {checksum: '9'.repeat(64)}); return {status: 0, output: ''} },
    verifyArchive: async () => {}, wait: async () => {},
    complete: async () => { completions++; release.draft = false },
  }
  const read = route => {
    if (route === `releases/${TARGET.releaseId}`) return clone(release)
    if (route === `git/ref/tags/v${TARGET.version}`) return {object: {type: 'commit', sha: TARGET.source}}
    throw new Error('Unexpected request; no reservation/tag write is permitted')
  }
  return {context, receipt, release, writes, read, counts: () => ({uploads, completions})}
}

test('recovery mismatched draft identity, authentication or existing tag fails before any mutation', async () => {
  for (const change of [entry => {entry.id++}, entry => {entry.tag_name = 'v2026.10.12'},
    entry => {entry.target_commitish = controller}, entry => {entry.prerelease = true},
    entry => {entry.body = ''}, entry => {entry.body = entry.body.replace(/"mac":"[0-9a-f]+"/, '"mac":"' + '0'.repeat(64) + '"')}]) {
    const f = fixture()
    change(f.release)
    await assert.rejects(recovery.recoveryPlan(f.context, f.read))
    assert.deepEqual(f.writes, [])
    assert.deepEqual(f.counts(), {uploads: 0, completions: 0})
  }
  const f = fixture()
  await assert.rejects(recovery.recoveryPlan(f.context, route => route.startsWith('git/ref/')
    ? {object: {type: 'commit', sha: controller}} : f.read(route)))
  assert.deepEqual(f.writes, [])
  await assert.rejects(recovery.recoveryPlan(f.context, () => {throw new Error('HTTP 404')}), /HTTP 404/)
  await assert.rejects(f.context.reserve({}), /cannot create/)
  const misplaced = fixture()
  misplaced.context.releases = async () => [{...clone(misplaced.release), id: TARGET.releaseId + 1}]
  await assert.rejects(recovery.recoveryPlan(misplaced.context, misplaced.read), /Unexpected reservation ID/)
  assert.deepEqual(misplaced.writes, [])
})

test('repeated recovery reuses the original reservation and completed reruns verify without uploads', async () => {
  const f = fixture()
  const first = await recovery.recoveryPlan(f.context, f.read)
  assert.equal(first.release.id, TARGET.releaseId)
  assert.deepEqual(first.receipt, f.receipt)
  await recovery.recoveryPublish(f.context, first.receipt, f.read)
  const counts = f.counts()
  assert.deepEqual(counts, {uploads: 1, completions: 1})
  const repeated = await recovery.recoveryPlan(f.context, f.read)
  await recovery.recoveryPublish(f.context, repeated.receipt, f.read)
  assert.deepEqual(f.counts(), counts)
  assert.equal(repeated.receipt.version, TARGET.version)
  const changed = clone(repeated.receipt)
  changed.frontendBytes.archiveSha256 = '8'.repeat(64)
  await assert.rejects(recovery.recoveryPublish(f.context, changed, f.read), /planned receipt changed/)
})

test('recovery workflow keeps the shared queue, original product and existing credential scopes', () => {
  const source = fs.readFileSync(path.join(__dirname, '../.github/workflows/recover-crate-release.yml'), 'utf8')
  assert.match(source, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.match(source, /environment: bamboo-release\n    permissions:\n      contents: write/)
  assert.match(source, /ref: \$\{\{ inputs.expected_controller_sha \}\}\n          path: controller/)
  assert.match(source, new RegExp(`ref: ${TARGET.source}\\n          path: product`))
  assert.equal([...source.matchAll(/secrets.BAMBOO_RELEASE_TOKEN/g)].length, 2)
  assert.equal([...source.matchAll(/secrets.BAMBOO_RELEASE_SIGNING_KEY/g)].length, 2)
  assert.equal([...source.matchAll(/secrets.CARGO_REGISTRY_TOKEN/g)].length, 2)
  assert.ok(source.indexOf(' authorize') < source.indexOf('secrets.BAMBOO_RELEASE_TOKEN'))
  assert.doesNotMatch(source, /GITHUB_EVENT_NAME:|GITHUB_SHA:|actions: write|packages: write|workflow_run:/)
  assert.doesNotMatch(source, /inputs\.(version|expected_source_sha|release_id|ci_run_id)/)
})
