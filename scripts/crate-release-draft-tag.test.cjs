const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawnSync } = require('node:child_process')
const { test } = require('node:test')
const controller = require('./crate-release.cjs')

function executable(directory, name, body) {
  fs.writeFileSync(path.join(directory, name), `#!${process.execPath}\n${body}\n`, { mode: 0o700 })
}

test('draft tag survives real receipt PATCH, same-reservation resume and attachment upload', async (t) => {
  // Exercise production adapters with an isolated CLI/API fixture, never Cargo or network.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-draft-tag-'))
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }))
  const bin = path.join(directory, 'bin')
  fs.mkdirSync(bin)
  const source = 'a'.repeat(40)
  const key = '11'.repeat(32)
  const version = '2026.10.11'
  const stateFile = path.join(directory, 'state.json')
  const pythonResult = spawnSync('python3', ['-c', 'import sys; print(sys.executable)'], { encoding: 'utf8' })
  assert.equal(pythonResult.status, 0, pythonResult.stderr)
  const python = pythonResult.stdout.trim()
  executable(bin, 'git', `if (process.argv[2] !== 'rev-parse') throw new Error('Unexpected git command'); process.stdout.write('${source}\\n')`)
  executable(bin, 'python3', `
const { spawnSync } = require('node:child_process')
const args = process.argv.slice(2)
if (args[0] === 'scripts/compute-publish-order.py') process.stdout.write('bamboo-agent\\n')
else if (args[0].endsWith('/crate-release-frontend.py')) {
  const result = spawnSync(process.env.FIXTURE_PYTHON, args, { stdio: 'inherit' })
  process.exit(result.status ?? 1)
} else throw new Error('No Cargo, registry or unexpected Python work is permitted')
`)
  executable(bin, 'gh', `
const fs = require('node:fs')
const path = require('node:path')
const args = process.argv.slice(2)
const statePath = process.env.FIXTURE_STATE
const state = JSON.parse(fs.readFileSync(statePath))
const route = args.find(value => value.startsWith('repos/'))
const save = () => fs.writeFileSync(statePath, JSON.stringify(state))
if (args[0] === 'api' && route?.endsWith('/releases?per_page=100')) {
  process.stdout.write(JSON.stringify([[state.release]]))
} else if (args[0] === 'api' && route?.endsWith('/tags?per_page=100')) {
  process.stdout.write(JSON.stringify([[{name: state.release.tag_name}]]))
} else if (args[0] === 'api' && route?.includes('/git/ref/tags/')) {
  process.stdout.write(JSON.stringify({object: {type: 'commit', sha: state.source}}))
} else if (args[0] === 'api' && route?.endsWith('/releases/42')) {
  if (args.includes('PATCH')) {
    const payload = JSON.parse(fs.readFileSync(0, 'utf8'))
    // The GitHub contract observed in the failed publication: omitted tag removes it.
    state.release.tag_name = payload.tag_name || 'untagged-fixture'
    if (Object.hasOwn(payload, 'body')) state.release.body = payload.body
    if (Object.hasOwn(payload, 'draft')) state.release.draft = payload.draft
    state.patches.push(payload)
    save()
  }
  process.stdout.write(JSON.stringify(state.release))
} else if (args[0] === 'api' && route?.includes('/releases/assets/')) {
  const id = route.split('/').at(-1)
  process.stdout.write(Buffer.from(state.bytes[id], 'base64'))
} else if (args[0] === 'release' && args[1] === 'upload') {
  if (state.release.tag_name !== args[2]) throw new Error('release not found')
  const files = args.slice(3, args.indexOf('--clobber'))
  for (const file of files) {
    const name = path.basename(file)
    const asset = state.release.assets.find(entry => entry.name === name) || {id: state.nextAsset++, name}
    if (!state.release.assets.includes(asset)) state.release.assets.push(asset)
    state.bytes[asset.id] = fs.readFileSync(file).toString('base64')
  }
  state.uploads.push({release_id: state.release.id, tag: args[2], names: files.map(file => path.basename(file))})
  save()
} else if (args[0] === 'api' && route?.endsWith('/releases')) {
  throw new Error('A second reservation must never be created')
} else throw new Error('Unexpected CLI request; real network access is forbidden')
`)
  const frontend = path.join(directory, 'crates/app/bamboo-server/frontend_package')
  fs.mkdirSync(frontend, { recursive: true })
  fs.mkdirSync(path.join(directory, 'scripts'))
  const manifest = { frontend_version: '2026.10.12', bundle_hash: 'sha256:' + 'c'.repeat(64),
    built_at: '2026-10-13T00:00:00.000Z' }
  fs.writeFileSync(path.join(frontend, 'frontend-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`)
  const zip = spawnSync(python, ['-c',
    'from pathlib import Path\nfrom zipfile import ZipFile\nimport sys\np=Path(sys.argv[1])\n' +
    'with ZipFile(p/"lotus-frontend.zip","w") as z:\n z.writestr("index.html",b"fixture")\n z.write(p/"frontend-manifest.json","frontend-manifest.json")', frontend], { encoding: 'utf8' })
  assert.equal(zip.status, 0, zip.stderr)
  fs.writeFileSync(path.join(directory, 'scripts/frontend-package-lock.json'), JSON.stringify({ sourceRevision: 'b'.repeat(40) }))
  const eventPath = path.join(directory, 'event.json')
  fs.writeFileSync(eventPath, JSON.stringify({ workflow_run: { name: 'CI', event: 'push', conclusion: 'success',
    head_branch: 'main', head_sha: source, html_url: 'https://example.invalid/ci',
    repository: { full_name: 'bigduu/Bamboo-agent' }, head_repository: { full_name: 'bigduu/Bamboo-agent' } } }))
  const previousEnv = process.env
  const previousCwd = process.cwd()
  try {
    process.chdir(directory)
    process.env = { PATH: bin, FIXTURE_STATE: stateFile, FIXTURE_PYTHON: python }
    const context = controller.makeContext({
      GITHUB_REPOSITORY: 'bigduu/Bamboo-agent', GITHUB_EVENT_PATH: eventPath, GITHUB_EVENT_NAME: 'workflow_run',
      GITHUB_RUN_ID: 'draft-fixture', RUNNER_TEMP: directory, CARGO_REGISTRY_TOKEN: 'fixture-only',
      BAMBOO_RELEASE_SIGNING_KEY: key, BAMBOO_RELEASE_SIGNING_KEY_SHA256: controller.sha256(Buffer.from(key, 'hex')),
      FRONTEND_PACKAGE: '@bigduu/lotus-next', FRONTEND_VERSION: '2026.10.12',
    })
    const initial = controller.signReceipt({ schemaVersion: 1, version, identity: context.identity,
      crates: context.crates, frontendBytes: context.frontendBytes, automatic: true, completed: false,
      ciRun: context.ciRun, packageChecksums: {} }, key)
    const originalBody = controller.receiptBody(initial)
    fs.writeFileSync(stateFile, JSON.stringify({ source, release: { id: 42, tag_name: 'v' + version,
      target_commitish: source, draft: true, body: originalBody, assets: [] }, patches: [], uploads: [], bytes: {}, nextAsset: 1 }))
    context.versions = async () => []
    context.registryFrontier = async () => []
    const first = await controller.plan(context)
    assert.equal(first.release.id, 42)
    assert.equal(first.receipt.version, version)
    let state = JSON.parse(fs.readFileSync(stateFile))
    assert.equal(state.release.body, originalBody, 'Equivalent same-reservation resume preserves authenticated body')
    assert.equal(state.release.tag_name, 'v' + version)
    assert.equal(state.release.target_commitish, source)
    assert.deepEqual(state.uploads, [{release_id: 42, tag: 'v' + version, names: ['frontend-manifest.json', 'lotus-frontend.zip']}])
    const second = await controller.plan(context)
    assert.equal(second.release.id, 42)
    assert.deepEqual(second.receipt, initial)
    state = JSON.parse(fs.readFileSync(stateFile))
    assert.equal(state.uploads.length, 1, 'Existing original attachments are restored, not overwritten')
    assert.equal(state.patches.length, 1)
    const published = new Map()
    context.registry = async name => published.get(name) || null
    context.package = async () => 'd'.repeat(64)
    context.cargoPublish = async name => { published.set(name, {checksum: 'd'.repeat(64)}); return {status: 0, output: ''} }
    context.verifyArchive = async () => {}
    await controller.publish(context, second.release, second.receipt)
    state = JSON.parse(fs.readFileSync(stateFile))
    assert.equal(state.release.id, 42)
    assert.equal(state.release.tag_name, 'v' + version)
    assert.equal(state.release.target_commitish, source)
    assert.equal(state.release.draft, false)
    assert.equal(state.uploads.at(-1).release_id, 42)
    assert.deepEqual(state.uploads.at(-1).names, ['release-provenance.json'])
    const receipt = controller.authenticateReceipt(controller.readBodyReceipt(state.release), key)
    assert.equal(receipt.completed, true)
    assert.equal(receipt.version, version)
    assert.deepEqual(receipt.identity, initial.identity)
    assert.deepEqual(receipt.packageChecksums, {'bamboo-agent': 'd'.repeat(64)})
  } finally {
    process.env = previousEnv
    process.chdir(previousCwd)
  }
})

test('accepted workflow controller loads outside the exact product source and fails closed on mismatched bytes/source', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-controller-loader-'))
  try {
    const workflow = fs.readFileSync(path.join(__dirname, '../.github/workflows/publish-crate.yml'), 'utf8')
    const block = workflow.match(/python3 - <<'PYCONTROLLER'\n([\s\S]+?)          PYCONTROLLER/)
    assert.ok(block)
    const bootstrap = block[1].replace(/^ {10}/gm, '')
    const product = path.join(directory, 'product')
    const bin = path.join(directory, 'bin')
    fs.mkdirSync(product)
    fs.mkdirSync(bin)
    fs.writeFileSync(path.join(product, 'Cargo.toml'), 'immutable product fixture\n')
    const productSource = 'a'.repeat(40)
    const controllerSource = 'b'.repeat(40)
    const files = {}
    for (const name of ['crate-release.cjs', 'crate-release-frontend.py']) {
      const bytes = fs.readFileSync(path.join(__dirname, name))
      files[name] = {type: 'file', path: 'scripts/' + name, encoding: 'base64', content: bytes.toString('base64'),
        sha: crypto.createHash('sha1').update(Buffer.concat([Buffer.from(`blob ${bytes.length}\0`), bytes])).digest('hex')}
    }
    const payload = path.join(directory, 'files.json')
    fs.writeFileSync(payload, JSON.stringify(files))
    executable(bin, 'gh', `
const fs = require('node:fs')
const route = process.argv[3]
if (!route.endsWith('?ref=' + process.env.CONTROLLER_REVISION)) throw new Error('Moving or product ref requested')
const name = route.split('/').at(-1).split('?')[0]
const data = JSON.parse(fs.readFileSync(process.env.FIXTURE_FILES))[name]
if (process.env.FIXTURE_BAD_BLOB === 'true') data.sha = '0'.repeat(40)
process.stdout.write(JSON.stringify(data))
`)
    executable(bin, 'git', `
if (process.argv[2] === 'rev-parse') process.stdout.write(process.env.SOURCE_REVISION + '\\n')
else if (process.argv[2] === 'status') process.stdout.write(process.env.FIXTURE_DIRTY === 'true' ? ' M Cargo.toml\\n' : '')
else throw new Error('Unexpected git command')
`)
    for (const [name, overrides, succeeds] of [
      ['accepted', {}, true],
      ['wrong-controller', {GITHUB_WORKFLOW_SHA: 'c'.repeat(40)}, false],
      ['wrong-blob', {FIXTURE_BAD_BLOB: 'true'}, false],
      ['dirty-product', {FIXTURE_DIRTY: 'true'}, false],
    ]) {
      const runner = path.join(directory, name)
      fs.mkdirSync(runner)
      const output = path.join(runner, 'outputs')
      fs.writeFileSync(output, '')
      const result = spawnSync('python3', ['-c', bootstrap], {cwd: product, encoding: 'utf8', env: {
        PATH: `${bin}:${process.env.PATH}`, GITHUB_REPOSITORY: 'bigduu/Bamboo-agent', CONTROLLER_REVISION: controllerSource,
        GITHUB_WORKFLOW_SHA: controllerSource, SOURCE_REVISION: productSource, RUNNER_TEMP: runner,
        GITHUB_OUTPUT: output, FIXTURE_FILES: payload, ...overrides,
      }})
      assert.equal(result.status === 0, succeeds, result.stderr)
      if (succeeds) {
        assert.equal(fs.readFileSync(output, 'utf8'), `path=${runner}/bamboo-release-controller/crate-release.cjs\nrevision=${controllerSource}\n`)
        for (const file of Object.keys(files)) assert.deepEqual(fs.readFileSync(path.join(runner, 'bamboo-release-controller', file)), fs.readFileSync(path.join(__dirname, file)))
      } else assert.equal(fs.readFileSync(output, 'utf8'), '')
      assert.equal(fs.readFileSync(path.join(product, 'Cargo.toml'), 'utf8'), 'immutable product fixture\n')
      assert.deepEqual(fs.readdirSync(product), ['Cargo.toml'])
    }
    assert.match(workflow, /run: node "\$\{\{ steps.controller.outputs.path \}\}" plan/)
    assert.match(workflow, /run: node "\$\{\{ steps.controller.outputs.path \}\}" publish/)
  } finally {
    fs.rmSync(directory, {recursive: true, force: true})
  }
})
