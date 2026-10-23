const assert = require('node:assert/strict')
const test = require('node:test')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const { spawnSync } = require('node:child_process')
const { pathToFileURL } = require('node:url')
const implementation = require('./sidecar-release.cjs')
const legacySuite = process.env.BAMBOO_TEST_EXECUTABLE_CONTRACT === '1'
const testContractVersion = legacySuite ? 1 : 2
const testContract = implementation.contractForVersion(testContractVersion)
// Run the same production functions with the version selected by the actual
// source-bound entrypoint. The legacy child retains the complete original suite.
const release = { ...implementation, TARGETS: testContract.targets,
  MANIFEST: testContract.manifest, PROVENANCE: testContract.provenance,
  createManifest: value => legacySuite ? JSON.parse(implementation.manifestBytes(value, 1)) : implementation.createManifest(value),
  manifestBytes: (value, version = testContractVersion) => implementation.manifestBytes(value, version),
  expectedBuildBindings: value => implementation.expectedBuildBindings(value, testContractVersion),
  assembleRelease: value => implementation.assembleRelease({ contractVersion: testContractVersion, ...value }),
  recoverPublishedRelease: value => implementation.recoverPublishedRelease({ contractVersion: testContractVersion, ...value }),
  publishAssets: value => implementation.publishAssets({ contractVersion: testContractVersion, ...value }),
}
const { nativeTarget, buildEnvironment, verifyBuildMessages, verifyStampedVersions,
  nativeCachePolicy, evictWorkspaceArtifacts } = require('./sidecar-build.cjs')
const { signReceipt, receiptBody, createPreparation } = require('./crate-release.cjs')

const clone = (value) => JSON.parse(JSON.stringify(value))
const identity = { repository: release.REPOSITORY, version: '2026.10.123', sourceRevision: '1'.repeat(40),
  sourceTree: '2'.repeat(40), workflowRevision: '3'.repeat(40), runId: '100', runAttempt: 2,
  ciRun: 'https://github.com/bigduu/Bamboo-agent/actions/runs/90' }
const frontendBytes = { manifestSha256: '4'.repeat(64), archiveSha256: '5'.repeat(64) }
const signingKey = 'a'.repeat(64) // Synthetic test authority only.

function temporary(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-executables-test-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  return root
}

test('native version verification accepts the real stamped workspace with its independent example version', (t) => {
  const root = temporary(t)
  // Exercise the production stamper against copies of the real manifests.
  // Reading their effective versions models metadata only; this invokes no Cargo.
  const script = String.raw`
import json, pathlib, shutil, subprocess, sys, tomllib
source, root, version = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
workspace = tomllib.loads((source / 'Cargo.toml').read_text())['workspace']
for member in workspace['members']:
    relative = pathlib.Path(member) / 'Cargo.toml'
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source / relative, destination)
subprocess.run([sys.executable, str(source / 'scripts/crate-release-lock.py'), 'stamp', version], cwd=root, check=True)
inherited = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']['package']['version']
packages = []
for member in workspace['members']:
    manifest = root / member / 'Cargo.toml'
    package = tomllib.loads(manifest.read_text())['package']
    value = package['version']
    packages.append({'id': package['name'], 'name': package['name'],
                     'version': inherited if isinstance(value, dict) and value.get('workspace') else value,
                     'manifest_path': str(manifest.resolve())})
managed = [root / 'Cargo.toml', *sorted((root / 'crates').glob('*/*/Cargo.toml'))]
print(json.dumps({'packages': packages, 'workspace_members': [p['id'] for p in packages],
                  'managedManifests': [p.relative_to(root).as_posix() for p in managed]}))
`
  const result = spawnSync(process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3'),
    ['-c', script, process.cwd(), root, identity.version], { encoding: 'utf8', env: {
      PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1' } })
  assert.equal(result.status, 0, result.stderr)
  const metadata = JSON.parse(result.stdout)
  const managed = spawnSync(process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3'),
    [path.resolve('scripts/crate-release-lock.py'), 'manifests'], { cwd: root, encoding: 'utf8', env: {
      PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1' } })
  assert.equal(managed.status, 0, managed.stderr)
  assert.deepEqual(JSON.parse(managed.stdout), metadata.managedManifests, 'Use exactly the existing stamper package set')
  const example = metadata.packages.find((p) => p.name === 'bamboo-tool-event-recorder-example')
  assert.equal(example.version, '0.1.0')
  const options = { root, manifestPaths: metadata.managedManifests }
  const server = verifyStampedVersions(metadata, identity.version, options)
  assert.equal(server.name, 'bamboo-server')
  assert.equal(server.version, identity.version)
  assert.equal(metadata.packages.find((p) => p.name === 'bamboo-agent').version, identity.version)
  assert.equal(example.version, '0.1.0', 'Do not stamp the independent example just to satisfy a release guard')
  for (const manifest of metadata.managedManifests) {
    const index = metadata.packages.findIndex((p) => p.manifest_path === path.resolve(fs.realpathSync(root), manifest))
    assert.ok(index >= 0)
    const wrong = clone(metadata); wrong.packages[index].version = '0.0.0'
    assert.throws(() => verifyStampedVersions(wrong, identity.version, options))
    const missing = clone(metadata); missing.packages.splice(index, 1)
    assert.throws(() => verifyStampedVersions(missing, identity.version, options))
    const repeated = clone(metadata); repeated.packages.push(clone(metadata.packages[index]))
    assert.throws(() => verifyStampedVersions(repeated, identity.version, options))
  }
  for (const required of ['Cargo.toml', 'crates/app/bamboo-server/Cargo.toml']) {
    assert.throws(() => verifyStampedVersions(metadata, identity.version, {
      ...options, manifestPaths: options.manifestPaths.filter((p) => p !== required) }))
  }
  for (const extra of ['../Cargo.toml', metadata.managedManifests[0]]) {
    assert.throws(() => verifyStampedVersions(metadata, identity.version, {
      ...options, manifestPaths: [...options.manifestPaths, extra] }))
  }
})

test('the shipped manual page allows HTTPS or loopback HTTP and rejects other origins before fetching', async () => {
  const page = pathToFileURL(path.resolve('crates/infra/bamboo-skills/manual/site/app.mjs'))
  const descriptors = new Map(['location', 'document', 'fetch'].map((name) =>
    [name, Object.getOwnPropertyDescriptor(globalThis, name)]))
  const cases = [
    ['https://docs.example.test/manual/', true],
    ['http://localhost:4173/manual/', true],
    ['http://127.0.0.1:4173/manual/', true],
    ['http://[::1]:4173/manual/', true],
    ['http://example.test/manual/', false],
    ['http://localhost.example.test/manual/', false],
    ['http://127.0.0.1.example.test/manual/', false],
    ['http://localhost@example.test/manual/', false],
    ['http://0.0.0.0:4173/manual/', false],
    ['file:///manual/index.html', false],
    ['data:text/html,manual', false],
    ['ftp://localhost/manual/', false],
  ]
  try {
    for (const [index, [url, allowed]] of cases.entries()) {
      let requests = 0
      const identity = { textContent: '' }
      Object.assign(globalThis, {
        location: new URL(url),
        document: { getElementById: (id) => { assert.equal(id, 'identity'); return identity } },
        // Stop at the resource boundary. Never make a real HTTP/model request.
        fetch: async () => { requests++; throw new Error('manual-origin fixture stops before any network') },
      })
      await import(`${page.href}?manual-origin-case=${index}`)
      assert.equal(requests, allowed ? 2 : 0, url)
      assert.match(identity.textContent, /Manual unavailable or failed integrity verification/)
    }
  } finally {
    for (const [name, descriptor] of descriptors) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor)
      else delete globalThis[name]
    }
  }
})

// Synthetic format fixtures only. They are never executed, installed as a real
// product, or offered to the CLI publication entrypoint.
function header(target) {
  const bytes = Buffer.alloc(65536)
  if (target.includes('apple')) {
    bytes.writeUInt32LE(0xfeedfacf, 0)
    bytes.writeUInt32LE(target.startsWith('aarch64') ? 0x0100000c : 0x01000007, 4)
    bytes.writeUInt32LE(2, 12)
  } else if (target.includes('windows')) {
    bytes.write('MZ'); bytes.writeUInt32LE(128, 60)
    bytes.writeUInt32LE(0x4550, 128); bytes.writeUInt16LE(0x8664, 132)
    bytes.writeUInt16LE(2, 150); bytes.writeUInt16LE(0x20b, 152)
  } else {
    Buffer.from('7f454c460201', 'hex').copy(bytes)
    bytes.writeUInt16LE(3, 16); bytes.writeUInt16LE(62, 18)
  }
  return bytes
}

function receipt(target, bytes) {
  return { schemaVersion: 1, ...identity, runAttempt: 1, target, buildMode: 'embedded', embeddedFrontend: true,
    format: 'raw', assetName: release.artifactName(target), sha256: release.sha256(bytes), sizeBytes: bytes.length,
    command: ['cargo', 'build', '--locked', '--release', '--verbose', '--bin', 'bamboo', '--target', target,
      '--message-format=json-render-diagnostics'], runtime: { version: `bamboo ${identity.version}`, status: 'passed' },
    rustc: 'rustc fixture (not executed)', cargo: 'cargo fixture (not executed)',
    cargoMessagesSha256: '6'.repeat(64), stampedDiffSha256: '7'.repeat(64), frontendBytes }
}

function inputs(t, { targets = release.TARGETS, contractVersion = testContractVersion } = {}) {
  const root = temporary(t)
  const sourceRoot = path.join(root, 'source'), inputDirectory = path.join(root, 'inputs'), outputDirectory = path.join(root, 'output')
  for (const dir of [sourceRoot, inputDirectory, outputDirectory, path.join(sourceRoot, 'browser-runtime')]) fs.mkdirSync(dir)
  for (const file of release.HOST_FILES) fs.writeFileSync(path.join(sourceRoot, file), `fixture-only ${file}\n`)
  const receipts = [], expectedBuilds = {}
  for (const target of targets) {
    const dir = path.join(inputDirectory, target), bytes = header(target)
    fs.mkdirSync(dir)
    fs.writeFileSync(path.join(dir, release.artifactName(target)), bytes)
    const proof = receipt(target, bytes)
    const receiptBytes = Buffer.from(JSON.stringify(proof))
    fs.writeFileSync(path.join(dir, 'build-receipt.json'), receiptBytes)
    expectedBuilds[release.targetOutputKey(target)] = JSON.stringify({ sha256: proof.sha256,
      sizeBytes: proof.sizeBytes, receiptSha256: release.sha256(receiptBytes), runAttempt: proof.runAttempt })
    receipts.push(proof)
  }
  return { root, sourceRoot, inputDirectory, outputDirectory, receipts, identity, frontendBytes, expectedBuilds, signingKey,
    contractVersion }
}

const repairTargets = Object.freeze(['aarch64-apple-darwin', 'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu'])

if (!legacySuite) test('repair publication produces a distinct v2 contract for exactly ARM macOS, Windows and Linux', (t) => {
  const value = inputs(t, { targets: repairTargets })
  const manifest = release.assembleRelease(value)
  assert.equal(manifest.schemaVersion, 2)
  assert.deepEqual(manifest.artifacts.map(item => item.target), repairTargets)
  const bytes = fs.readFileSync(path.join(value.outputDirectory, 'bamboo-sidecars-v2.json'))
  assert.deepEqual(JSON.parse(bytes), manifest)
  const proof = JSON.parse(fs.readFileSync(path.join(value.outputDirectory, 'bamboo-executables-provenance-v2.json')))
  assert.equal(proof.schemaVersion, 2)
  assert.equal(proof.kind, 'bamboo-complete-executables/v2')
  assert.deepEqual(proof.builds.map(item => item.target), repairTargets)
  assert.equal(proof.manifestSha256, release.sha256(bytes))
  assert.deepEqual(Buffer.from(proof.manifestBytesBase64, 'base64'), bytes)
})

if (!legacySuite) test('repair publication rejects Intel and every missing required platform', (t) => {
  for (const targets of [
    [...repairTargets, 'x86_64-apple-darwin'],
    ...repairTargets.map(missing => repairTargets.filter(target => target !== missing)),
  ]) assert.throws(() => release.assembleRelease(inputs(t, { targets })))
})

if (!legacySuite) test('v2 permits only the declared empty legacy output and preserves all required native bindings', (t) => {
  const value = inputs(t)
  const intel = release.targetOutputKey('x86_64-apple-darwin')
  const expectedBuilds = { ...value.expectedBuilds, [intel]: '' }
  assert.deepEqual([...release.expectedBuildBindings(expectedBuilds).keys()], repairTargets)
  assert.equal(release.assembleRelease({ ...value, expectedBuilds }).schemaVersion, 2)
  for (const extra of [{ [intel]: '{}' }, { [intel]: null }, { arbitrary: '' }]) {
    assert.throws(() => release.expectedBuildBindings({ ...value.expectedBuilds, ...extra }))
  }
  for (const target of repairTargets) {
    const missing = { ...expectedBuilds }; delete missing[release.targetOutputKey(target)]
    assert.throws(() => release.expectedBuildBindings(missing))
  }
})

if (!legacySuite) test('only one supported source ABI selects the job contract; the job cannot override it', (t) => {
  const root = temporary(t), file = path.join(root, 'scripts/sidecar-release.cjs')
  fs.mkdirSync(path.dirname(file))
  for (const version of [1, 2]) {
    fs.writeFileSync(file, `// BAMBOO_SHARED_EXECUTABLES_V${version}\n`)
    assert.equal(release.sourceContractVersion(root), version)
    assert.equal(release.boundSourceContract(root, { BAMBOO_EXECUTABLE_CONTRACT_VERSION: String(version) }), version)
    assert.throws(() => release.boundSourceContract(root, { BAMBOO_EXECUTABLE_CONTRACT_VERSION: String(3 - version) }))
    if (version === 1) assert.equal(release.boundSourceContract(root, {}), 1)
    else assert.throws(() => release.boundSourceContract(root, {}))
  }
  for (const contents of ['', '// BAMBOO_SHARED_EXECUTABLES_V3\n', '// BAMBOO_SHARED_EXECUTABLES_V01\n',
    '// BAMBOO_SHARED_EXECUTABLES_V1\n// BAMBOO_SHARED_EXECUTABLES_V2\n', 'x'.repeat(128 * 1024 + 1)]) {
    fs.writeFileSync(file, contents); assert.throws(() => release.sourceContractVersion(root))
  }
  const real = path.join(root, 'real'); fs.writeFileSync(real, '// BAMBOO_SHARED_EXECUTABLES_V2\n')
  fs.rmSync(file); fs.symlinkSync(real, file)
  assert.throws(() => release.sourceContractVersion(root))
})

test('actual producer writes a deterministic three-regular-file USTAR host and complete embedded manifest', (t) => {
  const value = inputs(t)
  const manifest = release.assembleRelease(value)
  assert.deepEqual(manifest.artifacts.map((item) => item.target), release.TARGETS)
  for (const item of manifest.artifacts) {
    assert.equal(item.buildMode, 'embedded'); assert.equal(item.embeddedFrontend, true)
    assert.deepEqual(release.fileIdentity(path.join(value.outputDirectory, item.assetName)),
      { sha256: item.sha256, sizeBytes: item.sizeBytes })
  }
  const bytes = fs.readFileSync(path.join(value.outputDirectory, release.HOST_ASSET))
  const tar = zlib.gunzipSync(bytes)
  const members = []
  for (let offset = 0; tar[offset];) {
    const record = tar.subarray(offset, offset + 512)
    assert.equal(record.toString('ascii', 257, 263), 'ustar\0')
    assert.equal(record[156], 48, 'No directory, PAX, symlink or special entry')
    const name = record.subarray(0, 100).toString().replace(/\0.*$/s, '')
    const size = parseInt(record.toString('ascii', 124, 136).replace(/\0.*$/s, ''), 8)
    const data = tar.subarray(offset + 512, offset + 512 + size)
    const file = manifest.browserHost.files.find((item) => item.path === name)
    assert.deepEqual({ sha256: release.sha256(data), sizeBytes: size }, { sha256: file.sha256, sizeBytes: file.sizeBytes })
    members.push(name)
    offset += 512 + Math.ceil(size / 512) * 512
  }
  assert.deepEqual(members, release.HOST_FILES)
  const other = path.join(value.root, 'repeat'); fs.mkdirSync(other)
  assert.deepEqual(release.createBrowserHost({ sourceRoot: value.sourceRoot, outputDirectory: other }), manifest.browserHost)
  assert.deepEqual(fs.readFileSync(path.join(other, release.HOST_ASSET)), bytes)
})

test('manifest rejects old API-only metadata, null identities, wrong names, duplicate or missing targets and invalid host lists', (t) => {
  const value = inputs(t)
  const valid = release.assembleRelease(value)
  const mutations = [
    (m) => { m.artifacts[0].buildMode = 'api-only' }, (m) => { m.artifacts[0].embeddedFrontend = false },
    (m) => { m.version = null }, (m) => { m.version = '0.0.0' }, (m) => { m.sourceRevision = 'short' },
    (m) => { m.artifacts[0].assetName = '../bamboo' }, (m) => { m.artifacts[0].sha256 = 'A'.repeat(64) },
    (m) => { m.artifacts[0].sizeBytes = 1 }, (m) => { m.artifacts.pop() },
    (m) => { m.artifacts[0] = m.artifacts[1] }, (m) => { m.browserHost.files[0].path = '../host.cjs' },
    (m) => { m.browserHost.files.push(m.browserHost.files[0]) },
  ]
  for (const mutate of mutations) { const changed = clone(valid); mutate(changed); assert.throws(() => release.createManifest(changed)) }
})

test('bounded file and host readers reject symlinks and refuse output replacement', (t) => {
  const value = inputs(t)
  const real = path.join(value.root, 'real'), link = path.join(value.root, 'link')
  fs.writeFileSync(real, 'data'); fs.symlinkSync(real, link)
  assert.throws(() => release.fileIdentity(link))
  assert.throws(() => release.fileIdentity(real, 3))
  release.createBrowserHost(value)
  assert.throws(() => release.createBrowserHost(value), /failed/i)
  fs.rmSync(path.join(value.outputDirectory, release.HOST_ASSET))
  const host = path.join(value.sourceRoot, release.HOST_FILES[0])
  fs.rmSync(host); fs.symlinkSync(real, host)
  assert.throws(() => release.createBrowserHost(value), /failed/i)
})

test('native format verification detects mislabelled architecture and non-executable Mach-O/PE/ELF', (t) => {
  const root = temporary(t), file = path.join(root, 'binary')
  for (const target of release.TARGETS) {
    fs.writeFileSync(file, header(target))
    assert.equal(release.binaryArchitecture(file, target), target)
    for (const other of release.TARGETS.filter((item) => item !== target)) assert.throws(() => release.binaryArchitecture(file, other))
  }
  const dll = header('x86_64-pc-windows-msvc'); dll.writeUInt16LE(0x2002, 150)
  fs.writeFileSync(file, dll); assert.throws(() => release.binaryArchitecture(file, 'x86_64-pc-windows-msvc'))
})

test('build provenance permits same-run earlier attempts, rejects other source/run/mode/runtime/command and changed bytes', (t) => {
  const value = inputs(t), original = value.receipts[0]
  release.validateBuildReceipt(original, identity, original.target)
  for (const mutate of [
    (p) => { p.sourceRevision = '8'.repeat(40) }, (p) => { p.sourceTree = '8'.repeat(40) },
    (p) => { p.runId = '101' }, (p) => { p.runAttempt = 3 }, (p) => { p.buildMode = 'api-only' },
    (p) => { p.embeddedFrontend = false }, (p) => { p.runtime.status = 'unrun' },
    (p) => { p.runtime.version = 'bamboo 0.0.0' }, (p) => { p.command.splice(2, 1) },
  ]) { const changed = clone(original); mutate(changed); assert.throws(() => release.validateBuildReceipt(changed, identity, original.target)) }
  fs.appendFileSync(path.join(value.inputDirectory, original.target, original.assetName), 'changed')
  assert.throws(() => release.assembleRelease(value))
})

test('native build receipts preserve original admission across later attempts of the same main CI', (t) => {
  const value = inputs(t), original = clone(value.receipts[0])
  const recorded = { schemaVersion: 1, sourceRevision: identity.sourceRevision,
    ciRun: { id: 90, run_attempt: 1 }, premerge: { runId: 80, runAttempt: 1, snapshotRevision: '8'.repeat(40) } }
  original.validation = clone(recorded)
  const bytes = JSON.stringify(original)
  const current = { ...clone(recorded), ciRun: { ...recorded.ciRun, run_attempt: 2 } }
  release.validateBuildReceipt(original, { ...identity, validation: current }, original.target)
  assert.equal(JSON.stringify(original), bytes, 'Build receipt fields must remain bound to their original admission')
  for (const mutate of [
    value => { value.ciRun.id++ }, value => { value.sourceRevision = '7'.repeat(40) },
    value => { value.premerge.snapshotRevision = '9'.repeat(40) }, value => { value.ciRun.run_attempt = 0 },
  ]) {
    const changed = clone(current); mutate(changed)
    assert.throws(() => release.validateBuildReceipt(original, { ...identity, validation: changed }, original.target))
  }
  const later = { ...clone(original), validation: current }
  assert.throws(() => release.validateBuildReceipt(later, { ...identity, validation: recorded }, later.target))
})

test('official publication requires a completed authenticated same-source crate Release and its immutable direct tag', () => {
  const signingKey = 'a'.repeat(64), signingKeyDigest = release.sha256(Buffer.from(signingKey, 'hex'))
  const receipt = signReceipt({ schemaVersion: 1, identity: { repository: identity.repository,
    sourceRevision: identity.sourceRevision, frontend: { packageName: '@bigduu/lotus-next', packageVersion: '2026.10.16' } },
    version: identity.version, crates: ['bamboo-agent'], frontendBytes, packageChecksums: { 'bamboo-agent': '9'.repeat(64) },
    automatic: true, completed: true, ciRun: identity.ciRun }, signingKey)
  const original = { tag_name: `v${identity.version}`, target_commitish: identity.sourceRevision,
    draft: false, prerelease: false, body: receiptBody(receipt), assets: [] }
  const originalTag = { object: { type: 'commit', sha: identity.sourceRevision } }
  const request = (value, tag) => (route) => JSON.stringify(route.startsWith('releases/') ? value : tag)
  release.publishedRelease(identity, { signingKey, signingKeyDigest, request: request(original, originalTag) })
  const recorded = { schemaVersion: 1, sourceRevision: identity.sourceRevision,
    ciRun: { id: 90, run_attempt: 1 }, premerge: { runId: 80, runAttempt: 1, snapshotRevision: '8'.repeat(40) } }
  const current = { ...clone(recorded), ciRun: { ...recorded.ciRun, run_attempt: 2 } }
  const signedOriginal = { ...original, body: receiptBody(signReceipt({ ...clone(receipt), validation: recorded }, signingKey)) }
  const signedBytes = signedOriginal.body
  release.publishedRelease({ ...identity, validation: current }, { signingKey, signingKeyDigest, request: request(signedOriginal, originalTag) })
  assert.equal(signedOriginal.body, signedBytes, 'A later admission attempt must not replace signed reservation bytes')
  for (const mutate of [
    value => { value.ciRun.id++ }, value => { value.sourceRevision = '7'.repeat(40) },
    value => { value.premerge.runId++ }, value => { value.premerge.runAttempt++ },
    value => { value.premerge.snapshotRevision = '9'.repeat(40) }, value => { value.ciRun.run_attempt = 0 },
  ]) {
    const changed = clone(current); mutate(changed)
    assert.throws(() => release.publishedRelease({ ...identity, validation: changed }, {
      signingKey, signingKeyDigest, request: request(signedOriginal, originalTag),
    }))
  }
  release.publishedRelease(identity, {
    signingKey, signingKeyDigest, request: request(signedOriginal, originalTag),
  }) // Historical callers retain their existing omitted-validation contract.
  for (const mutate of [
    (r, t) => { t.object.sha = '8'.repeat(40) }, (r, t) => { t.object.type = 'tag' },
    (r) => { r.draft = true }, (r) => { r.target_commitish = 'main' },
    (r) => { r.assets = [{ name: 'same' }, { name: 'same' }] },
    (r) => { r.body = r.body.replace('"completed":true', '"completed":false') },
    (r) => { r.body = r.body.replaceAll(identity.ciRun, identity.ciRun + '1') },
    (r) => { r.body = r.body.replace('"automatic":true', '"automatic":false') },
  ]) {
    const value = clone(original), tag = clone(originalTag); mutate(value, tag)
    assert.throws(() => release.publishedRelease(identity, { signingKey, signingKeyDigest, request: request(value, tag) }))
  }
})

test('native fan-out verifies original-attempt preparation transport before crates finish without draft access', (t) => {
  const inputDirectory = temporary(t)
  const manifest = Buffer.from('synthetic preserved manifest'), archive = Buffer.from('synthetic preserved archive')
  fs.writeFileSync(path.join(inputDirectory, 'frontend-manifest.json'), manifest)
  fs.writeFileSync(path.join(inputDirectory, 'lotus-frontend.zip'), archive)
  const preparedBytes = { manifestSha256: release.sha256(manifest), archiveSha256: release.sha256(archive) }
  const receipt = signReceipt({ schemaVersion: 1, identity: { repository: identity.repository,
    sourceRevision: identity.sourceRevision, frontend: { packageName: '@bigduu/lotus-next', packageVersion: '2026.10.16' } },
    version: identity.version, crates: ['bamboo-agent'], frontendBytes: preparedBytes, packageChecksums: {},
    automatic: true, completed: false, ciRun: identity.ciRun }, signingKey)
  const original = { id: 42, tag_name: `v${identity.version}`, target_commitish: identity.sourceRevision,
    draft: true, prerelease: false, body: receiptBody(receipt), assets: [] }
  const preparation = createPreparation({ release: original, receipt }, { ...identity, runAttempt: 1 }, testContractVersion)
  const tag = { object: { type: 'commit', sha: identity.sourceRevision } }
  const options = { preparation, contractVersion: testContractVersion, inputDirectory }
  const restored = release.preparedFrontend(identity, options)
  assert.deepEqual(restored.receipt, receipt)
  assert.deepEqual(restored.files, [{ name: 'frontend-manifest.json', bytes: manifest }, { name: 'lotus-frontend.zip', bytes: archive }])
  // No Release/body request or secret is supplied; crate progress cannot affect
  // the preparation snapshot or force these workers to wait for publication.
  original.draft = false; original.body = 'Concurrent mutable crate progress'
  assert.deepEqual(release.preparedFrontend(identity, options).receipt, receipt)
  original.draft = true
  assert.throws(() => release.publishedRelease(identity, {
    request: route => JSON.stringify(route.startsWith('releases/') ? original : tag),
  }), /Crate publication must complete/)
  fs.appendFileSync(path.join(inputDirectory, 'lotus-frontend.zip'), 'substituted bytes')
  assert.throws(() => release.preparedFrontend(identity, options), /independent preparation output/)
  fs.writeFileSync(path.join(inputDirectory, 'lotus-frontend.zip'), archive)
  fs.writeFileSync(path.join(inputDirectory, 'extra'), 'unbound input')
  assert.throws(() => release.preparedFrontend(identity, options), /exactly the preserved frontend/)
  fs.rmSync(path.join(inputDirectory, 'extra'))
  const otherRun = { ...identity, runId: '101' }
  assert.throws(() => release.preparedFrontend(otherRun, options), /preparation mismatch: runId/)
  if (process.platform !== 'win32') {
    fs.rmSync(path.join(inputDirectory, 'lotus-frontend.zip'))
    fs.symlinkSync(path.join(inputDirectory, 'frontend-manifest.json'), path.join(inputDirectory, 'lotus-frontend.zip'))
    assert.throws(() => release.preparedFrontend(identity, options), /bounded regular release asset/)
  }
})

test('native dependency cache maps the real runner-temp target and keeps historical/frozen publication restore-only', (t) => {
  const root = temporary(t), temporaryRoot = path.join(root, 'runner-temp'), source = path.join(root, 'source')
  fs.mkdirSync(source); fs.mkdirSync(path.join(source, 'scripts')); fs.mkdirSync(path.join(source, '.cargo'))
  for (const name of ['Cargo.toml', 'Cargo.lock', 'scripts/sidecar-build.cjs', '.cargo/config.toml']) {
    fs.writeFileSync(path.join(source, name), `synthetic ${name}`)
  }
  const value = { root: source, temporary: temporaryRoot, contractVersion: testContractVersion,
    target: release.TARGETS[0], sourceParents: '8'.repeat(40) + ' ' + '9'.repeat(40), sourceMessage: 'ordinary release' }
  const policy = nativeCachePolicy(value)
  const [workspace, target] = policy.workspace.split(' -> ')
  assert.equal(path.resolve(workspace, target), path.join(temporaryRoot, 'bamboo-executable-target'))
  assert.equal(policy.save, !legacySuite)
  assert.equal(nativeCachePolicy({ ...value, contractVersion: 1 }).save, false)
  assert.equal(nativeCachePolicy({ ...value, sourceParents: 'dbafb74522794012fc8f0a61e761817b3913989a ' + '9'.repeat(40) }).save, false)
  assert.equal(nativeCachePolicy({ ...value, sourceMessage: 'Merge release/da348a63f1d019a22df547c78575c500b6b1294e' }).save, false)
  assert.equal(nativeCachePolicy({ ...value, cutRevision: 'da348a63f1d019a22df547c78575c500b6b1294e' }).save, false)
  const originalKey = policy.key
  for (const name of ['Cargo.toml', 'Cargo.lock', 'scripts/sidecar-build.cjs', '.cargo/config.toml']) {
    fs.appendFileSync(path.join(source, name), 'changed')
    assert.notEqual(nativeCachePolicy(value).key, originalKey)
  }
  assert.notEqual(nativeCachePolicy({ ...value, target: release.TARGETS.at(-1) }).key, nativeCachePolicy(value).key)
})

test('native cache eviction removes restored workspace binaries and embedded build outputs while keeping dependencies', (t) => {
  const root = temporary(t), targetDirectory = path.join(root, 'target'), target = release.TARGETS[0]
  const metadata = { workspace_members: ['agent', 'server'], packages: [
    { id: 'agent', name: 'bamboo-agent', targets: [{ name: 'bamboo' }] },
    { id: 'server', name: 'bamboo-server', targets: [{ name: 'bamboo_server' }] },
    { id: 'dependency', name: 'serde', targets: [{ name: 'serde' }] },
  ] }
  const stale = [], dependency = []
  for (const profile of ['release', `${target}/release`]) {
    for (const name of ['bamboo', 'bamboo.d', 'deps/libbamboo_server-old.rlib',
      '.fingerprint/bamboo-server-old/marker', 'build/bamboo-server-old/out/frontend_package_embedded.rs']) {
      const file = path.join(targetDirectory, profile, name)
      fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, 'stale workspace fixture'); stale.push(file)
    }
    for (const name of ['deps/libserde-old.rlib', '.fingerprint/serde-old/marker', 'build/serde-old/out/marker']) {
      const file = path.join(targetDirectory, profile, name)
      fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, 'dependency fixture'); dependency.push(file)
    }
  }
  assert.ok(evictWorkspaceArtifacts(metadata, targetDirectory, target).length >= 10)
  for (const file of stale) assert.equal(fs.existsSync(file), false)
  for (const file of dependency) assert.equal(fs.readFileSync(file, 'utf8'), 'dependency fixture')
  assert.deepEqual(evictWorkspaceArtifacts(metadata, targetDirectory, target), [])
  if (process.platform !== 'win32') {
    const link = path.join(root, 'unsafe-target')
    fs.symlinkSync(targetDirectory, link)
    assert.throws(() => evictWorkspaceArtifacts(metadata, link, target), /must not be a symlink/)
  }
})

test('assembly rejects additional transports, foreign frontend bytes and missing platforms before publishing', (t) => {
  const value = inputs(t)
  const file = path.join(value.inputDirectory, value.receipts[0].target, 'build-receipt.json')
  const changed = clone(value.receipts[0]); changed.frontendBytes.archiveSha256 = '8'.repeat(64)
  const receiptBytes = Buffer.from(JSON.stringify(changed)); fs.writeFileSync(file, receiptBytes)
  const binding = JSON.parse(value.expectedBuilds[release.targetOutputKey(changed.target)])
  binding.receiptSha256 = release.sha256(receiptBytes)
  value.expectedBuilds[release.targetOutputKey(changed.target)] = JSON.stringify(binding)
  assert.throws(() => release.assembleRelease(value), /frontend bytes/)
  fs.mkdirSync(path.join(value.inputDirectory, 'foreign'))
  assert.throws(() => release.assembleRelease(value), /transport directory/)
})

test('substituting a binary and matching forged receipt cannot bypass the independent native-job output digest', (t) => {
  const value = inputs(t), original = clone(value.receipts[0])
  const file = path.join(value.inputDirectory, original.target, original.assetName)
  const bytes = fs.readFileSync(file); bytes[4096] = 9; fs.writeFileSync(file, bytes)
  original.sha256 = release.sha256(bytes)
  fs.writeFileSync(path.join(value.inputDirectory, original.target, 'build-receipt.json'), JSON.stringify(original))
  assert.throws(() => release.assembleRelease(value), /independent build-job output/)
  const missing = clone(value.expectedBuilds); delete missing[release.targetOutputKey(release.TARGETS[0])]
  assert.throws(() => release.expectedBuildBindings(missing), legacySuite ? /four unique/ : /three unique/)
  assert.throws(() => release.expectedBuildBindings(null), /independent build-job outputs/)
  const wrongAttempt = inputs(t)
  const key = release.targetOutputKey(release.TARGETS[0])
  const binding = JSON.parse(wrongAttempt.expectedBuilds[key]); binding.runAttempt = 2
  wrongAttempt.expectedBuilds[key] = JSON.stringify(binding)
  assert.throws(() => release.assembleRelease(wrongAttempt), /bound native-job attempt/)
})

test('Cargo unit evidence must bind both real embedded outputs and the exact resulting binary', (t) => {
  const targetDirectory = temporary(t), out = path.join(targetDirectory, 'build-out')
  fs.mkdirSync(out)
  const manifest = Buffer.from('manifest'), zip = Buffer.from('zip')
  fs.writeFileSync(path.join(out, 'frontend_package_manifest.json'), manifest)
  fs.writeFileSync(path.join(out, 'frontend_package.zip'), zip)
  const embedded = 'pub static DUPLICATE_FRONTEND_PACKAGE_ZIP: Option<&[u8]> = Some(include_bytes!("zip"));\n' +
    'pub static DUPLICATE_FRONTEND_PACKAGE_MANIFEST: Option<&[u8]> = Some(include_bytes!("manifest"));\n'
  fs.writeFileSync(path.join(out, 'frontend_package_embedded.rs'), embedded)
  const binary = path.join(targetDirectory, 'bamboo')
  const options = { binary, targetDirectory, serverPackageId: 'server-unit',
    frontendBytes: { archiveSha256: release.sha256(zip), manifestSha256: release.sha256(manifest) } }
  const messages = [{ reason: 'build-script-executed', package_id: 'server-unit', out_dir: out },
    { reason: 'compiler-artifact', target: { name: 'bamboo', kind: ['bin'] }, executable: binary },
    { reason: 'build-finished', success: true }]
  verifyBuildMessages(messages, options)
  const wrong = clone(messages); wrong[0].package_id = 'foreign-unit'
  assert.throws(() => verifyBuildMessages(wrong, options))
  fs.writeFileSync(path.join(out, 'frontend_package_embedded.rs'), embedded.replaceAll('Some(include_bytes!', 'None; //'))
  assert.throws(() => verifyBuildMessages(messages, options))
  fs.writeFileSync(path.join(out, 'frontend_package_embedded.rs'), embedded)
  fs.writeFileSync(path.join(out, 'frontend_package.zip'), 'wrong')
  assert.throws(() => verifyBuildMessages(messages, options))
})

test('Cargo never inherits publication credentials or API-only/compiler-profile overrides', () => {
  const env = buildEnvironment({ PATH: '/fixture/bin', GH_TOKEN: 'fake', CARGO_REGISTRY_TOKEN: 'fake',
    BAMBOO_RELEASE_SIGNING_KEY: 'fake', BAMBOO_FRONTEND_BUILD_MODE: 'embedded' }, '/own/target')
  assert.equal(env.PATH, '/fixture/bin')
  assert.equal(env.BAMBOO_FRONTEND_BUILD_MODE, 'embedded')
  assert.equal(env.CARGO_TARGET_DIR, '/own/target')
  for (const key of ['GH_TOKEN', 'CARGO_REGISTRY_TOKEN', 'BAMBOO_RELEASE_SIGNING_KEY']) assert.equal(env[key], undefined)
  for (const overrides of [{ BAMBOO_FRONTEND_BUILD_MODE: 'api-only' }, { RUSTFLAGS: '' }, { CARGO_PROFILE_RELEASE_LTO: 'off' }]) {
    assert.throws(() => buildEnvironment(overrides, '/own/target'))
  }
  for (const [platform, arch, target] of [['darwin', 'arm64', 'aarch64-apple-darwin'], ['darwin', 'x64', 'x86_64-apple-darwin'],
    ['win32', 'x64', 'x86_64-pc-windows-msvc'], ['linux', 'x64', 'x86_64-unknown-linux-gnu']]) assert.equal(nativeTarget(platform, arch), target)
  assert.throws(() => nativeTarget('linux', 'arm64'))
})

test('real controller children retain ordinary environment but never receive signing, registry or unrelated GitHub authority', () => {
  const output = release.execute(process.execPath, ['-e',
    'process.stdout.write(JSON.stringify({marker:process.env.FIXTURE_MARKER,leaked:Object.keys(process.env).filter(k=>/TOKEN|SIGNING_KEY/.test(k))}))'],
  { env: { PATH: process.env.PATH, FIXTURE_MARKER: 'retained', GH_TOKEN: 'synthetic-test-value',
    CARGO_REGISTRY_TOKEN: 'synthetic-test-value', BAMBOO_RELEASE_SIGNING_KEY: 'synthetic-test-value' } })
  assert.deepEqual(JSON.parse(output), { marker: 'retained', leaked: [] })
})

function publishingFixture(t) {
  const value = inputs(t); release.assembleRelease(value)
  const stored = new Map(), uploads = []
  const assets = () => [...stored.keys()].map((name) => ({ name }))
  return { ...value, stored, uploads, context: { directory: value.outputDirectory, release: { assets: [] },
    readAsset: (asset) => stored.get(asset.name), listAssets: assets,
    upload: (file) => { const name = path.basename(file); uploads.push(name); stored.set(name, fs.readFileSync(file)) } } }
}

test('same-main CI reruns recover original signed native admission and reject changed premerge evidence', async (t) => {
  const value = publishingFixture(t)
  const recorded = { schemaVersion: 1, sourceRevision: identity.sourceRevision,
    ciRun: { id: 90, run_attempt: 1 }, premerge: { runId: 80, runAttempt: 1, snapshotRevision: '8'.repeat(40) } }
  const proofFile = path.join(value.outputDirectory, release.PROVENANCE)
  const proof = JSON.parse(fs.readFileSync(proofFile))
  proof.validation = recorded
  for (const build of proof.builds) build.validation = clone(recorded)
  signReceipt(proof, signingKey)
  fs.writeFileSync(proofFile, JSON.stringify(proof))
  for (const name of fs.readdirSync(value.outputDirectory)) value.stored.set(name, fs.readFileSync(path.join(value.outputDirectory, name)))
  const current = { ...clone(recorded), ciRun: { ...recorded.ciRun, run_attempt: 2 } }
  const outputDirectory = path.join(value.root, 'same-main-rerun'); fs.mkdirSync(outputDirectory)
  const recovery = { identity: { ...identity, validation: current }, sourceRoot: value.sourceRoot,
    outputDirectory, frontendBytes, signingKey, release: { assets: [...value.stored.keys()].map(name => ({ name })) },
    readAsset: asset => value.stored.get(asset.name) }
  await release.recoverPublishedRelease(recovery)
  const original = JSON.parse(fs.readFileSync(path.join(outputDirectory, release.PROVENANCE)))
  assert.deepEqual(original.validation, recorded)
  assert.ok(original.builds.every(build => build.validation.ciRun.run_attempt === 1))
  const changed = clone(current); changed.premerge.snapshotRevision = '9'.repeat(40)
  await assert.rejects(() => release.recoverPublishedRelease({ ...recovery,
    identity: { ...identity, validation: changed }, outputDirectory: path.join(value.root, 'rejected-rerun') }), /admission changed/)
})

if (!legacySuite) test('v2 refuses foreign executable contracts and Intel assets before any recovery read or upload', async (t) => {
  const value = publishingFixture(t)
  for (const name of ['bamboo-sidecars-v1.json', 'bamboo-executables-provenance-v1.json',
    'bamboo-sidecars-v3.json', 'bamboo-x86_64-apple-darwin']) {
    let reads = 0
    const candidate = { assets: [{ name }] }
    await assert.rejects(() => release.publishAssets({ ...value.context, release: candidate,
      readAsset: () => { reads++; throw new Error('foreign read') } }))
    assert.deepEqual(value.uploads, [])
    const outputDirectory = path.join(value.root, 'recovery-' + name); fs.mkdirSync(outputDirectory)
    await assert.rejects(() => release.recoverPublishedRelease({ ...value, outputDirectory, release: candidate,
      readAsset: () => { reads++; throw new Error('foreign read') } }))
    assert.equal(reads, 0)
    assert.deepEqual(fs.readdirSync(outputDirectory), [])
  }
})

test('release assets upload once, verify each result, and commit the manifest last; identical reruns do no writes', async (t) => {
  const value = publishingFixture(t)
  assert.deepEqual(await release.publishAssets(value.context), { reused: false })
  assert.equal(value.uploads.at(-1), release.MANIFEST)
  assert.equal(value.uploads.length, legacySuite ? 7 : 6)
  value.context.release.assets = [...value.stored.keys()].map((name) => ({ name }))
  value.uploads.length = 0
  assert.deepEqual(await release.publishAssets(value.context), { reused: true })
  assert.deepEqual(value.uploads, [])
})

test('rerunning only a failed final publisher preserves immutable provenance from earlier successful platform builds', (t) => {
  const value = inputs(t)
  release.assembleRelease(value)
  const repeat = path.join(value.root, 'retry'); fs.mkdirSync(repeat)
  release.assembleRelease({ ...value, outputDirectory: repeat, identity: { ...identity, runAttempt: 3 } })
  for (const name of fs.readdirSync(value.outputDirectory)) {
    assert.deepEqual(fs.readFileSync(path.join(repeat, name)), fs.readFileSync(path.join(value.outputDirectory, name)))
  }
})

test('a full workflow rerun recovers the authenticated original bytes and uploads only the missing manifest', async (t) => {
  const value = publishingFixture(t)
  value.context.upload = (file) => {
    const name = path.basename(file)
    if (name === release.MANIFEST) throw new Error('manifest transport failed')
    value.uploads.push(name); value.stored.set(name, fs.readFileSync(file))
  }
  await assert.rejects(() => release.publishAssets(value.context), /manifest transport failed/)
  assert.ok(value.stored.has(release.PROVENANCE)); assert.ok(!value.stored.has(release.MANIFEST))
  // The supported full rerun really rebuilds: attempts and executable bytes
  // differ. They must not replace the first authenticated published identity.
  const retryIdentity = { ...identity, runAttempt: 3 }
  const rebuilt = path.join(value.root, 'rebuilt'); fs.mkdirSync(rebuilt)
  for (const target of release.TARGETS) {
    const binary = path.join(value.inputDirectory, target, release.artifactName(target))
    const bytes = fs.readFileSync(binary); bytes[4096] = 42; fs.writeFileSync(binary, bytes)
    const next = { ...receipt(target, bytes), runAttempt: 3 }
    const proof = Buffer.from(JSON.stringify(next))
    fs.writeFileSync(path.join(value.inputDirectory, target, 'build-receipt.json'), proof)
    value.expectedBuilds[release.targetOutputKey(target)] = JSON.stringify({ sha256: next.sha256,
      sizeBytes: next.sizeBytes, receiptSha256: release.sha256(proof), runAttempt: next.runAttempt })
  }
  release.assembleRelease({ ...value, identity: retryIdentity, outputDirectory: rebuilt })
  assert.notDeepEqual(fs.readFileSync(path.join(rebuilt, release.PROVENANCE)), value.stored.get(release.PROVENANCE))
  const recovered = path.join(value.root, 'recovered'); fs.mkdirSync(recovered)
  const assets = () => [...value.stored.keys()].map((name) => ({ name }))
  await release.recoverPublishedRelease({ ...value, identity: retryIdentity, outputDirectory: recovered,
    release: { assets: assets() }, readAsset: (asset) => value.stored.get(asset.name) })
  for (const name of fs.readdirSync(value.outputDirectory)) {
    assert.deepEqual(fs.readFileSync(path.join(recovered, name)), fs.readFileSync(path.join(value.outputDirectory, name)))
  }
  const uploads = []
  await release.publishAssets({ directory: recovered, release: { assets: assets() },
    readAsset: (asset) => value.stored.get(asset.name), listAssets: assets,
    upload: (file) => { uploads.push(path.basename(file)); value.stored.set(path.basename(file), fs.readFileSync(file)) } })
  assert.deepEqual(uploads, [release.MANIFEST])
})

test('same-main CI reruns recover original signed native admission and reject changed premerge evidence', async (t) => {
  const value = publishingFixture(t)
  const recorded = { schemaVersion: 1, sourceRevision: identity.sourceRevision,
    ciRun: { id: 90, run_attempt: 1 }, premerge: { runId: 80, runAttempt: 1, snapshotRevision: '8'.repeat(40) } }
  const proofFile = path.join(value.outputDirectory, release.PROVENANCE)
  const proof = JSON.parse(fs.readFileSync(proofFile))
  proof.validation = recorded
  for (const build of proof.builds) build.validation = clone(recorded)
  signReceipt(proof, signingKey)
  fs.writeFileSync(proofFile, JSON.stringify(proof))
  for (const name of fs.readdirSync(value.outputDirectory)) value.stored.set(name, fs.readFileSync(path.join(value.outputDirectory, name)))
  const originalBytes = fs.readFileSync(proofFile)
  const current = { ...clone(recorded), ciRun: { ...recorded.ciRun, run_attempt: 2 } }
  const outputDirectory = path.join(value.root, 'same-main-rerun'); fs.mkdirSync(outputDirectory)
  const recovery = { identity: { ...identity, validation: current }, sourceRoot: value.sourceRoot,
    outputDirectory, frontendBytes, signingKey, release: { assets: [...value.stored.keys()].map(name => ({ name })) },
    readAsset: asset => value.stored.get(asset.name) }
  await release.recoverPublishedRelease(recovery)
  assert.deepEqual(fs.readFileSync(path.join(outputDirectory, release.PROVENANCE)), originalBytes,
    'Recovery must retain the exact authenticated checkpoint bytes')
  const original = JSON.parse(fs.readFileSync(path.join(outputDirectory, release.PROVENANCE)))
  assert.deepEqual(original.validation, recorded)
  assert.ok(original.builds.every(build => build.validation.ciRun.run_attempt === 1))
  for (const mutate of [
    value => { value.ciRun.id++ }, value => { value.sourceRevision = '7'.repeat(40) },
    value => { value.premerge.runId++ }, value => { value.premerge.runAttempt++ },
    value => { value.premerge.snapshotRevision = '9'.repeat(40) }, value => { value.ciRun.run_attempt = 0 },
  ]) {
    const changed = clone(current); mutate(changed)
    await assert.rejects(() => release.recoverPublishedRelease({ ...recovery,
      identity: { ...identity, validation: changed }, outputDirectory: path.join(value.root, 'rejected-rerun') }))
  }
  // The signed checkpoint remains bound to the original admission. A nested
  // build must not silently acquire the newer dispatch's admission identity.
  const inconsistent = clone(proof)
  inconsistent.builds[0].validation = clone(current)
  signReceipt(inconsistent, signingKey)
  value.stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(inconsistent)))
  await assert.rejects(() => release.recoverPublishedRelease({ ...recovery,
    outputDirectory: path.join(value.root, 'inconsistent-build') }), /must not move backwards/)
})

test('recovery rejects unsigned/tampered/foreign checkpoints, missing assets and corrupt bytes without any release writes', async (t) => {
  const value = publishingFixture(t); await release.publishAssets(value.context)
  const original = value.stored.get(release.PROVENANCE)
  const target = release.artifactName(release.TARGETS[0])
  const variants = [
    (stored) => { const p = JSON.parse(original); delete p.authentication; stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.builds[0].sha256 = '8'.repeat(64); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.runId = '999'; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.workflowRevision = '9'.repeat(40); signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.runAttempt = 0; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.ciRun += '/foreign'; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.manifestBytesBase64 = '!'; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); const m = JSON.parse(Buffer.from(p.manifestBytesBase64, 'base64')); m.sourceRevision = '8'.repeat(40); const bytes = Buffer.from(JSON.stringify(m)); p.manifestBytesBase64 = bytes.toString('base64'); p.manifestSha256 = release.sha256(bytes); signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.sourceRevision = '8'.repeat(40); signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.kind = 'another-protocol'; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { stored.delete(target) },
    (stored) => { stored.set(target, Buffer.from('corrupt executable')) },
    (stored) => { stored.set(release.HOST_ASSET, Buffer.from('corrupt host')) },
  ]
  for (const [index, mutate] of variants.entries()) {
    const stored = new Map(value.stored); mutate(stored)
    const outputDirectory = path.join(value.root, `bad-recovery-${index}`); fs.mkdirSync(outputDirectory)
    await assert.rejects(() => release.recoverPublishedRelease({ ...value, outputDirectory,
      release: { assets: [...stored.keys()].map((name) => ({ name })) }, readAsset: (asset) => stored.get(asset.name) }))
  }
})

test('partial uploads resume without replacement, while conflicts or a committed manifest with missing assets fail closed', async (t) => {
  const value = publishingFixture(t), name = release.artifactName(release.TARGETS[0])
  value.stored.set(name, fs.readFileSync(path.join(value.outputDirectory, name)))
  value.context.release.assets = [{ name }]
  await release.publishAssets(value.context)
  assert.ok(!value.uploads.includes(name))
  value.uploads.length = 0
  value.stored.set(name, Buffer.from('wrong'))
  await assert.rejects(() => release.publishAssets(value.context), /refusing replacement/)
  assert.deepEqual(value.uploads, [])
  value.stored.delete(name)
  value.context.release.assets = [...value.stored.keys()].map((key) => ({ name: key }))
  await assert.rejects(() => release.publishAssets(value.context), /missing release assets/)
  assert.deepEqual(value.uploads, [])
})

test('publication detects corrupted server responses before exposing a manifest', async (t) => {
  const value = publishingFixture(t)
  value.context.readAsset = () => Buffer.from('corrupt response')
  await assert.rejects(() => release.publishAssets(value.context), /Uploaded bytes differ/)
  assert.ok(!value.uploads.includes(release.MANIFEST))
})

test('workflow retains successful-CI authority and binds its native matrix to the accepted source contract', () => {
  const source = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const matrix = source.split('  executables:\n')[1].split('  publish-executables:\n')[0]
  const final = source.split('  publish-executables:\n')[1]
  assert.match(source, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.equal(source.split('bamboo-release-controller/scripts/crate-release.cjs" plan').length - 1, 1)
  const preparation = source.split('  automatic:\n')[1].split('  publish-crates:\n')[0]
  const crates = source.split('  publish-crates:\n')[1].split('  executables:\n')[0]
  assert.doesNotMatch(preparation, /crate-release\.cjs" publish|crate-release-lock\.py stamp/)
  assert.match(crates, /needs: automatic/)
  assert.ok(crates.includes('SOURCE_VALIDATION: ${{ needs.automatic.outputs.source_validation }}'))
  assert.doesNotMatch(crates, /steps\.source\./, 'The crate worker must inherit preparation evidence across jobs')
  assert.doesNotMatch(crates, /needs:.*executables|crate-release\.cjs" plan/)
  assert.match(crates, /crate-release\.cjs" resume/)
  assert.ok(crates.indexOf('crate-release.cjs" resume') < crates.indexOf('crate-release-lock.py stamp'))
  assert.match(matrix, /needs: automatic/)
  assert.doesNotMatch(matrix, /contents: write|secrets\.|api-only|cargo publish/)
  assert.ok(matrix.includes('matrix: ${{ fromJSON(needs.automatic.outputs.executable_matrix) }}'))
  assert.ok(matrix.includes('BAMBOO_EXECUTABLE_CONTRACT_VERSION: ${{ needs.automatic.outputs.executable_contract_version }}'))
  assert.ok(matrix.includes('PREPARED_RELEASE: ${{ needs.automatic.outputs.prepared_release }}'))
  assert.ok(matrix.includes('name: bamboo-prepared-frontend-attempt-${{ fromJSON(needs.automatic.outputs.prepared_release).runAttempt }}'))
  assert.match(preparation, /name: bamboo-prepared-frontend-attempt-\$\{\{ github\.run_attempt \}\}/)
  assert.ok(preparation.indexOf('crate-release.cjs" plan') < preparation.indexOf('name: bamboo-prepared-frontend-attempt-'))
  assert.doesNotMatch(matrix, /GH_TOKEN:/)
  assert.match(matrix, /workspaces: \$\{\{ steps\.cache_policy\.outputs\.workspace \}\}/)
  assert.match(matrix, /cache-workspace-crates: false/)
  assert.match(matrix, /save-if: \$\{\{ steps\.cache_policy\.outputs\.save == 'true' \}\}/)
  assert.ok(final.includes('BAMBOO_EXECUTABLE_CONTRACT_VERSION: ${{ needs.automatic.outputs.executable_contract_version }}'))
  assert.doesNotMatch(matrix, /inputs\./)
  assert.match(matrix, /persist-credentials: false/)
  for (const target of release.LEGACY_TARGETS) {
    const key = release.targetOutputKey(target)
    assert.ok(matrix.includes(key + ': ${{ steps.build.outputs.' + key + ' }}'))
    assert.ok(final.includes('name: bamboo-executable-' + target + '-attempt-${{ fromJSON(needs.executables.outputs.' + key + ').runAttempt }}'))
    assert.ok(final.includes('path: ${{ runner.temp }}/bamboo-executable-inputs/' + target))
  }
  const downloads = final.split('      - uses: actions/download-artifact@').slice(1)
  for (const target of release.LEGACY_TARGETS) {
    const step = downloads.find(block => block.includes('bamboo-executable-' + target + '-attempt-'))
    assert.ok(step)
    if (target === 'x86_64-apple-darwin') {
      assert.match(step, /if: needs\.automatic\.outputs\.executable_contract_version == '1'/)
    } else assert.doesNotMatch(step.split('\n      - ')[0], /\n        if:/)
  }
  assert.ok(matrix.includes('name: bamboo-executable-${{ matrix.target }}-attempt-${{ github.run_attempt }}'))
  assert.doesNotMatch(matrix, /overwrite: true/)
  for (const match of source.matchAll(/uses: ([^\s]+)@([^\s]+)/g)) assert.match(match[2], /^[0-9a-f]{40}$/, 'Release actions must use immutable reviewed commits')
  assert.ok(final.includes('EXPECTED_BUILDS: ${{ toJSON(needs.executables.outputs) }}'))
  assert.match(final, /needs: \[automatic, publish-crates, executables\]/)
  assert.match(final, /environment: bamboo-release\n    permissions:\n      contents: write/)
  assert.match(final, /bamboo-release-controller\/scripts\/sidecar-release.cjs" publish/)
  assert.doesNotMatch(final, /--clobber|cargo build|cargo publish|inputs\./)
})

test('the actual source preflight rejects every missing or incompatible producer before reservation or crate publication', (t) => {
  const root = temporary(t)
  const workflow = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const automatic = workflow.split('  automatic:\n')[1].split('  executables:\n')[0]
  const name = 'Require the complete executable policy before reserving a version'
  const preflight = automatic.split(`      - name: ${name}\n`)[1].split('\n      - ')[0]
  const script = preflight.split("          python3 - <<'PY'\n")[1].split('\n          PY')[0]
    .split('\n').map((line) => line.slice(10)).join('\n')
  assert.ok(automatic.indexOf(name) < automatic.indexOf('Reserve or resume the exact release identity'))
  assert.ok(automatic.indexOf(name) < automatic.indexOf('Publish verified crates'))
  assert.doesNotMatch(preflight, /GH_TOKEN|CARGO_REGISTRY_TOKEN|crate-release.cjs (?:plan|publish)/)
  for (const version of [1, 2]) {
    const sourceRoot = path.join(root, `source-v${version}`), output = path.join(root, `output-v${version}`)
    const files = { 'scripts/crate-release.cjs': '',
      'scripts/sidecar-build.cjs': `// BAMBOO_SHARED_EXECUTABLES_V${version}`,
      'scripts/sidecar-release.cjs': `// BAMBOO_SHARED_EXECUTABLES_V${version}`,
      'scripts/sidecar-browser-host.py': '# BAMBOO_SHARED_EXECUTABLES_V1',
      '.github/workflows/publish-crate.yml': `# BAMBOO_SHARED_EXECUTABLES_V${version}` }
    for (const [file, marker] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(sourceRoot, file)), { recursive: true })
      fs.writeFileSync(path.join(sourceRoot, file), marker + '\n')
    }
    const run = () => {
      fs.rmSync(output, { force: true })
      return spawnSync(process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3'),
        ['-c', script], { cwd: sourceRoot, encoding: 'utf8', env: {
          PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1', GITHUB_OUTPUT: output } })
    }
    assert.equal(run().status, 0)
    const actual = Object.fromEntries(fs.readFileSync(output, 'utf8').trim().split('\n').map(line => {
      const at = line.indexOf('='); return [line.slice(0, at), line.slice(at + 1)]
    }))
    assert.equal(actual.version, String(version))
    const expected = [
      { target: 'aarch64-apple-darwin', os: 'macos-latest' },
      ...(version === 1 ? [{ target: 'x86_64-apple-darwin', os: 'macos-15-intel' }] : []),
      { target: 'x86_64-pc-windows-msvc', os: 'windows-latest' },
      { target: 'x86_64-unknown-linux-gnu', os: 'ubuntu-22.04' },
    ]
    assert.deepEqual(JSON.parse(actual.matrix), { include: expected })
    for (const [file, marker] of Object.entries(files)) {
      const target = path.join(sourceRoot, file); fs.rmSync(target)
      assert.notEqual(run().status, 0, `Missing source producer must stop before publication: ${file}`)
      assert.ok(!fs.existsSync(output))
      if (marker) {
        fs.writeFileSync(target, 'legacy producer without this ABI\n')
        assert.notEqual(run().status, 0, `An earlier incomplete producer must stop: ${file}`)
        assert.ok(!fs.existsSync(output))
        fs.writeFileSync(target, marker + '\n' + marker + '\n')
        assert.notEqual(run().status, 0, `Duplicate ABI must stop: ${file}`)
        assert.ok(!fs.existsSync(output))
      }
      fs.writeFileSync(target, marker + '\n')
    }
    const producer = path.join(sourceRoot, 'scripts/sidecar-release.cjs')
    for (const marker of ['// BAMBOO_SHARED_EXECUTABLES_V3', '// BAMBOO_SHARED_EXECUTABLES_V01']) {
      fs.writeFileSync(producer, marker + '\n')
      assert.notEqual(run().status, 0)
      assert.ok(!fs.existsSync(output))
    }
  }
})

test('a fresh dispatch after main advances completes the original signed checkpoint without expired job artifacts', async (t) => {
  const value = publishingFixture(t)
  value.context.upload = (file) => {
    const name = path.basename(file)
    if (name === release.MANIFEST) throw new Error('manifest upload interrupted')
    value.stored.set(name, fs.readFileSync(file))
  }
  await assert.rejects(() => release.publishAssets(value.context), /manifest upload interrupted/)
  assert.ok(value.stored.has(release.PROVENANCE)); assert.ok(!value.stored.has(release.MANIFEST))
  const freshDispatch = { ...identity, workflowRevision: '8'.repeat(40), runId: '101', runAttempt: 1 }
  const outputDirectory = path.join(value.root, 'fresh-dispatch-recovery'); fs.mkdirSync(outputDirectory)
  const assets = () => [...value.stored.keys()].map(name => ({ name }))
  // No artifact input/bindings are supplied: only authenticated public checkpoint assets survive.
  await release.recoverPublishedRelease({ identity: freshDispatch, sourceRoot: value.sourceRoot,
    outputDirectory, frontendBytes, signingKey, release: { assets: assets() },
    readAsset: asset => value.stored.get(asset.name) })
  for (const name of fs.readdirSync(value.outputDirectory)) {
    assert.deepEqual(fs.readFileSync(path.join(outputDirectory, name)), fs.readFileSync(path.join(value.outputDirectory, name)))
  }
  const uploads = []
  await release.publishAssets({ directory: outputDirectory, release: { assets: assets() },
    readAsset: asset => value.stored.get(asset.name), listAssets: assets,
    upload: file => { uploads.push(path.basename(file)); value.stored.set(path.basename(file), fs.readFileSync(file)) } })
  assert.deepEqual(uploads, [release.MANIFEST])
  const original = JSON.parse(value.stored.get(release.PROVENANCE))
  assert.equal(original.runId, identity.runId)
  assert.equal(original.workflowRevision, identity.workflowRevision)
})

test('checkpoint recovery preserves the signed original manifest serialization after controller changes', async (t) => {
  const value = publishingFixture(t)
  const manifest = JSON.parse(fs.readFileSync(path.join(value.outputDirectory, release.MANIFEST)))
  const originalBytes = Buffer.from(`${JSON.stringify(manifest, null, 4)}\n`)
  const proof = JSON.parse(fs.readFileSync(path.join(value.outputDirectory, release.PROVENANCE)))
  proof.manifestSha256 = release.sha256(originalBytes)
  proof.manifestBytesBase64 = originalBytes.toString('base64')
  signReceipt(proof, signingKey)
  fs.writeFileSync(path.join(value.outputDirectory, release.MANIFEST), originalBytes)
  fs.writeFileSync(path.join(value.outputDirectory, release.PROVENANCE), `${JSON.stringify(proof, null, 2)}\n`)
  value.context.upload = file => {
    const name = path.basename(file)
    if (name === release.MANIFEST) throw new Error('manifest transport interrupted')
    value.stored.set(name, fs.readFileSync(file))
  }
  await assert.rejects(() => release.publishAssets(value.context), /manifest transport interrupted/)
  const outputDirectory = path.join(value.root, 'original-controller-bytes'); fs.mkdirSync(outputDirectory)
  await release.recoverPublishedRelease({ ...value, outputDirectory,
    identity: { ...identity, workflowRevision: '8'.repeat(40), runId: '101', runAttempt: 1 },
    release: { assets: [...value.stored.keys()].map(name => ({ name })) },
    readAsset: asset => value.stored.get(asset.name) })
  assert.deepEqual(fs.readFileSync(path.join(outputDirectory, release.MANIFEST)), originalBytes)
  assert.deepEqual(fs.readFileSync(path.join(outputDirectory, release.PROVENANCE)), value.stored.get(release.PROVENANCE))
})

test('signed checkpoint recovery never invokes a changed current browser-host builder', async (t) => {
  const value = publishingFixture(t)
  value.context.upload = file => {
    if (path.basename(file) === release.MANIFEST) throw new Error('manifest interrupted')
    value.stored.set(path.basename(file), fs.readFileSync(file))
  }
  await assert.rejects(() => release.publishAssets(value.context), /manifest interrupted/)
  const outputDirectory = path.join(value.root, 'no-new-builder'); fs.mkdirSync(outputDirectory)
  const previous = process.env.PYTHON
  process.env.PYTHON = process.execPath
  try {
    await release.recoverPublishedRelease({ ...value, outputDirectory,
      sourceRoot: path.join(value.root, 'unavailable-current-builder-inputs'),
      identity: { ...identity, workflowRevision: '8'.repeat(40), runId: '101', runAttempt: 1 },
      release: { assets: [...value.stored.keys()].map(name => ({ name })) },
      readAsset: asset => value.stored.get(asset.name) })
  } finally {
    if (previous === undefined) delete process.env.PYTHON
    else process.env.PYTHON = previous
  }
  for (const name of fs.readdirSync(value.outputDirectory)) {
    assert.deepEqual(fs.readFileSync(path.join(outputDirectory, name)), fs.readFileSync(path.join(value.outputDirectory, name)))
  }
})

test('legacy checkpoint compatibility retains hash verification with and without its committed manifest', async (t) => {
  const value = publishingFixture(t); await release.publishAssets(value.context)
  const proof = JSON.parse(value.stored.get(release.PROVENANCE)); delete proof.manifestBytesBase64; signReceipt(proof, signingKey)
  value.stored.set(release.PROVENANCE, Buffer.from(`${JSON.stringify(proof, null, 2)}\n`))
  for (const committed of [true, false]) {
    const stored = new Map(value.stored)
    if (!committed) stored.delete(release.MANIFEST)
    const outputDirectory = path.join(value.root, `legacy-${committed}`); fs.mkdirSync(outputDirectory)
    await release.recoverPublishedRelease({ ...value, outputDirectory,
      release: { assets: [...stored.keys()].map(name => ({ name })) }, readAsset: asset => stored.get(asset.name) })
    assert.deepEqual(fs.readFileSync(path.join(outputDirectory, release.MANIFEST)), fs.readFileSync(path.join(value.outputDirectory, release.MANIFEST)))
  }
})

test(`checkpoint v${testContractVersion} recovery remains valid when the current manifest constructor changes`, async (t) => {
  const value = publishingFixture(t)
  value.context.upload = file => {
    if (path.basename(file) === release.MANIFEST) throw new Error('manifest interrupted')
    value.stored.set(path.basename(file), fs.readFileSync(file))
  }
  await assert.rejects(() => release.publishAssets(value.context), /manifest interrupted/)
  // Load the real controller with only its moving constructor changed. All
  // recovery, authentication, schema and transport code remains production code.
  const filename = require.resolve('./sidecar-release.cjs')
  const source = fs.readFileSync(filename, 'utf8')
  const marker = 'function createManifest({ version, sourceRevision, artifacts, browserHost }) {'
  assert.equal(source.split(marker).length, 2)
  const Module = require('node:module')
  const changed = new Module(filename, module)
  changed.filename = filename
  changed.paths = module.paths
  changed._compile(source.replace(marker, `${marker}\n  throw new Error('new controller manifest constructor invoked')`), filename)
  const outputDirectory = path.join(value.root, 'frozen-v1-recovery'); fs.mkdirSync(outputDirectory)
  await changed.exports.recoverPublishedRelease({ ...value, outputDirectory,
    identity: { ...identity, workflowRevision: '8'.repeat(40), runId: '101', runAttempt: 1 },
    release: { assets: [...value.stored.keys()].map(name => ({ name })) },
    readAsset: asset => value.stored.get(asset.name) })
  for (const name of [release.MANIFEST, release.PROVENANCE]) {
    assert.deepEqual(fs.readFileSync(path.join(outputDirectory, name)), fs.readFileSync(path.join(value.outputDirectory, name)))
  }
})

test(`frozen v${testContractVersion} recovery validation rejects foreign identities, targets and unsafe asset shapes`, (t) => {
  const value = publishingFixture(t)
  const manifest = JSON.parse(fs.readFileSync(path.join(value.outputDirectory, release.MANIFEST)))
  const proof = JSON.parse(fs.readFileSync(path.join(value.outputDirectory, release.PROVENANCE)))
  const validate = legacySuite ? release.validateManifestV1 : release.validateManifestV2
  assert.equal(validate(manifest, identity, proof.builds), manifest)
  for (const mutate of [
    x => { x.schemaVersion = legacySuite ? 2 : 1 },
    x => { x.version = '2026.10.124' },
    x => { x.sourceRevision = '8'.repeat(40) },
    x => { x.unrecognized = true },
    x => { x.artifacts.pop() },
    x => { x.artifacts[1] = clone(x.artifacts[0]) },
    x => { x.artifacts[0].assetName = '../bamboo' },
    x => { x.artifacts[0].sha256 = 'f'.repeat(64) },
    x => { x.artifacts[0].buildMode = 'api-only' },
    x => { x.browserHost.files[0].path = '../escape' },
    x => { x.browserHost.files.push(clone(x.browserHost.files[0])) },
    x => { x.browserHost.files[0].sizeBytes = 8 * 1024 * 1024 },
    x => { x.browserHost.sha256 = 'not-a-digest' },
  ]) {
    const changed = clone(manifest); mutate(changed)
    assert.throws(() => validate(changed, identity, proof.builds))
  }
})

if (!legacySuite) test('the complete original controller contract suite also passes for frozen four-platform v1', (t) => {
  const env = { ...process.env, BAMBOO_TEST_EXECUTABLE_CONTRACT: '1' }
  // A new CLI test runner must not inherit this runner's child-v8 protocol.
  // Otherwise Node exits successfully without actually running the v1 files.
  delete env.NODE_TEST_CONTEXT
  const child = spawnSync(process.execPath, ['--test', '--test-reporter=tap', __filename], {
    encoding: 'utf8', env, maxBuffer: 8 * 1024 * 1024,
  })
  assert.equal(child.status, 0, child.stdout + child.stderr)
  const counts = Object.fromEntries([...child.stdout.matchAll(/^# (tests|pass|fail|skipped|cancelled|todo) (\d+)$/gm)]
    .map(([, key, value]) => [key, Number(value)]))
  assert.ok(counts.tests >= 27)
  assert.equal(counts.tests, counts.pass)
  assert.equal(counts.fail, 0); assert.equal(counts.skipped, 0); assert.equal(counts.cancelled, 0); assert.equal(counts.todo, 0)
  t.diagnostic('Historical v1 suite: ' + JSON.stringify(counts))
})
