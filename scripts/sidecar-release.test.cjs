const assert = require('node:assert/strict')
const test = require('node:test')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const { spawnSync } = require('node:child_process')
const { pathToFileURL } = require('node:url')
const release = require('./sidecar-release.cjs')
const { nativeTarget, buildEnvironment, verifyBuildMessages, verifyStampedVersions } = require('./sidecar-build.cjs')
const { signReceipt, receiptBody } = require('./crate-release.cjs')

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

function inputs(t) {
  const root = temporary(t)
  const sourceRoot = path.join(root, 'source'), inputDirectory = path.join(root, 'inputs'), outputDirectory = path.join(root, 'output')
  for (const dir of [sourceRoot, inputDirectory, outputDirectory, path.join(sourceRoot, 'browser-runtime')]) fs.mkdirSync(dir)
  for (const file of release.HOST_FILES) fs.writeFileSync(path.join(sourceRoot, file), `fixture-only ${file}\n`)
  const receipts = [], expectedBuilds = {}
  for (const target of release.TARGETS) {
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
  return { root, sourceRoot, inputDirectory, outputDirectory, receipts, identity, frontendBytes, expectedBuilds, signingKey }
}

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
  assert.throws(() => release.expectedBuildBindings(missing), /four unique/)
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
  for (const [platform, arch, target] of [['darwin', 'arm64', release.TARGETS[0]], ['darwin', 'x64', release.TARGETS[1]],
    ['win32', 'x64', release.TARGETS[2]], ['linux', 'x64', release.TARGETS[3]]]) assert.equal(nativeTarget(platform, arch), target)
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

test('release assets upload once, verify each result, and commit the manifest last; identical reruns do no writes', async (t) => {
  const value = publishingFixture(t)
  assert.deepEqual(await release.publishAssets(value.context), { reused: false })
  assert.equal(value.uploads.at(-1), release.MANIFEST)
  assert.equal(value.uploads.length, 7)
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

test('recovery rejects unsigned/tampered/foreign checkpoints, missing assets and corrupt bytes without any release writes', async (t) => {
  const value = publishingFixture(t); await release.publishAssets(value.context)
  const original = value.stored.get(release.PROVENANCE)
  const target = release.artifactName(release.TARGETS[0])
  const variants = [
    (stored) => { const p = JSON.parse(original); delete p.authentication; stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.builds[0].sha256 = '8'.repeat(64); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
    (stored) => { const p = JSON.parse(original); p.runId = '999'; signReceipt(p, signingKey); stored.set(release.PROVENANCE, Buffer.from(JSON.stringify(p))) },
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

test('workflow retains the original successful-CI authority, one allocator/queue and the four standard native builds', () => {
  const source = fs.readFileSync('.github/workflows/publish-crate.yml', 'utf8')
  const matrix = source.split('  executables:\n')[1].split('  publish-executables:\n')[0]
  const final = source.split('  publish-executables:\n')[1]
  assert.match(source, /group: bamboo-crate-publication\n  queue: max\n  cancel-in-progress: false/)
  assert.equal(source.split('bamboo-release-controller/scripts/crate-release.cjs" plan').length - 1, 1)
  assert.match(matrix, /needs: automatic/)
  assert.doesNotMatch(matrix, /contents: write|secrets\.|api-only|cargo publish/)
  for (const target of release.TARGETS) assert.ok(matrix.includes(`target: ${target}\n`))
  for (const runner of ['macos-latest', 'macos-15-intel', 'ubuntu-22.04', 'windows-latest']) assert.ok(matrix.includes(`os: ${runner}\n`))
  assert.match(matrix, /persist-credentials: false/)
  for (const target of release.TARGETS) {
    const key = release.targetOutputKey(target)
    assert.ok(matrix.includes(key + ': ${{ steps.build.outputs.' + key + ' }}'))
    assert.ok(final.includes('name: bamboo-executable-' + target + '-attempt-${{ fromJSON(needs.executables.outputs.' + key + ').runAttempt }}'))
    assert.ok(final.includes('path: ${{ runner.temp }}/bamboo-executable-inputs/' + target))
  }
  assert.ok(matrix.includes('name: bamboo-executable-${{ matrix.target }}-attempt-${{ github.run_attempt }}'))
  assert.doesNotMatch(matrix, /overwrite: true/)
  for (const match of source.matchAll(/uses: ([^\s]+)@([^\s]+)/g)) assert.match(match[2], /^[0-9a-f]{40}$/, 'Release actions must use immutable reviewed commits')
  assert.ok(final.includes('EXPECTED_BUILDS: ${{ toJSON(needs.executables.outputs) }}'))
  assert.match(final, /needs: \[automatic, executables\]/)
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
  const files = { 'scripts/crate-release.cjs': '', 'scripts/sidecar-build.cjs': '// BAMBOO_SHARED_EXECUTABLES_V1',
    'scripts/sidecar-release.cjs': '// BAMBOO_SHARED_EXECUTABLES_V1',
    'scripts/sidecar-browser-host.py': '# BAMBOO_SHARED_EXECUTABLES_V1',
    '.github/workflows/publish-crate.yml': '# BAMBOO_SHARED_EXECUTABLES_V1' }
  for (const [file, marker] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
    fs.writeFileSync(path.join(root, file), marker + '\n')
  }
  const run = () => spawnSync(process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3'),
    ['-c', script], { cwd: root, encoding: 'utf8', env: { PATH: process.env.PATH, PYTHONDONTWRITEBYTECODE: '1' } })
  assert.equal(run().status, 0)
  for (const [file, marker] of Object.entries(files)) {
    const target = path.join(root, file); fs.rmSync(target)
    assert.notEqual(run().status, 0, `Missing source producer must stop before publication: ${file}`)
    if (marker) {
      fs.writeFileSync(target, 'legacy producer without this ABI\n')
      assert.notEqual(run().status, 0, `An earlier incomplete producer must stop: ${file}`)
    }
    fs.writeFileSync(target, marker + '\n')
  }
})
