#!/usr/bin/env node
// BAMBOO_SHARED_EXECUTABLES_V2
// One standard, complete Bamboo executable is shared by CLI and desktop users.
const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { assertVersion, validateSource, readBodyReceipt, validateReceipt,
  readAuthenticatedReceipt, assertSigningConfiguration, assertCurrentMain, currentMainWrite, signReceipt, authenticateReceipt, spawnCommand } = require('./crate-release.cjs')

const REPOSITORY = 'bigduu/Bamboo-agent'
const MANIFEST = 'bamboo-sidecars-v2.json'
const HOST_ASSET = 'bamboo-browser-host-v1.tar.gz'
const PROVENANCE = 'bamboo-executables-provenance-v2.json'
const PROVENANCE_KIND = 'bamboo-complete-executables/v2'
const LEGACY_TARGETS = Object.freeze(['aarch64-apple-darwin', 'x86_64-apple-darwin',
  'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu'])
const TARGETS = Object.freeze(['aarch64-apple-darwin', 'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu'])
const CONTRACTS = Object.freeze({
  1: Object.freeze({ version: 1, targets: LEGACY_TARGETS, manifest: 'bamboo-sidecars-v1.json',
    provenance: 'bamboo-executables-provenance-v1.json', kind: 'bamboo-complete-executables/v1' }),
  2: Object.freeze({ version: 2, targets: TARGETS, manifest: MANIFEST, provenance: PROVENANCE, kind: PROVENANCE_KIND }),
})
const HOST_FILES = Object.freeze(['browser-runtime/host.cjs', 'browser-runtime/package-lock.json',
  'browser-runtime/package.json'])
const DIGEST = /^[0-9a-f]{64}$/
const SHA = /^[0-9a-f]{40}$/
const MAX_BINARY = 1024 * 1024 * 1024
const MAX_HOST = 8 * 1024 * 1024
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const artifactName = (target) => {
  assert.ok(LEGACY_TARGETS.includes(target), 'Unsupported sidecar target')
  return `bamboo-${target}${target.includes('windows') ? '.exe' : ''}`
}
const targetOutputKey = (target) => {
  assert.ok(LEGACY_TARGETS.includes(target))
  return target.replaceAll('-', '_')
}

function contractForVersion(version = 2) {
  assert.ok(version === 1 || version === 2, 'Unsupported executable publication contract')
  return CONTRACTS[version]
}

// The accepted source selects its fixed contract. A job input can only confirm
// that selection; it cannot remove a target from an existing release contract.
function sourceContractVersion(sourceRoot = process.cwd()) {
  const file = path.join(sourceRoot, 'scripts/sidecar-release.cjs')
  const stat = fs.lstatSync(file)
  assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size > 0 && stat.size <= 128 * 1024)
  const markers = [...fs.readFileSync(file, 'utf8').matchAll(/^\/\/ BAMBOO_SHARED_EXECUTABLES_V([0-9]+)$/gm)]
  assert.equal(markers.length, 1, 'Missing or ambiguous source executable contract')
  assert.ok(markers[0][1] === '1' || markers[0][1] === '2', 'Unknown source executable contract')
  return Number(markers[0][1])
}

function boundSourceContract(sourceRoot = process.cwd(), env = process.env) {
  const version = sourceContractVersion(sourceRoot)
  if (env.BAMBOO_EXECUTABLE_CONTRACT_VERSION !== undefined) {
    assert.equal(env.BAMBOO_EXECUTABLE_CONTRACT_VERSION, String(version), 'Job contract differs from accepted source')
  } else assert.equal(version, 1, 'A v2 source requires its independently bound job contract')
  return version
}

function assertContractAssets(assets, contractVersion) {
  const contract = contractForVersion(contractVersion)
  for (const asset of assets) {
    if (/^bamboo-(sidecars|executables-provenance)-v[0-9]+\.json$/.test(asset.name)) {
      assert.ok(asset.name === contract.manifest || asset.name === contract.provenance,
        'Release contains an incompatible executable contract')
    }
    if (contractVersion === 2) assert.notEqual(asset.name, artifactName('x86_64-apple-darwin'),
      'Intel assets do not belong to the v2 release contract')
  }
}

function fileIdentity(file, maximum = MAX_BINARY, minimum = 1) {
  const before = fs.lstatSync(file)
  assert.ok(before.isFile() && !before.isSymbolicLink() && before.size >= minimum && before.size <= maximum,
    'Expected a bounded regular release asset')
  const descriptor = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0))
  try {
    const opened = fs.fstatSync(descriptor)
    assert.ok(opened.isFile() && opened.dev === before.dev && opened.ino === before.ino && opened.size === before.size,
      'Release asset changed while opening')
    const hash = crypto.createHash('sha256')
    const buffer = Buffer.alloc(1024 * 1024)
    let bytes = 0, count
    while ((count = fs.readSync(descriptor, buffer, 0, buffer.length, null)) !== 0) {
      hash.update(buffer.subarray(0, count))
      bytes += count
      assert.ok(bytes <= maximum, 'Release asset grew beyond its bound')
    }
    const after = fs.fstatSync(descriptor)
    assert.ok(bytes === before.size && after.size === before.size && after.mtimeMs === before.mtimeMs,
      'Release asset changed while hashing')
    return { sha256: hash.digest('hex'), sizeBytes: bytes }
  } finally { fs.closeSync(descriptor) }
}

function requireDigest(value, maximum, minimum = 1) {
  assert.ok(value && DIGEST.test(value.sha256) && Number.isSafeInteger(value.sizeBytes) &&
    value.sizeBytes >= minimum && value.sizeBytes <= maximum, 'Invalid asset checksum or size')
}

function createBrowserHost({ sourceRoot, outputDirectory }) {
  const output = path.resolve(outputDirectory, HOST_ASSET)
  const executable = process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3')
  const host = JSON.parse(execute(executable, [path.join(__dirname, 'sidecar-browser-host.py'),
    path.resolve(sourceRoot), output], { maxBuffer: 1024 * 1024 }))
  assert.deepEqual(fileIdentity(output, MAX_HOST), { sha256: host.sha256, sizeBytes: host.sizeBytes })
  return host
}

// This pure function also supports explicitly synthetic cross-repository
// contract fixtures. Publication separately requires real build provenance.
function createManifest({ version, sourceRevision, artifacts, browserHost }) {
  return createManifestForContract({ version, sourceRevision, artifacts, browserHost }, 2)
}

function createManifestForContract({ version, sourceRevision, artifacts, browserHost }, contractVersion) {
  const contract = contractForVersion(contractVersion)
  assertVersion(version)
  assert.match(version, /^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/)
  assert.match(sourceRevision, SHA)
  assert.ok(Array.isArray(artifacts))
  const sorted = [...artifacts].sort((a, b) => a.target.localeCompare(b.target, 'en'))
  assert.deepEqual(sorted.map((item) => item.target), contract.targets,
    'Exactly the selected contract targets are required, without duplicates')
  const canonicalArtifacts = sorted.map((item) => {
    assert.equal(item.assetName, artifactName(item.target))
    assert.equal(item.format, 'raw')
    assert.equal(item.buildMode, 'embedded')
    assert.equal(item.embeddedFrontend, true)
    requireDigest(item, MAX_BINARY, 65536)
    return { target: item.target, buildMode: 'embedded', embeddedFrontend: true,
      format: 'raw', assetName: item.assetName, sha256: item.sha256, sizeBytes: item.sizeBytes }
  })
  assert.equal(browserHost?.assetName, HOST_ASSET)
  requireDigest(browserHost, MAX_HOST)
  assert.deepEqual(browserHost.files?.map((file) => file.path).sort(), HOST_FILES)
  assert.ok(browserHost.files.reduce((sum, file) => sum + file.sizeBytes, 0) <= MAX_HOST)
  for (const file of browserHost.files) requireDigest(file, MAX_HOST)
  return { schemaVersion: contract.version, version, sourceRevision, artifacts: canonicalArtifacts,
    browserHost: { assetName: HOST_ASSET, sha256: browserHost.sha256, sizeBytes: browserHost.sizeBytes,
      files: [...browserHost.files].sort((a, b) => a.path.localeCompare(b.path, 'en'))
        .map(({ path, sha256, sizeBytes }) => ({ path, sha256, sizeBytes })) } }
}

function manifestBytes(value, contractVersion = 2) {
  return Buffer.from(`${JSON.stringify(contractVersion === 2 ? createManifest(value) :
    createManifestForContract(value, contractVersion), null, 2)}\n`)
}

// Frozen recovery contract for already-signed schemaVersion 1 bytes. Future
// constructors/serializers must not redefine this historical asset schema.
function validateManifestV1(value, identity, builds) {
  const keys = (object, expected) => assert.deepEqual(Object.keys(object).sort(), [...expected].sort())
  keys(value, ['schemaVersion', 'version', 'sourceRevision', 'artifacts', 'browserHost'])
  assert.equal(value.schemaVersion, 1)
  assert.match(value.version, /^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/)
  assert.equal(value.version, identity.version)
  assert.match(value.sourceRevision, /^[0-9a-f]{40}$/)
  assert.equal(value.sourceRevision, identity.sourceRevision)
  const targets = ['aarch64-apple-darwin', 'x86_64-apple-darwin',
    'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu']
  assert.ok(Array.isArray(value.artifacts))
  assert.deepEqual(value.artifacts.map(item => item.target).sort(), targets)
  for (const item of value.artifacts) {
    keys(item, ['target', 'buildMode', 'embeddedFrontend', 'format', 'assetName', 'sha256', 'sizeBytes'])
    const receipt = builds.find(build => build.target === item.target)
    assert.ok(receipt)
    for (const key of ['target', 'assetName', 'sha256', 'sizeBytes']) assert.equal(item[key], receipt[key])
    assert.equal(item.buildMode, 'embedded')
    assert.equal(item.embeddedFrontend, true)
    assert.equal(item.format, 'raw')
    requireDigest(item, 1024 * 1024 * 1024, 65536)
  }
  const host = value.browserHost
  keys(host, ['assetName', 'sha256', 'sizeBytes', 'files'])
  assert.equal(host.assetName, 'bamboo-browser-host-v1.tar.gz')
  requireDigest(host, 8 * 1024 * 1024)
  assert.ok(Array.isArray(host.files))
  assert.deepEqual(host.files.map(file => file.path).sort(),
    ['browser-runtime/host.cjs', 'browser-runtime/package-lock.json', 'browser-runtime/package.json'])
  for (const file of host.files) {
    keys(file, ['path', 'sha256', 'sizeBytes'])
    requireDigest(file, 8 * 1024 * 1024)
  }
  assert.ok(host.files.reduce((sum, file) => sum + file.sizeBytes, 0) <= 8 * 1024 * 1024)
  return value
}

// This version also remains independent of the moving manifest constructor.
function validateManifestV2(value, identity, builds) {
  const keys = (object, expected) => assert.deepEqual(Object.keys(object).sort(), [...expected].sort())
  keys(value, ['schemaVersion', 'version', 'sourceRevision', 'artifacts', 'browserHost'])
  assert.equal(value.schemaVersion, 2)
  assert.match(value.version, /^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/)
  assert.equal(value.version, identity.version)
  assert.match(value.sourceRevision, /^[0-9a-f]{40}$/)
  assert.equal(value.sourceRevision, identity.sourceRevision)
  const targets = ['aarch64-apple-darwin', 'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu']
  assert.deepEqual(builds?.map(item => item.target), targets)
  assert.ok(Array.isArray(value.artifacts))
  assert.deepEqual(value.artifacts.map(item => item.target), targets)
  for (const item of value.artifacts) {
    keys(item, ['target', 'buildMode', 'embeddedFrontend', 'format', 'assetName', 'sha256', 'sizeBytes'])
    const receipt = builds.find(build => build.target === item.target)
    assert.ok(receipt)
    for (const key of ['target', 'assetName', 'sha256', 'sizeBytes']) assert.equal(item[key], receipt[key])
    assert.equal(item.buildMode, 'embedded')
    assert.equal(item.embeddedFrontend, true)
    assert.equal(item.format, 'raw')
    requireDigest(item, 1024 * 1024 * 1024, 65536)
  }
  const host = value.browserHost
  keys(host, ['assetName', 'sha256', 'sizeBytes', 'files'])
  assert.equal(host.assetName, 'bamboo-browser-host-v1.tar.gz')
  requireDigest(host, 8 * 1024 * 1024)
  assert.ok(Array.isArray(host.files))
  assert.deepEqual(host.files.map(file => file.path),
    ['browser-runtime/host.cjs', 'browser-runtime/package-lock.json', 'browser-runtime/package.json'])
  for (const file of host.files) {
    keys(file, ['path', 'sha256', 'sizeBytes'])
    requireDigest(file, 8 * 1024 * 1024)
  }
  assert.ok(host.files.reduce((sum, file) => sum + file.sizeBytes, 0) <= 8 * 1024 * 1024)
  return value
}

function binaryArchitecture(file, target) {
  assert.ok(LEGACY_TARGETS.includes(target))
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0))
  try {
    const header = Buffer.alloc(64)
    assert.equal(fs.readSync(fd, header, 0, header.length, 0), header.length)
    if (target.endsWith('apple-darwin')) {
      assert.equal(header.readUInt32LE(0), 0xfeedfacf, 'Expected a thin 64-bit Mach-O executable')
      assert.equal(header.readUInt32LE(4), target.startsWith('aarch64') ? 0x0100000c : 0x01000007,
        'Mach-O architecture differs from the advertised target')
      assert.equal(header.readUInt32LE(12), 2, 'Expected Mach-O MH_EXECUTE')
    } else if (target.includes('windows')) {
      assert.equal(header.toString('ascii', 0, 2), 'MZ', 'Expected a PE executable')
      const offset = header.readUInt32LE(60)
      assert.ok(offset >= 64 && offset <= 1024 * 1024)
      const pe = Buffer.alloc(26)
      assert.equal(fs.readSync(fd, pe, 0, pe.length, offset), pe.length)
      assert.equal(pe.readUInt32LE(0), 0x4550, 'Invalid PE signature')
      assert.equal(pe.readUInt16LE(4), 0x8664, 'Expected PE AMD64')
      assert.equal(pe.readUInt16LE(24), 0x20b, 'Expected PE32+')
      assert.ok((pe.readUInt16LE(22) & 0x2002) === 2, 'Expected an executable, not a DLL')
    } else {
      assert.equal(header.subarray(0, 6).toString('hex'), '7f454c460201', 'Expected ELF64 little-endian')
      assert.equal(header.readUInt16LE(18), 62, 'Expected ELF x86-64')
      assert.ok([2, 3].includes(header.readUInt16LE(16)), 'Expected an ELF executable or PIE')
    }
    return target
  } finally { fs.closeSync(fd) }
}

function execute(program, args, options = {}) {
  // Reuse the publisher's credential policy: only gh receives GitHub auth;
  // signing/registry authority never reaches Git, Python, or other children.
  const result = spawnCommand([program, ...args], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, ...options })
  assert.ok(!result.error && result.status === 0, `${program} failed (${result.status}): ${result.stderr || result.error?.message || ''}`)
  return result.stdout
}

function git(args) { return execute('git', args).trim() }

// Job outputs select the allocator's existing release. Neither the matrix nor
// the desktop consumer allocates a version, creates a tag, or publishes crates.
function workflowIdentity(env = process.env) {
  assert.equal(env.GITHUB_REPOSITORY, REPOSITORY)
  const sourceRevision = env.SOURCE_REVISION
  const version = assertVersion(env.TARGET_VERSION)
  assert.match(version, /^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/)
  assert.match(sourceRevision, SHA)
  const event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8'))
  const validation = env.SOURCE_VALIDATION ? JSON.parse(env.SOURCE_VALIDATION) : undefined
  validateSource({ eventName: env.GITHUB_EVENT_NAME, event, repository: REPOSITORY, sourceRevision,
    sourceTree: git(['rev-parse', 'HEAD^{tree}']), validation })
  assert.match(env.GITHUB_RUN_ID, /^[1-9][0-9]*$/)
  assert.match(env.GITHUB_RUN_ATTEMPT, /^[1-9][0-9]*$/)
  assert.match(env.GITHUB_WORKFLOW_SHA, SHA)
  assert.equal(git(['rev-parse', 'HEAD']), sourceRevision)
  assert.equal(git(['status', '--porcelain', '--untracked-files=all']), '', 'Release checkout must start clean')
  return { repository: REPOSITORY, version, sourceRevision, sourceTree: git(['rev-parse', 'HEAD^{tree}']),
    workflowRevision: env.GITHUB_WORKFLOW_SHA, runId: env.GITHUB_RUN_ID,
    runAttempt: Number(env.GITHUB_RUN_ATTEMPT), ciRun: (validation?.ciRun || event.workflow_run).html_url,
    ...(validation ? { validation } : {}) }
}

function gh(route, options = {}) {
  return execute('gh', ['api', `repos/${REPOSITORY}/${route}`], options)
}

function publishedRelease(identity, { signingKey, signingKeyDigest, request = gh } = {}) {
  const release = JSON.parse(request(`releases/tags/v${identity.version}`))
  const tag = JSON.parse(request(`git/ref/tags/v${identity.version}`))
  assert.equal(release.tag_name, `v${identity.version}`)
  assert.equal(release.draft, false, 'Crate publication must complete before executable publication')
  assert.equal(release.prerelease, false)
  assert.equal(release.target_commitish, identity.sourceRevision)
  assert.equal(tag.object?.type, 'commit', 'Release tag must directly bind the accepted source')
  assert.equal(tag.object.sha, identity.sourceRevision)
  const receipt = signingKey ? (() => {
    assertSigningConfiguration(signingKey, signingKeyDigest)
    return readAuthenticatedReceipt(release, signingKey, { required: true })
  })() : readBodyReceipt(release)
  assert.equal(receipt?.identity?.repository, REPOSITORY)
  assert.equal(receipt.identity.sourceRevision, identity.sourceRevision)
  validateReceipt(receipt, receipt.identity, receipt.crates, identity.version)
  assert.equal(receipt.automatic, true)
  assert.equal(receipt.completed, true)
  assert.equal(receipt.ciRun, identity.ciRun)
  if (identity.validation) assert.deepEqual(receipt.validation, identity.validation)
  assert.ok(receipt.crates.includes('bamboo-agent'))
  assert.ok(Array.isArray(release.assets))
  assert.equal(new Set(release.assets.map((asset) => asset.name)).size, release.assets.length)
  return { release, receipt }
}

function assetBytes(asset, maximum) {
  assert.ok(Number.isSafeInteger(asset.id) && asset.id > 0)
  assert.ok(Number.isSafeInteger(asset.size) && asset.size > 0 && asset.size <= maximum)
  const bytes = execute('gh', ['api', `repos/${REPOSITORY}/releases/assets/${asset.id}`,
    '-H', 'Accept: application/octet-stream'], { encoding: null, maxBuffer: maximum })
  assert.equal(bytes.length, asset.size)
  return bytes
}

function validateBuildReceipt(receipt, identity, target) {
  assert.equal(receipt?.schemaVersion, 1)
  if (identity.validation) assert.deepEqual(receipt.validation, identity.validation)
  for (const key of ['repository', 'version', 'sourceRevision', 'sourceTree', 'workflowRevision', 'runId', 'ciRun']) {
    assert.equal(receipt[key], identity[key], `Executable build provenance mismatch: ${key}`)
  }
  assert.ok(Number.isSafeInteger(receipt.runAttempt) && receipt.runAttempt >= 1 && receipt.runAttempt <= identity.runAttempt)
  assert.equal(receipt.target, target)
  assert.equal(receipt.buildMode, 'embedded')
  assert.equal(receipt.embeddedFrontend, true)
  assert.equal(receipt.format, 'raw')
  assert.equal(receipt.assetName, artifactName(target))
  requireDigest(receipt, MAX_BINARY, 65536)
  assert.deepEqual(receipt.command, ['cargo', 'build', '--locked', '--release', '--verbose', '--bin', 'bamboo',
    '--target', target, '--message-format=json-render-diagnostics'])
  assert.equal(receipt.runtime?.version, `bamboo ${identity.version}`)
  assert.equal(receipt.runtime?.status, 'passed')
  assert.match(receipt.cargoMessagesSha256, DIGEST)
  assert.match(receipt.stampedDiffSha256, DIGEST)
  assert.match(receipt.rustc, /^rustc /)
  assert.match(receipt.cargo, /^cargo /)
  assert.match(receipt.frontendBytes?.manifestSha256, DIGEST)
  assert.match(receipt.frontendBytes?.archiveSha256, DIGEST)
  return receipt
}

function expectedBuildBindings(outputs, contractVersion = 2) {
  const contract = contractForVersion(contractVersion)
  assert.ok(outputs && typeof outputs === 'object' && !Array.isArray(outputs), 'Require independent build-job outputs')
  outputs = { ...outputs }
  // GitHub declares the legacy output as well so the same reviewed workflow
  // can recover v1. The absent v2 Intel job may supply only its empty output.
  const intel = targetOutputKey('x86_64-apple-darwin')
  if (contractVersion === 2 && Object.hasOwn(outputs, intel)) {
    assert.equal(outputs[intel], '', 'A v2 release cannot acquire an Intel build binding')
    delete outputs[intel]
  }
  assert.deepEqual(Object.keys(outputs).sort(), contract.targets.map(targetOutputKey).sort(),
    'Require exactly the contract native build-job output bindings')
  return new Map(contract.targets.map((target) => {
    assert.equal(typeof outputs[targetOutputKey(target)], 'string')
    const value = JSON.parse(outputs[targetOutputKey(target)])
    requireDigest(value, MAX_BINARY, 65536)
    assert.match(value.receiptSha256, DIGEST)
    assert.ok(Number.isSafeInteger(value.runAttempt) && value.runAttempt >= 1, 'Invalid native-job attempt binding')
    return [target, value]
  }))
}

function assembleRelease({ identity, inputDirectory, outputDirectory, sourceRoot, frontendBytes, expectedBuilds, signingKey,
  contractVersion = 2 }) {
  const contract = contractForVersion(contractVersion)
  const bindings = expectedBuildBindings(expectedBuilds, contractVersion)
  assert.deepEqual(fs.readdirSync(inputDirectory).sort(), [...contract.targets].sort(), 'Missing or unexpected platform transport directory')
  const receipts = contract.targets.map((target) => {
    const directory = path.join(inputDirectory, target)
    assert.ok(fs.lstatSync(directory).isDirectory() && !fs.lstatSync(directory).isSymbolicLink())
    assert.deepEqual(fs.readdirSync(directory).sort(), [artifactName(target), 'build-receipt.json'].sort())
    const receiptPath = path.join(directory, 'build-receipt.json')
    assert.equal(fileIdentity(receiptPath, 1024 * 1024).sha256, bindings.get(target).receiptSha256,
      'Transported receipt differs from the independent build-job output')
    const receipt = validateBuildReceipt(JSON.parse(fs.readFileSync(receiptPath)), identity, target)
    assert.equal(receipt.runAttempt, bindings.get(target).runAttempt, 'Transport differs from the bound native-job attempt')
    assert.equal(receipt.sha256, bindings.get(target).sha256, 'Executable digest differs from the independent build job')
    assert.equal(receipt.sizeBytes, bindings.get(target).sizeBytes)
    assert.deepEqual(receipt.frontendBytes, frontendBytes, 'Platform binary embedded different release frontend bytes')
    const binary = path.join(directory, artifactName(target))
    assert.deepEqual(fileIdentity(binary, MAX_BINARY, 65536), { sha256: receipt.sha256, sizeBytes: receipt.sizeBytes })
    binaryArchitecture(binary, target)
    fs.copyFileSync(binary, path.join(outputDirectory, receipt.assetName), fs.constants.COPYFILE_EXCL)
    return receipt
  })
  const browserHost = createBrowserHost({ sourceRoot, outputDirectory })
  const manifest = createManifestForContract({ ...identity, artifacts: receipts, browserHost }, contractVersion)
  const originalManifestBytes = manifestBytes(manifest, contractVersion)
  fs.writeFileSync(path.join(outputDirectory, contract.manifest), originalManifestBytes, { flag: 'wx' })
  // A failed final upload can rerun without changing immutable provenance.
  // Record the attempts that actually built these bytes, not the uploader's
  // later retry number (the complete per-target attempts remain in builds).
  const provenance = signReceipt({ schemaVersion: contract.version, kind: contract.kind, ...identity,
    runAttempt: Math.max(...receipts.map((item) => item.runAttempt)),
    manifestSha256: sha256(originalManifestBytes),
    manifestBytesBase64: originalManifestBytes.toString('base64'), builds: receipts }, signingKey)
  fs.writeFileSync(path.join(outputDirectory, contract.provenance),
    `${JSON.stringify(provenance, null, 2)}\n`, { flag: 'wx' })
  return manifest
}

// All binaries and the deterministic host precede provenance in the upload
// order. Once that signed checkpoint exists, a later full workflow rerun can
// finish the original manifest even if its new builds have different bytes.
// Preserve the authenticated original build run/controller even across a new
// recovery dispatch. Never adopt unsigned, foreign-source/CI or incomplete assets.
async function recoverPublishedRelease({ identity, release, outputDirectory, sourceRoot,
  frontendBytes, signingKey, readAsset, contractVersion = 2 }) {
  const contract = contractForVersion(contractVersion)
  assertContractAssets(release.assets, contractVersion)
  const checkpoint = release.assets.find((asset) => asset.name === contract.provenance)
  if (!checkpoint) return null
  const bytes = await readAsset(checkpoint, 1024 * 1024)
  assert.ok(bytes.length <= 1024 * 1024)
  const proof = authenticateReceipt(JSON.parse(bytes), signingKey)
  if (identity.validation) assert.deepEqual(proof.validation, identity.validation)
  assert.equal(proof.schemaVersion, contract.version)
  assert.equal(proof.kind, contract.kind)
  for (const key of ['repository', 'version', 'sourceRevision', 'sourceTree', 'ciRun']) {
    assert.equal(proof[key], identity[key], `Recovery provenance mismatch: ${key}`)
  }
  assert.match(proof.workflowRevision, SHA)
  assert.match(proof.runId, /^[1-9][0-9]*$/)
  assert.ok(Number.isSafeInteger(proof.runAttempt) && proof.runAttempt >= 1)
  // Authentication binds the original controller/run and each platform receipt.
  // The caller already revalidates current protected-main promotion; a later
  // dispatch must not relabel the immutable checkpoint as its own fresh build.
  const originalBuildIdentity = { ...identity, workflowRevision: proof.workflowRevision,
    runId: proof.runId, runAttempt: proof.runAttempt }
  assert.deepEqual(proof.builds?.map((item) => item.target), contract.targets)
  for (const item of proof.builds) {
    validateBuildReceipt(item, originalBuildIdentity, item.target)
    assert.deepEqual(item.frontendBytes, frontendBytes)
  }
  assert.equal(proof.runAttempt, Math.max(...proof.builds.map((item) => item.runAttempt)))
  let originalManifestBytes, legacyHostCreated = false
  if (proof.manifestBytesBase64 !== undefined) {
    assert.equal(typeof proof.manifestBytesBase64, 'string')
    originalManifestBytes = Buffer.from(proof.manifestBytesBase64, 'base64')
    assert.equal(originalManifestBytes.toString('base64'), proof.manifestBytesBase64)
  } else {
    const committed = release.assets.find((asset) => asset.name === contract.manifest)
    if (committed) originalManifestBytes = await readAsset(committed, 1024 * 1024)
    else {
      // Legacy checkpoints lack the original serialized manifest. Keep their
      // existing deterministic compatibility route; new checkpoints never use it.
      const browserHost = createBrowserHost({ sourceRoot, outputDirectory })
      originalManifestBytes = manifestBytes({ ...identity, artifacts: proof.builds, browserHost }, contractVersion)
      legacyHostCreated = true
    }
  }
  assert.ok(originalManifestBytes.length > 0 && originalManifestBytes.length <= 1024 * 1024)
  assert.equal(sha256(originalManifestBytes), proof.manifestSha256,
    'Recovery manifest does not match the authenticated checkpoint')
  const manifest = JSON.parse(originalManifestBytes.toString('utf8'))
  // Validate the frozen contract and signed build identities without reserializing
  // the original bytes or running the current controller's archive builder.
  if (contractVersion === 1) validateManifestV1(manifest, identity, proof.builds)
  else validateManifestV2(manifest, identity, proof.builds)
  const required = [...proof.builds, manifest.browserHost]
  assert.ok(required.every((item) => release.assets.some((asset) => asset.name === item.assetName)),
    'Authenticated checkpoint has missing release assets')
  for (const item of required) {
    const asset = release.assets.find((asset) => asset.name === item.assetName)
    const data = await readAsset(asset, item.sizeBytes)
    assert.deepEqual({ sha256: sha256(data), sizeBytes: data.length },
      { sha256: item.sha256, sizeBytes: item.sizeBytes }, 'Recovery asset differs from authenticated provenance')
    const file = path.join(outputDirectory, item.assetName)
    if (item.assetName === HOST_ASSET && legacyHostCreated) {
      assert.deepEqual(fs.readFileSync(file), data)
    } else fs.writeFileSync(file, data, { flag: 'wx' })
    if (item.target) binaryArchitecture(file, item.target)
  }
  fs.writeFileSync(path.join(outputDirectory, contract.provenance), bytes, { flag: 'wx' })
  fs.writeFileSync(path.join(outputDirectory, contract.manifest), originalManifestBytes, { flag: 'wx' })
  return manifest
}

// Never clobber release assets. An interrupted upload can resume only when the
// server still has these exact bytes. The manifest is the final commit marker.
async function publishAssets({ directory, release, readAsset, upload, listAssets, contractVersion = 2 }) {
  const contract = contractForVersion(contractVersion)
  const names = [...contract.targets.map(artifactName), HOST_ASSET, contract.provenance, contract.manifest]
  assert.deepEqual(fs.readdirSync(directory).sort(), [...names].sort())
  const expected = new Map(names.map((name) => [name, fileIdentity(path.join(directory, name),
    name.startsWith('bamboo-') && contract.targets.some((target) => artifactName(target) === name) ? MAX_BINARY : MAX_HOST)]))
  const assertExisting = async (assets) => {
    assertContractAssets(assets, contractVersion)
    assert.equal(new Set(assets.map((asset) => asset.name)).size, assets.length)
    for (const name of names) {
      const existing = assets.find((asset) => asset.name === name)
      if (!existing) continue
      const bytes = await readAsset(existing, expected.get(name).sizeBytes)
      assert.deepEqual({ sha256: sha256(bytes), sizeBytes: bytes.length }, expected.get(name),
        `Existing release asset differs; refusing replacement: ${name}`)
    }
  }
  let assets = release.assets
  await assertExisting(assets) // Detect every conflict before the first write.
  if (assets.some((asset) => asset.name === contract.manifest)) {
    assert.ok(names.every((name) => assets.some((asset) => asset.name === name)), 'Committed manifest has missing release assets')
    return { reused: true }
  }
  for (const name of names) {
    if (assets.some((asset) => asset.name === name)) continue
    await upload(path.join(directory, name))
    assets = await listAssets()
    const uploaded = assets.find((asset) => asset.name === name)
    assert.ok(uploaded, 'Uploaded asset did not become visible')
    const bytes = await readAsset(uploaded, expected.get(name).sizeBytes)
    assert.deepEqual({ sha256: sha256(bytes), sizeBytes: bytes.length }, expected.get(name), 'Uploaded bytes differ')
  }
  await assertExisting(assets)
  return { reused: false }
}

async function main() {
  assert.equal(process.argv[2], 'publish')
  const identity = workflowIdentity()
  const contractVersion = boundSourceContract()
  const contract = contractForVersion(contractVersion)
  const guard = () => assertCurrentMain(REPOSITORY, identity.sourceRevision)
  await guard()
  const { release, receipt } = publishedRelease(identity, { signingKey: process.env.BAMBOO_RELEASE_SIGNING_KEY,
    signingKeyDigest: process.env.BAMBOO_RELEASE_SIGNING_KEY_SHA256 })
  assert.ok(process.env.BAMBOO_RELEASE_SIGNING_KEY, 'Final asset publication requires the existing protected signing authority')
  const directory = path.join(process.env.RUNNER_TEMP, 'bamboo-executable-release')
  fs.mkdirSync(directory) // Fresh output; never adopt unexplained runner files.
  const recovery = { identity, release, outputDirectory: directory, sourceRoot: process.cwd(),
    frontendBytes: receipt.frontendBytes, signingKey: process.env.BAMBOO_RELEASE_SIGNING_KEY, readAsset: assetBytes,
    contractVersion }
  const manifest = await recoverPublishedRelease(recovery) || assembleRelease({ ...recovery,
    inputDirectory: path.join(process.env.RUNNER_TEMP, 'bamboo-executable-inputs'),
    outputDirectory: directory, sourceRoot: process.cwd(), frontendBytes: receipt.frontendBytes,
    expectedBuilds: JSON.parse(process.env.EXPECTED_BUILDS) })
  assert.equal(git(['rev-parse', 'HEAD']), identity.sourceRevision)
  assert.equal(git(['status', '--porcelain', '--untracked-files=all']), '', 'Source drift blocks asset uploads')
  await publishAssets({ directory, release, readAsset: assetBytes, contractVersion,
    upload: (file) => currentMainWrite(guard, () => execute('gh', ['release', 'upload', `v${identity.version}`, file, '--repo', REPOSITORY])),
    listAssets: () => JSON.parse(gh(`releases/${release.id}/assets?per_page=100`)) })
  assert.equal(git(['rev-parse', 'HEAD']), identity.sourceRevision)
  assert.equal(git(['status', '--porcelain', '--untracked-files=all']), '')
  console.log(JSON.stringify({ status: 'published-and-verified', version: identity.version,
    sourceRevision: identity.sourceRevision, manifestSha256: fileIdentity(path.join(directory, contract.manifest), 1024 * 1024).sha256,
    contractVersion, targets: contract.targets }))
}

module.exports = { REPOSITORY, MANIFEST, HOST_ASSET, PROVENANCE, TARGETS, LEGACY_TARGETS, HOST_FILES, MAX_BINARY, MAX_HOST,
  contractForVersion, sourceContractVersion, boundSourceContract,
  artifactName, targetOutputKey, sha256, fileIdentity, createBrowserHost, createManifest, manifestBytes, validateManifestV1, validateManifestV2,
  binaryArchitecture, execute, git, workflowIdentity, publishedRelease, assetBytes,
  validateBuildReceipt, expectedBuildBindings, assembleRelease, recoverPublishedRelease, publishAssets }
if (require.main === module) main().catch((error) => { console.error(error.message); process.exitCode = 1 })
