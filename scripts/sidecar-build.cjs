#!/usr/bin/env node
// BAMBOO_SHARED_EXECUTABLES_V2
// Build the standard release executable once per native target. The output is
// shared by Bamboo users and Bodhi; no desktop-specific Cargo variant exists.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { contractForVersion, boundSourceContract, artifactName, targetOutputKey, fileIdentity, sha256, binaryArchitecture,
  execute, git, workflowIdentity, preparedFrontend } = require('./sidecar-release.cjs')

function nativeTarget(platform = process.platform, arch = process.arch) {
  const targets = { 'darwin/arm64': 'aarch64-apple-darwin', 'darwin/x64': 'x86_64-apple-darwin',
    'linux/x64': 'x86_64-unknown-linux-gnu', 'win32/x64': 'x86_64-pc-windows-msvc' }
  assert.ok(targets[`${platform}/${arch}`], 'Unsupported native release runner')
  return targets[`${platform}/${arch}`]
}

function buildEnvironment(env, targetDirectory) {
  const result = Object.fromEntries(Object.entries(env).filter(([name]) =>
    !/TOKEN|SECRET|SIGNING_KEY|PASSWORD|API_KEY|AUTHORIZATION|CREDENTIAL/i.test(name)))
  // The runner may carry cache paths but must not change standard release code.
  for (const name of Object.keys(result)) {
    assert.ok(!/^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTC_WRAPPER|RUSTC_WORKSPACE_WRAPPER|RUSTC_BOOTSTRAP|CARGO_PROFILE_|CARGO_BUILD_RUSTFLAGS)/.test(name),
      'Executable publication does not accept compiler/profile overrides')
  }
  assert.ok(!env.BAMBOO_FRONTEND_BUILD_MODE || env.BAMBOO_FRONTEND_BUILD_MODE === 'embedded',
    'Standard release executables must embed the frontend')
  return { ...result, BAMBOO_FRONTEND_BUILD_MODE: 'embedded', CARGO_TARGET_DIR: targetDirectory }
}

function verifyBuildMessages(messages, { binary, targetDirectory, serverPackageId, frontendBytes }) {
  const builds = messages.filter((item) => item.reason === 'build-script-executed' && item.package_id === serverPackageId)
  assert.equal(builds.length, 1, 'Require the actual bamboo-server build-script unit')
  const out = path.resolve(builds[0].out_dir)
  assert.ok(path.relative(targetDirectory, out).split(path.sep)[0] !== '..' && out !== path.resolve(targetDirectory),
    'Embedded frontend output escaped this build target')
  const embed = fs.readFileSync(path.join(out, 'frontend_package_embedded.rs'), 'utf8')
  for (const name of ['DUPLICATE_FRONTEND_PACKAGE_ZIP', 'DUPLICATE_FRONTEND_PACKAGE_MANIFEST']) {
    assert.match(embed, new RegExp(`${name}: Option<&\\[u8\\]> = Some\\(include_bytes!`), 'The executable must contain both frontend assets')
  }
  assert.equal(fileIdentity(path.join(out, 'frontend_package.zip')).sha256, frontendBytes.archiveSha256)
  assert.equal(fileIdentity(path.join(out, 'frontend_package_manifest.json')).sha256, frontendBytes.manifestSha256)
  const artifacts = messages.filter((item) => item.reason === 'compiler-artifact' &&
    item.target?.name === 'bamboo' && item.target.kind?.includes('bin') && item.executable)
  assert.equal(artifacts.length, 1, 'Require exactly the advertised Cargo executable')
  assert.equal(path.resolve(artifacts[0].executable), path.resolve(binary))
  assert.equal(messages.at(-1)?.reason, 'build-finished')
  assert.equal(messages.at(-1)?.success, true)
}

function sourceState(allowed) {
  const changes = git(['diff', '--name-only']).split('\n').filter(Boolean)
  for (const name of changes) assert.ok(allowed.has(name), `Unexpected source modification: ${name}`)
  assert.equal(git(['ls-files', '--others', '--exclude-standard']), '', 'Unexpected untracked release source')
  assert.equal(git(['diff', '--cached', '--name-only']), '', 'Release stamping must not modify the index')
  return { paths: changes, diffSha256: sha256(execute('git', ['diff', '--binary'], { encoding: null })) }
}

function verifyStampedVersions(metadata, version, { root = process.cwd(), manifestPaths }) {
  root = fs.realpathSync(root)
  assert.ok(Array.isArray(manifestPaths) && manifestPaths.length, 'Require the shared stamper manifest set')
  const expected = new Set()
  for (const manifest of manifestPaths) {
    assert.equal(typeof manifest, 'string')
    assert.ok(!path.isAbsolute(manifest), 'Stamped manifests must belong to this source root')
    const absolute = path.resolve(root, manifest)
    assert.ok(path.relative(root, absolute).split(path.sep)[0] !== '..', 'Stamped manifest escaped this source root')
    assert.ok(!expected.has(absolute), 'Duplicate stamped manifest')
    expected.add(absolute)
  }
  const managed = new Map()
  const members = new Set(metadata.workspace_members)
  for (const member of metadata.packages) {
    const manifest = fs.realpathSync(member.manifest_path)
    if (!expected.has(manifest)) continue
    assert.ok(!managed.has(manifest), 'Duplicate managed package in Cargo metadata')
    assert.ok(members.has(member.id), 'Managed package is not a workspace member')
    assert.equal(member.version, version, `Managed package ${member.name} has the wrong release version`)
    managed.set(manifest, member)
  }
  assert.equal(managed.size, expected.size, 'Cargo metadata omitted a stamped package')
  const agent = managed.get(path.resolve(root, 'Cargo.toml'))
  const server = managed.get(path.resolve(root, 'crates/app/bamboo-server/Cargo.toml'))
  assert.equal(agent?.name, 'bamboo-agent', 'Require the stamped Bamboo executable package')
  assert.equal(server?.name, 'bamboo-server', 'Require the stamped Bamboo server package')
  return server
}

function nativeCachePolicy({ root, temporary, contractVersion, target, sourceParents, sourceMessage, cutRevision }) {
  assert.ok(contractForVersion(contractVersion).targets.includes(target))
  const frozen = cutRevision === 'da348a63f1d019a22df547c78575c500b6b1294e' ||
    sourceParents.split(' ')[0] === 'dbafb74522794012fc8f0a61e761817b3913989a' ||
    sourceMessage.toLowerCase().includes('release/da348a63f1d019a22df547c78575c500b6b1294e')
  const targetDirectory = path.join(path.resolve(temporary), 'bamboo-executable-target')
  const compatibility = ['Cargo.toml', 'Cargo.lock', 'scripts/sidecar-build.cjs', '.cargo/config.toml']
    .filter((name) => fs.existsSync(path.join(root, name)))
    .map((name) => `${name}:${fileIdentity(path.join(root, name), 8 * 1024 * 1024).sha256}`).join('\n')
  // rust-cache adds compiler, environment, Cargo.lock and workspace manifests.
  // This namespace also isolates the producer ABI, target and source build profile.
  return { workspace: `${path.resolve(root)} -> ${path.relative(path.resolve(root), targetDirectory)}`,
    key: `bamboo-executable-deps-v${contractVersion}-${target}-${sha256(compatibility)}`,
    save: contractVersion === 2 && !frozen }
}

function evictWorkspaceArtifacts(metadata, targetDirectory, target) {
  // Keep third-party dependencies, but never let a restored workspace binary,
  // fingerprint or generated embed output stand in for this run's actual build.
  const members = new Set(metadata.workspace_members)
  const names = new Set(metadata.packages.filter((item) => members.has(item.id))
    .flatMap((item) => [item.name, ...item.targets.map((entry) => entry.name)])
    .flatMap((name) => [name, name.replaceAll('-', '_')]))
  for (const name of names) assert.match(name, /^[a-zA-Z0-9_-]+$/)
  const regularDirectory = (directory) => {
    let info
    try { info = fs.lstatSync(directory) } catch (error) { if (error.code === 'ENOENT') return false; throw error }
    assert.ok(info.isDirectory() && !info.isSymbolicLink(), 'Cached build directory must not be a symlink')
    return true
  }
  if (!regularDirectory(targetDirectory)) return []
  const removed = []
  const matches = (entry) => [...names].some((name) =>
    [name, `lib${name}`].some((prefix) => entry === prefix || entry.startsWith(`${prefix}-`) || entry.startsWith(`${prefix}.`)))
  const targetRoot = path.join(targetDirectory, target)
  regularDirectory(targetRoot)
  for (const profile of [path.join(targetDirectory, 'release'), path.join(targetRoot, 'release')]) {
    if (!regularDirectory(profile)) continue
    for (const relative of ['', 'deps', 'build', '.fingerprint']) {
      const directory = path.join(profile, relative)
      if (!regularDirectory(directory)) continue
      for (const name of fs.readdirSync(directory)) {
        if (!matches(name)) continue
        const file = path.join(directory, name)
        fs.rmSync(file, { recursive: true, force: true })
        removed.push(path.relative(targetDirectory, file))
      }
    }
  }
  return removed
}

async function originalSelection(identity, target, selecting, env = process.env) {
  const preparation = env.PREPARED_RELEASE ? JSON.parse(env.PREPARED_RELEASE) : undefined
  if (!preparation?.receipt?.nativeOriginals) {
    assert.ok(!env.NATIVE_ORIGINAL_PLAN, 'Historical preparation cannot acquire an original-output plan')
    return null
  }
  assert.equal(env.GITHUB_SHA, identity.workflowRevision, 'Actual controller SHA must not be relabeled as product source')
  const originals = require('./native-originals.cjs')
  const value = env.NATIVE_ORIGINAL_PLAN ? JSON.parse(env.NATIVE_ORIGINAL_PLAN) : undefined
  const selected = await originals.workerSelection(identity, preparation, value, target, selecting)
  if (!selecting) assert.equal(selected.action, 'build', 'Retained native outputs cannot start another build or cache producer')
  return selected
}

async function recordOriginalTransport(env = process.env) {
  const originals = require('./native-originals.cjs')
  const preparation = JSON.parse(env.PREPARED_RELEASE)
  const identity = originals.preparedIdentity(preparation, env), target = env.BAMBOO_RELEASE_TARGET
  assert.equal(nativeTarget(), target)
  await originalSelection(identity, target, false, env)
  const receiptFile = path.join(env.RUNNER_TEMP, 'bamboo-executable-output', 'build-receipt.json')
  fileIdentity(receiptFile, 1024 * 1024)
  const bytes = fs.readFileSync(receiptFile), build = JSON.parse(bytes)
  const bound = JSON.parse(env.EXPECTED_NATIVE_BUILD)
  assert.deepEqual(bound, { sha256: build.sha256, sizeBytes: build.sizeBytes,
    receiptSha256: sha256(bytes), runAttempt: identity.runAttempt }, 'Receipt differs from the independent build-step output')
  const allowed = new Set(git(['ls-files', '*Cargo.toml']).split('\n'))
  for (const name of ['Cargo.lock', 'crates/app/bamboo-server/frontend_package/frontend-manifest.json',
    'crates/app/bamboo-server/frontend_package/lotus-frontend.zip']) allowed.add(name)
  assert.equal(sourceState(allowed).diffSha256, build.stampedDiffSha256, 'Source changed after native build')
  const record = originals.transportRecord(identity, target, bytes,
    { id: Number(env.ORIGINAL_ARTIFACT_ID), sha256: env.ORIGINAL_ARTIFACT_SHA256 })
  originals.emit(originals.TRANSPORT_PREFIX, record)
}

async function main() {
  if (process.argv[2] === 'record-transport') return recordOriginalTransport()
  const identity = workflowIdentity()
  const contractVersion = boundSourceContract()
  const target = process.env.BAMBOO_RELEASE_TARGET
  assert.ok(contractForVersion(contractVersion).targets.includes(target))
  assert.equal(nativeTarget(), target, 'Build and runtime verification require a native target')
  const selection = await originalSelection(identity, target, process.argv[2] === 'select')
  if (process.argv[2] === 'select') {
    assert.ok(selection, 'Only the explicit new native lifecycle has a selector')
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `action=${selection.action}\n`)
    require('./native-originals.cjs').emit(require('./native-originals.cjs').SELECTION_PREFIX,
      { runId: identity.runId, runAttempt: identity.runAttempt, ...selection })
    return
  }
  if (process.argv[2] === 'cache-policy') {
    const policy = nativeCachePolicy({ root: process.cwd(), temporary: process.env.RUNNER_TEMP, contractVersion, target,
      cutRevision: identity.validation?.cutRevision,
      sourceParents: git(['show', '-s', '--format=%P', 'HEAD']), sourceMessage: git(['show', '-s', '--format=%B', 'HEAD']) })
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `workspace=${policy.workspace}\nkey=${policy.key}\nsave=${policy.save}\n`)
    return
  }
  assert.equal(process.argv[2], undefined)
  const temporary = path.resolve(process.env.RUNNER_TEMP)
  const { receipt, files } = preparedFrontend(identity, {
    preparation: JSON.parse(process.env.PREPARED_RELEASE), contractVersion,
    inputDirectory: path.join(temporary, 'bamboo-prepared-frontend') })
  const output = path.join(temporary, 'bamboo-executable-output')
  const targetDirectory = path.join(temporary, 'bamboo-executable-target')
  fs.mkdirSync(output)
  const buildEnv = buildEnvironment(process.env, targetDirectory)
  const frontendRoot = 'crates/app/bamboo-server/frontend_package'
  for (const { name, bytes } of files) {
    fs.writeFileSync(path.join(frontendRoot, name), bytes)
  }
  const allowed = new Set(git(['ls-files', '*Cargo.toml']).split('\n'))
  allowed.add('Cargo.lock')
  allowed.add(`${frontendRoot}/frontend-manifest.json`)
  allowed.add(`${frontendRoot}/lotus-frontend.zip`)
  const python = process.platform === 'win32' ? 'python' : 'python3'
  const manifestPaths = JSON.parse(execute(python, ['scripts/crate-release-lock.py', 'manifests'], { env: buildEnv }))
  execute(python, ['scripts/crate-release-lock.py', 'stamp', identity.version], { env: buildEnv })
  // Resolve only local stamped versions, then reject *any* external lock drift.
  execute('cargo', ['metadata', '--format-version', '1'], { env: buildEnv, maxBuffer: 64 * 1024 * 1024 })
  execute(python, ['scripts/crate-release-lock.py'], { env: buildEnv })
  const metadata = JSON.parse(execute('cargo', ['metadata', '--format-version', '1', '--no-deps', '--locked'], { env: buildEnv }))
  // Independent non-published examples keep their own version. Require every
  // package actually managed by the existing stamper, including CLI and server.
  const server = verifyStampedVersions(metadata, identity.version, { manifestPaths })
  const stamped = sourceState(allowed)
  const removed = evictWorkspaceArtifacts(metadata, targetDirectory, target)
  console.log(`Evicted ${removed.length} cached workspace artifacts; dependency cache remains available`)
  const args = ['build', '--locked', '--release', '--verbose', '--bin', 'bamboo', '--target', target,
    '--message-format=json-render-diagnostics']
  const log = path.join(temporary, 'bamboo-executable-cargo.jsonl')
  const fd = fs.openSync(log, 'wx')
  let result
  try { result = spawnSync('cargo', args, { env: buildEnv, stdio: ['ignore', fd, 'inherit'] }) }
  finally { fs.closeSync(fd) }
  assert.ok(!result.error && result.status === 0, 'Standard executable Cargo build failed')
  assert.ok(fs.statSync(log).size <= 64 * 1024 * 1024, 'Cargo evidence exceeded its bound')
  const messages = fs.readFileSync(log, 'utf8').trim().split('\n').map((line) => JSON.parse(line))
  const binary = path.join(targetDirectory, target, 'release', process.platform === 'win32' ? 'bamboo.exe' : 'bamboo')
  verifyBuildMessages(messages, { binary, targetDirectory, serverPackageId: server.id, frontendBytes: receipt.frontendBytes })
  binaryArchitecture(binary, target)
  const version = execute(binary, ['--version'], { env: buildEnv, timeout: 15000 }).trim()
  assert.equal(version, `bamboo ${identity.version}`, 'The executable did not receive the formal release version')
  assert.deepEqual(sourceState(allowed), stamped, 'Build changed release source after controlled stamping')
  assert.equal(git(['rev-parse', 'HEAD']), identity.sourceRevision)
  const destination = path.join(output, artifactName(target))
  fs.copyFileSync(binary, destination, fs.constants.COPYFILE_EXCL)
  const provenance = { schemaVersion: 1, ...identity, target, buildMode: 'embedded', embeddedFrontend: true,
    format: 'raw', assetName: artifactName(target), ...fileIdentity(destination, undefined, 65536),
    command: ['cargo', ...args], rustc: execute('rustc', ['-Vv'], { env: buildEnv }).trim(),
    cargo: execute('cargo', ['-V'], { env: buildEnv }).trim(), runtime: { status: 'passed', version },
    frontendBytes: receipt.frontendBytes, cargoMessagesSha256: fileIdentity(log).sha256,
    stampedDiffSha256: stamped.diffSha256, stampedPaths: stamped.paths }
  const receiptBytes = Buffer.from(`${JSON.stringify(provenance, null, 2)}\n`)
  fs.writeFileSync(path.join(output, 'build-receipt.json'), receiptBytes, { flag: 'wx' })
  // The runner's job-output channel is independent of the transport archive.
  // A substituted binary plus a forged receipt cannot change this binding.
  assert.ok(process.env.GITHUB_OUTPUT)
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `${targetOutputKey(target)}=${JSON.stringify({
    sha256: provenance.sha256, sizeBytes: provenance.sizeBytes, receiptSha256: sha256(receiptBytes),
    runAttempt: identity.runAttempt })}\n`)
  console.log(JSON.stringify({ status: 'standard-executable-built-and-version-verified', target,
    version: identity.version, sourceRevision: identity.sourceRevision, sha256: provenance.sha256 }))
}

module.exports = { nativeTarget, buildEnvironment, verifyBuildMessages, sourceState, verifyStampedVersions,
  nativeCachePolicy, evictWorkspaceArtifacts, originalSelection, recordOriginalTransport }
if (require.main === module) main().catch((error) => { console.error(error.message); process.exitCode = 1 })
