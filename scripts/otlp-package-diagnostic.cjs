#!/usr/bin/env node
// A manual, non-publishing feature diagnostic. This is not the release
// producer and its 0.0.0 executable cannot serve as a formal asset receipt.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { nativeTarget, verifyBuildMessages } = require('./sidecar-build.cjs')
const { binaryArchitecture, fileIdentity } = require('./sidecar-release.cjs')

const SAFE_ENVIRONMENT = new Set([
  'PATH', 'HOME', 'USERPROFILE', 'RUSTUP_HOME', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP',
  'LOCALAPPDATA', 'APPDATA', 'PROGRAMFILES', 'PROGRAMFILES(X86)', 'PROGRAMW6432',
  'COMSPEC', 'PATHEXT', 'OS', 'MSYSTEM', 'NUMBER_OF_PROCESSORS',
])

function isolatedEnvironment(environment, temporary) {
  const result = Object.fromEntries(Object.entries(environment).filter(([name]) =>
    SAFE_ENVIRONMENT.has(name.toUpperCase())))
  return { ...result, CARGO_HOME: path.join(temporary, 'otlp-diagnostic-cargo-home'),
    CARGO_TARGET_DIR: path.join(temporary, 'otlp-diagnostic-target'),
    CARGO_INCREMENTAL: '0', CARGO_TERM_COLOR: 'never', RUSTUP_TOOLCHAIN: 'stable',
    BAMBOO_FRONTEND_BUILD_MODE: 'embedded' }
}

function verifyFeatureMessages(messages, { rootPackageId, infrastructurePackageId }) {
  const artifacts = messages.filter(item => item.reason === 'compiler-artifact')
  const root = artifacts.filter(item => item.package_id === rootPackageId &&
    item.target?.name === 'bamboo' && item.target.kind?.includes('bin') && item.executable)
  assert.equal(root.length, 1, 'Require one root binary compiler unit')
  assert.ok(root[0].features?.includes('default') && root[0].features.includes('otel-otlp'),
    'The root binary did not compile the default OTLP feature')
  const infra = artifacts.filter(item => item.package_id === infrastructurePackageId &&
    item.target?.name === 'bamboo_infrastructure' && item.target.kind?.includes('lib'))
  assert.equal(infra.length, 1, 'Require one infrastructure library compiler unit')
  assert.ok(infra[0].features?.includes('otel-otlp'), 'Infrastructure OTLP feature is missing')
  const unit = item => ({ packageId: item.package_id, target: item.target.name,
    features: item.features, profile: item.profile, fresh: item.fresh })
  return { root: unit(root[0]), infrastructure: unit(infra[0]) }
}

function execute(command, args, env, timeout = 120_000) {
  const result = spawnSync(command, args, { env, timeout, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 })
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${command} failed: ${result.stderr}`)
  return result.stdout.trim()
}

function main() {
  const { EXPECTED_SOURCE_SHA: source, EXPECTED_TREE_SHA: tree, DISPATCH_SOURCE_SHA: dispatch,
    DIAGNOSTIC_TARGET: target, RUNNER_TEMP: runnerTemp } = process.env
  assert.match(source || '', /^[0-9a-f]{40}$/)
  assert.match(tree || '', /^[0-9a-f]{40}$/)
  assert.equal(source, dispatch, 'Dispatch and selected source differ')
  assert.equal(target, nativeTarget(), 'The diagnostic must execute on its native platform')
  assert.ok(runnerTemp && path.isAbsolute(runnerTemp), 'Require a private runner temporary directory')
  const temp = path.resolve(runnerTemp)
  const env = isolatedEnvironment(process.env, temp)
  const git = args => execute('git', args, env)
  const assertSource = () => {
    assert.equal(git(['rev-parse', 'HEAD']), source)
    assert.equal(git(['rev-parse', 'HEAD^{tree}']), tree)
    assert.equal(git(['status', '--porcelain', '--untracked-files=all']), '', 'Source must stay clean')
  }
  assertSource()
  const metadata = JSON.parse(execute('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'], env))
  const root = metadata.packages.find(item => item.name === 'bamboo-agent')
  const server = metadata.packages.find(item => item.name === 'bamboo-server')
  const infra = metadata.packages.find(item => item.name === 'bamboo-infrastructure')
  assert.ok(root && server && infra && root.version === '0.0.0', 'Require the unstamped source packages')
  console.log(execute(process.execPath, ['scripts/frontend-package.cjs', 'verify'], env))
  const frontend = path.join(process.cwd(), 'crates/app/bamboo-server/frontend_package')
  const frontendBytes = { archiveSha256: fileIdentity(path.join(frontend, 'lotus-frontend.zip')).sha256,
    manifestSha256: fileIdentity(path.join(frontend, 'frontend-manifest.json')).sha256,
    lockSha256: fileIdentity('scripts/frontend-package-lock.json').sha256 }
  const args = ['build', '--locked', '--release', '--verbose', '--bin', 'bamboo', '--target', target,
    '--message-format=json-render-diagnostics']
  const log = path.join(temp, 'otlp-diagnostic-cargo.jsonl')
  const fd = fs.openSync(log, 'wx')
  let result
  try { result = spawnSync('cargo', args, { env, stdio: ['ignore', fd, 'inherit'] }) }
  finally { fs.closeSync(fd) }
  if (result.error) throw result.error
  assert.equal(result.status, 0, 'Native executable compilation failed')
  assert.ok(fs.statSync(log).size <= 64 * 1024 * 1024, 'Cargo evidence exceeded its bound')
  const messages = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line))
  const binary = path.join(env.CARGO_TARGET_DIR, target, 'release', process.platform === 'win32' ? 'bamboo.exe' : 'bamboo')
  verifyBuildMessages(messages, { binary, targetDirectory: env.CARGO_TARGET_DIR, serverPackageId: server.id, frontendBytes })
  const featureUnits = verifyFeatureMessages(messages, { rootPackageId: root.id, infrastructurePackageId: infra.id })
  binaryArchitecture(binary, target)
  const version = execute(binary, ['--version'], env, 15_000)
  assert.equal(version, 'bamboo 0.0.0', 'Diagnostic must retain the unstamped version')
  assertSource()
  console.log(JSON.stringify({ kind: 'otlp-native-diagnostic/v1', source, tree, target, version,
    binary: fileIdentity(binary), cargoMessages: fileIdentity(log), featureUnits, frontendBytes,
    cargoLock: fileIdentity('Cargo.lock'), rustc: execute('rustc', ['-Vv'], env),
    cargo: execute('cargo', ['-V'], env), command: ['cargo', ...args], published: false }))
}

module.exports = { isolatedEnvironment, verifyFeatureMessages }
if (require.main === module) {
  try { main() } catch (error) { console.error(error.message); process.exitCode = 1 }
}
