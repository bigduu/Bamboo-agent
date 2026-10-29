#!/usr/bin/env node
// One explicit Linux diagnostic, not a replacement for canonical feature CI.
// Every selected test uses synthetic values and local/loopback collectors.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { isolatedEnvironment } = require('./otlp-package-diagnostic.cjs')
const { fileIdentity } = require('./sidecar-release.cjs')

const CASES = [
  'telemetry::config::tests::exported_references_hide_untrusted_ids_and_preserve_local_correlation',
  'telemetry::config::tests::export_references_are_concurrent_and_domain_separated',
  'telemetry::config::tests::zero_export_sampling_keeps_local_spans_without_sending_them',
  'telemetry::config::tests::explicit_guard_shutdown_is_bounded_and_idempotent',
  'telemetry::tests::otlp::local_otlp_collector_receives_allowlisted_trace_bytes',
  'telemetry::tests::otlp::retryable_collector_status_is_sent_once',
  'telemetry::tests::otlp::ambient_otel_headers_and_resources_cannot_leak_credentials',
  'telemetry::tests::otlp::unavailable_collector_never_backpressures_spans_and_shutdown_is_bounded',
  'telemetry::tests::otlp::zero_sample_ratio_exports_no_roots',
]

function selectTestExecutable(messages, packageId, targetDirectory) {
  const artifacts = messages.filter(item => item.reason === 'compiler-artifact' && item.package_id === packageId &&
    item.target?.name === 'bamboo_infrastructure' && item.target.kind?.includes('lib') &&
    item.profile?.test === true && item.executable)
  assert.equal(artifacts.length, 1, 'Require exactly one infrastructure test executable')
  const artifact = artifacts[0]
  assert.ok(artifact.features?.includes('otel-otlp') && artifact.features.includes('test-utils'),
    'The test executable is missing its owned feature context')
  const relative = path.relative(targetDirectory, path.resolve(artifact.executable))
  assert.ok(relative && !relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative),
    'Test executable escaped its isolated target directory')
  return artifact
}

function requireOnePassed(result, name) {
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${name} failed:\n${result.stdout || ''}\n${result.stderr || ''}`)
  assert.match(result.stdout || '', /^test result: ok\. 1 passed; 0 failed; 0 ignored;/mu,
    `${name} did not produce exactly one passing result`)
}

function execute(command, args, env) {
  const result = spawnSync(command, args, { env, encoding: 'utf8', timeout: 120_000, maxBuffer: 32 * 1024 * 1024 })
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${command} failed: ${result.stderr}`)
  return result.stdout.trim()
}

function main() {
  const { EXPECTED_SOURCE_SHA: source, EXPECTED_TREE_SHA: tree, DISPATCH_SOURCE_SHA: dispatch,
    RUNNER_TEMP: runnerTemp } = process.env
  assert.match(source || '', /^[0-9a-f]{40}$/)
  assert.match(tree || '', /^[0-9a-f]{40}$/)
  assert.equal(source, dispatch, 'Dispatch and selected source differ')
  assert.equal(process.platform, 'linux', 'The collector diagnostic owns Linux only')
  assert.ok(runnerTemp && path.isAbsolute(runnerTemp))
  const temp = path.resolve(runnerTemp)
  const env = { ...isolatedEnvironment(process.env, temp), CARGO_PROFILE_TEST_DEBUG: '0' }
  const git = args => execute('git', args, env)
  const assertSource = () => {
    assert.equal(git(['rev-parse', 'HEAD']), source)
    assert.equal(git(['rev-parse', 'HEAD^{tree}']), tree)
    assert.equal(git(['status', '--porcelain', '--untracked-files=all']), '', 'Source must stay clean')
  }
  assertSource()
  const metadata = JSON.parse(execute('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'], env))
  const infra = metadata.packages.find(item => item.name === 'bamboo-infrastructure')
  assert.ok(infra, 'Require the infrastructure package')
  const args = ['test', '--locked', '-p', 'bamboo-infrastructure', '--features', 'otel-otlp,test-utils',
    '--lib', '--no-run', '--message-format=json-render-diagnostics']
  const log = path.join(temp, 'otlp-collector-cargo.jsonl')
  const fd = fs.openSync(log, 'wx')
  let compiled
  try { compiled = spawnSync('cargo', args, { env, stdio: ['ignore', fd, 'inherit'] }) }
  finally { fs.closeSync(fd) }
  if (compiled.error) throw compiled.error
  assert.equal(compiled.status, 0, 'Collector test compilation failed')
  assert.ok(fs.statSync(log).size <= 64 * 1024 * 1024, 'Cargo evidence exceeded its bound')
  const messages = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line))
  assert.equal(messages.at(-1)?.reason, 'build-finished')
  assert.equal(messages.at(-1)?.success, true)
  const artifact = selectTestExecutable(messages, infra.id, env.CARGO_TARGET_DIR)
  const binary = fileIdentity(artifact.executable)
  const results = []
  for (const name of CASES) {
    const result = spawnSync(artifact.executable, [name, '--exact', '--nocapture'],
      { env, encoding: 'utf8', timeout: 30_000, maxBuffer: 2 * 1024 * 1024 })
    requireOnePassed(result, name)
    results.push({ name, passed: 1 })
    console.log(`PASS ${name}`)
  }
  assertSource()
  assert.deepEqual(fileIdentity(artifact.executable), binary, 'The test executable changed while executing')
  console.log(JSON.stringify({ kind: 'otlp-synthetic-collector-diagnostic/v1', source, tree,
    testExecutable: binary, cargoMessages: fileIdentity(log), features: artifact.features,
    profile: artifact.profile, fresh: artifact.fresh, cases: results, command: ['cargo', ...args],
    rustc: execute('rustc', ['-Vv'], env), cargo: execute('cargo', ['-V'], env) }))
}

module.exports = { CASES, selectTestExecutable, requireOnePassed }
if (require.main === module) {
  try { main() } catch (error) { console.error(error.message); process.exitCode = 1 }
}
