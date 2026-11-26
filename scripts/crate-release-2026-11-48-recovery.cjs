#!/usr/bin/env node
// #3115 only. The bamboo-config upload returned HTTP 500 after its archive was retained.
// The signed reservation and terminal job log bind the 25 unstarted tail crates.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({
  repository: 'bigduu/Bamboo-agent',
  version: '2026.11.48',
  source: 'e54b55afdc40fbd9231550852c3f6b64a5029351',
  tree: '26ae0f1467f001e77d416da1362819d5cabad06b',
  runId: 45729058776,
  workflowId: 242748806,
  suiteId: 123726841057,
  releaseId: 458800022,
  featurePr: 3113,
  featureRunId: 45726683468,
  jobId: 139606975375,
  logSha256: 'ed525bd4d86a3b81679ae53d2a9185ba7a1aea3de11f3be546e3c1be106063d4',
  logBytes: 210194,
  baselineSha256: 'be65d010c74e59f3acdb572002126e9e5014b93de1409e9d9809c99f0481f699',
  checksumsSha256: '9c463c8fa14778e083b4e2219da78bc419eccc6d5433b7f03e9478cfb0f63b15',
})
const TAIL = Object.freeze([
  'bamboo-compression',
  'bamboo-infrastructure',
  'bamboo-litellm-llms-types',
  'bamboo-llm',
  'bamboo-plugin-protocol',
  'bamboo-plugin',
  'bamboo-hooks',
  'bamboo-mcp',
  'bamboo-memory',
  'bamboo-metrics',
  'bamboo-permission',
  'bamboo-tickets',
  'bamboo-orchestration',
  'bamboo-skills',
  'bamboo-storage',
  'bamboo-tools',
  'bamboo-engine',
  'bamboo-notification',
  'bamboo-projects',
  'bamboo-server-tools',
  'bamboo-sdk',
  'bamboo-server',
  'bamboo-client-core',
  'bamboo-tui',
  'bamboo-agent',
])
const JOBS = Object.freeze([
  [139606221200, 'automatic', 'success'],
  [139606975326, 'Complete executable (aarch64-unknown-linux-gnu)', 'success'],
  [139606975355, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [139606975375, 'Publish and verify the crate closure', 'failure'],
  [139606975431, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [139606975437, 'Complete executable (aarch64-pc-windows-msvc)', 'success'],
  [139606975653, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [139617465842, 'Publish and verify complete executable assets', 'skipped'],
])
const ATTEMPTS = Object.freeze([
  { jobs: JOBS, log: { jobId: TARGET.jobId, size: TARGET.logBytes, sha256: TARGET.logSha256 } },
])
const ARTIFACTS = Object.freeze([
  [15076251311, 'bamboo-prepared-frontend-attempt-1', 4350098, '86fa5c7d5e60a5f9b479d2fd0e9c6f3d2aa17cc3e7a675df4e8175a87fffb7da'],
  [15076412821, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 93560467, 'a963dc631fc748005d6b0f5c22b1818a3f438efbb4f6907dab3232b694b5f609'],
  [15076937153, 'bamboo-executable-aarch64-pc-windows-msvc-attempt-1', 85403497, '6f408ddab4a19958a924f0582ea58434f89396ea907900d1e4128939068969d2'],
  [15076845942, 'bamboo-executable-aarch64-unknown-linux-gnu-attempt-1', 103148046, 'd11003c6db7626ce770b16a3255275827aa0a74940bba90b1b4476da0b00aea1'],
  [15076851992, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 91330695, 'ebb8f8efa485228160d3677ee9198ff7958ad9ef95b5866ca524b7d579affd91'],
  [15076971236, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 100579961, '96e4683bf4d507cbb68f124aae59cf74d0d49454c4b859a5386dd991f4f84c45'],
])
const WORKFLOW = 'recover-crate-2026-11-48.yml'
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, attempts: ATTEMPTS, artifacts: ARTIFACTS,
  stateKey: 'recovery20261148', workflow: WORKFLOW, publishedPrefix: 5,
  retryRegistry: true, verifyCargoDownload: true,
})
module.exports = { TARGET, TAIL, ATTEMPTS, ARTIFACTS, WORKFLOW, ...controller }
if (require.main === module) runRecoveryCli(controller)
