#!/usr/bin/env node
// #3078 only. The bamboo-sdk upload returned HTTP 500 after its archive was retained.
// The signed reservation and terminal job log bind the four unstarted crates.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({
  repository: 'bigduu/Bamboo-agent',
  version: '2026.11.47',
  source: 'f8f9b0bc98025ae3acd0c771dc98b96e33c1bfba',
  tree: '04a078ae3026ec6ae65d9f892e245856c34fd91f',
  runId: 45610087197,
  workflowId: 242748806,
  suiteId: 123407501740,
  releaseId: 458057047,
  featurePr: 3076,
  featureRunId: 45604375435,
  jobId: 139191861769,
  logSha256: '7b30c0a07983de8e0b4f6c916c27dc5c2a8c581687c5ba5cd2c74229ba812d81',
  logBytes: 209988,
  baselineSha256: '74d208b4b7350f570a17ced03bfdd31f1a9673c0546a428151b98e3e92b732db',
  checksumsSha256: '0186108857155684b441190990fb5cbbfe56cc2a1be469c38e3402ba610dcb6a',
})
const TAIL = Object.freeze(['bamboo-server', 'bamboo-client-core', 'bamboo-tui', 'bamboo-agent'])
const JOBS = Object.freeze([
  [139191045556, 'automatic', 'success'],
  [139191861769, 'Publish and verify the crate closure', 'failure'],
  [139191861799, 'Complete executable (aarch64-pc-windows-msvc)', 'success'],
  [139191861874, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [139191861878, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [139191861916, 'Complete executable (aarch64-unknown-linux-gnu)', 'success'],
  [139191861960, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [139210831799, 'Publish and verify complete executable assets', 'skipped'],
])
const ATTEMPTS = Object.freeze([
  { jobs: JOBS, log: { jobId: TARGET.jobId, size: TARGET.logBytes, sha256: TARGET.logSha256 } },
])
const ARTIFACTS = Object.freeze([
  [15019148960, 'bamboo-prepared-frontend-attempt-1', 4344167, '2db14dfaccb04ec9fa81915e90487c9aa944fb0196cd2c74201512cdd03f6c14'],
  [15021549232, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 93411128, 'd2e0cf79ce2e9a962ebcc9abbdf8670213c0a8171f5b1bc762330a86f8015561'],
  [15021902050, 'bamboo-executable-aarch64-pc-windows-msvc-attempt-1', 85204552, 'e6c0ff7f881a7e18ee5f3734178bace4499259da19e3ac4870634229c22a0988'],
  [15020217772, 'bamboo-executable-aarch64-unknown-linux-gnu-attempt-1', 102794379, 'ce7f7deb1acd11684f5adb49d7378ac61e86fc8f9ed9df8ec7be863a9a0c57da'],
  [15022445355, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 91208562, 'a7cd9852ac4eadb8184fef368f9c09a6d639cfda8df385a80b6af1b443859836'],
  [15021321670, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 100110091, '962e50a83189c60ffa271962d04497b1910807fa2f69c82953bf1a5f7384a03e'],
])
const WORKFLOW = 'recover-crate-2026-11-47.yml'
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, attempts: ATTEMPTS, artifacts: ARTIFACTS,
  stateKey: 'recovery20261147', workflow: WORKFLOW, publishedPrefix: 26,
  retryRegistry: true, verifyCargoDownload: true,
})
module.exports = { TARGET, TAIL, ATTEMPTS, ARTIFACTS, WORKFLOW, ...controller }
if (require.main === module) runRecoveryCli(controller)
