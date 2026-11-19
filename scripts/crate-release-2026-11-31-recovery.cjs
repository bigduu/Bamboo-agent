#!/usr/bin/env node
// #2840 only. The original failure was an incomplete registry response body.
// The published prefix and the pinned failure log bind the four unstarted crates.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({
  repository: 'bigduu/Bamboo-agent',
  version: '2026.11.31',
  source: '1387680f8ce96d808ba48b7b570fba5dded4e021',
  tree: '9d85d9c34d776fd42f8c6eff1996c0349a44ac12',
  runId: 44437436231,
  workflowId: 242748806,
  suiteId: 120290916871,
  releaseId: 450766353,
  featurePr: 2838,
  featureRunId: 44434309578,
  jobId: 135270367921,
  logSha256: '7017d5cea018526129707a9159ed84a33f71ff775250da75e5e6c3f6c8a94112',
  logBytes: 201692,
  baselineSha256: '0c8b4d63f589b3d7667131d481d043417cc78cea23995688c999f13783f17cd9',
  checksumsSha256: '1f4f395b897b6c24168c15aea9b821b18a7fc3478059bf614e35e4a6aada3da7',
})
const TAIL = Object.freeze(['bamboo-server', 'bamboo-client-core', 'bamboo-tui', 'bamboo-agent'])
const JOBS = Object.freeze([
  [135269724363, 'automatic', 'success'],
  [135270367921, 'Publish and verify the crate closure', 'failure'],
  [135270367992, 'Complete executable (aarch64-pc-windows-msvc)', 'success'],
  [135270368014, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [135270368035, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [135270368044, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [135270368065, 'Complete executable (aarch64-unknown-linux-gnu)', 'success'],
  [135282467362, 'Publish and verify complete executable assets', 'skipped'],
])
const ATTEMPTS = Object.freeze([
  { jobs: JOBS, log: { jobId: TARGET.jobId, size: TARGET.logBytes, sha256: TARGET.logSha256 } },
])
const ARTIFACTS = Object.freeze([
  [14488619297, 'bamboo-prepared-frontend-attempt-1', 4299467, '5a2038a54fd3a17f2192c9c4601c5d5d526c9db92b1facb442dc9d349805541e'],
  [14491321092, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 78722520, 'a026bbed2a19b2316a6609ae56fadbe0caa90696b4be3fc05edcbe5094d4a2b3'],
  [14490911593, 'bamboo-executable-aarch64-pc-windows-msvc-attempt-1', 72006821, 'b89abb222d914395546c84017230309be1a142b40b9d46c909317e69c4aee3f3'],
  [14489836662, 'bamboo-executable-aarch64-unknown-linux-gnu-attempt-1', 86205417, '4b7c534bc3c8eab82aea4778690ccedb52de01b423b2d3ea3ae56417f506bde7'],
  [14489613236, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 77251549, '091f2d24eff6c93c05a1f99307b4d3761ff42dd8c35ceb1b47b5ddbea08af6d3'],
  [14490001566, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 84805022, '793abeee9df7657a4f1b5917cfa5c3229a804e329106ba5a6f187d74f9a37f06'],
])
const WORKFLOW = 'recover-crate-2026-11-31.yml'
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, attempts: ATTEMPTS, artifacts: ARTIFACTS,
  stateKey: 'recovery20261131', workflow: WORKFLOW, publishedPrefix: 26,
  retryRegistry: true, verifyCargoDownload: true,
})
module.exports = { TARGET, TAIL, ATTEMPTS, ARTIFACTS, WORKFLOW, ...controller }
if (require.main === module) runRecoveryCli(controller)
