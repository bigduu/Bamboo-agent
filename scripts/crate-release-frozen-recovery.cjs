#!/usr/bin/env node
// #2371 only: the fixed v2026.10.19 recovery under its original receipt schema.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({ repository: 'bigduu/Bamboo-agent', version: '2026.10.19',
  source: '4786b7db862cabbebce49e288da62b284ccf5b72', tree: 'b9735f6b88a49ee90cd2442180fd22cfcec7264b',
  runId: 40613036533, workflowId: 242748806, suiteId: 110081656847, jobId: 122569588592,
  releaseId: 426156779, featureRunId: 40588035791, featurePr: 2369,
  logSha256: '42225eab1e3c99da236485e956e5cc1492b8f061fa5b37d9371eed85c9d05104', logBytes: 189857,
  baselineSha256: '9227ba4c429a503bb996a8eb925923ec71f10a4ab1055741c0e9789558128979',
  checksumsSha256: 'a7e03affa9b33ec1e9c09010d0a2fa2917660274083bd6541bd4f51b6bbede81' })
const TAIL = Object.freeze(['bamboo-client-core', 'bamboo-tui', 'bamboo-agent'])
const JOBS = Object.freeze([
  [122568930492, 'automatic', 'success'],
  [122569588526, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [122569588556, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [TARGET.jobId, 'Publish and verify the crate closure', 'failure'],
  [122569588610, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [122582873055, 'Publish and verify complete executable assets', 'skipped'],
])
const ARTIFACTS = Object.freeze([
  [12784515163, 'bamboo-prepared-frontend-attempt-1', 4266920, '13bc2e940c27a73a1b50e77e0e1ebf58184116d3c8f3f62ba977f85ef4f378cc'],
  [12785005824, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 80961535, 'cef4b4d5505b050ff01a0d24a6ab88cd3990ea59c05d415c56532861290532c1'],
  [12784129333, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 74697517, '32fb940079fca53fea0a02a871a3a9b2271da2dbb27e9c53a52261169c6e3a79'],
  [12783854647, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 73526710, '459def894362836109e219f48d9871d6e70a9900bf05eed32fd37f5285509fda'],
])
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, artifacts: ARTIFACTS,
  attempts: [{ jobs: JOBS, log: { jobId: TARGET.jobId, size: TARGET.logBytes, sha256: TARGET.logSha256 } }],
  stateKey: 'recovery2371', workflow: 'recover-frozen-crate-release.yml', publishedPrefix: 26,
})
module.exports = { TARGET, TAIL, JOBS, ARTIFACTS, ...controller }
if (require.main === module) runRecoveryCli(controller)
