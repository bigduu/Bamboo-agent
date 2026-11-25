#!/usr/bin/env node
// #3053 only. The original upload returned HTTP 500 after the archive was retained.
// The signed reservation and terminal job log bind the six unstarted crates.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({
  repository: 'bigduu/Bamboo-agent',
  version: '2026.11.43',
  source: 'd9a04783cc9eff82e406a78e291136537eff10c6',
  tree: '3394a25233e17f4e3e58ac57943410ce727c509d',
  runId: 45451853467,
  workflowId: 242748806,
  suiteId: 122987044173,
  releaseId: 457147104,
  featurePr: 3050,
  featureRunId: 45447773120,
  jobId: 138665459903,
  logSha256: '6790b622c2a8161bf3bd0743c0254d2784a77deac9d6c5ae336b877302b1b349',
  logBytes: 209609,
  baselineSha256: '72e4f8fc7cb91cb4865ab8a2a3a7555d22ab9aecec362022d1424c7e7c7afe48',
  checksumsSha256: '6712eb2099e485f8a574f21ad077a54be2c6a7a78f6e19f3074f4a13ded7ad8e',
})
const TAIL = Object.freeze([
  'bamboo-server-tools', 'bamboo-sdk', 'bamboo-server', 'bamboo-client-core', 'bamboo-tui', 'bamboo-agent',
])
const JOBS = Object.freeze([
  [138664034551, 'automatic', 'success'],
  [138665459903, 'Publish and verify the crate closure', 'failure'],
  [138665459959, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [138665460001, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [138665460055, 'Complete executable (aarch64-unknown-linux-gnu)', 'success'],
  [138665460062, 'Complete executable (aarch64-pc-windows-msvc)', 'success'],
  [138665460170, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [138687003322, 'Publish and verify complete executable assets', 'skipped'],
])
const ATTEMPTS = Object.freeze([
  { jobs: JOBS, log: { jobId: TARGET.jobId, size: TARGET.logBytes, sha256: TARGET.logSha256 } },
])
const ARTIFACTS = Object.freeze([
  [14950232018, 'bamboo-prepared-frontend-attempt-1', 4372450, '77d0b07f2b8ee96ecea002c2cb14ea27ea974e448a33213bf3ecd937bc837c0a'],
  [14953370641, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 93337894, 'f1ec5276f772a88d903d9180069310c43010e6f1ae05859b2642693a4a9596d4'],
  [14952048872, 'bamboo-executable-aarch64-pc-windows-msvc-attempt-1', 85189854, 'a6aae6af9e8e6e3bf3c6a9ebe1c2f10eb0afff3261071cbe4cd951b893731f9e'],
  [14952081695, 'bamboo-executable-aarch64-unknown-linux-gnu-attempt-1', 102785435, '95a7e794a31a361be94f0f6c346fdccbad7aff8d68a7f12a700eb6b52689cac8'],
  [14953085009, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 91119440, '7f26348ebaf746b1e5f9e7e5e1621c3e3b759a07a5f034c3e787f5a866b70253'],
  [14949924999, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 99997271, '16a42608e193f848eb2dfe062d156a7751292ca318f3f4c2369cb5ffa7d7d520'],
])
const WORKFLOW = 'recover-crate-2026-11-43.yml'
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, attempts: ATTEMPTS, artifacts: ARTIFACTS,
  stateKey: 'recovery20261143', workflow: WORKFLOW, publishedPrefix: 24,
  retryRegistry: true, verifyCargoDownload: true,
})
module.exports = { TARGET, TAIL, ATTEMPTS, ARTIFACTS, WORKFLOW, ...controller }
if (require.main === module) runRecoveryCli(controller)
