#!/usr/bin/env node
// #2762 only. Attempt 1 stopped in the registry readback after bamboo-infrastructure.
// The signed original prefix and the failed readback bind the unstarted tail.
const { createFrozenRecovery, runRecoveryCli } = require('./frozen-crate-recovery-controller.cjs')

const TARGET = Object.freeze({
  repository: 'bigduu/Bamboo-agent', version: '2026.11.22',
  source: '13336567999504a8c7deb89fba94f7907b04c3ee', tree: 'be8941ef66caac847aef43b2a2b2dca3f1306233',
  runId: 43712198357, workflowId: 242748806, suiteId: 118346762013, releaseId: 446034086,
  featurePr: 2760, featureRunId: 43709789184,
  jobId: 132806472481,
  logSha256: '276e52b5511f2365c70130bb7cd948b612e817bb1c215c466540cb650fdfcf6d',
  baselineSha256: 'bc66b3667997ca7645a6c24556e6a8f1f50796486a23e6ed6ebef29080f6e072',
  checksumsSha256: '015750607c93d350f46bba756b34ef14800f7d8fa11bdc9871ce44a7916d96eb',
})
const TAIL = Object.freeze([
  'bamboo-llm', 'bamboo-plugin-protocol', 'bamboo-plugin', 'bamboo-hooks',
  'bamboo-mcp', 'bamboo-memory', 'bamboo-metrics', 'bamboo-permission', 'bamboo-tickets',
  'bamboo-orchestration', 'bamboo-skills', 'bamboo-storage', 'bamboo-tools', 'bamboo-engine',
  'bamboo-notification', 'bamboo-projects', 'bamboo-server-tools', 'bamboo-sdk', 'bamboo-server',
  'bamboo-client-core', 'bamboo-tui', 'bamboo-agent',
])
const JOBS = Object.freeze([
  [132806009388, 'automatic', 'success'],
  [132806472481, 'Publish and verify the crate closure', 'failure'],
  [132806472528, 'Complete executable (aarch64-apple-darwin)', 'success'],
  [132806472597, 'Complete executable (aarch64-pc-windows-msvc)', 'success'],
  [132806472525, 'Complete executable (aarch64-unknown-linux-gnu)', 'success'],
  [132806472547, 'Complete executable (x86_64-pc-windows-msvc)', 'success'],
  [132806472524, 'Complete executable (x86_64-unknown-linux-gnu)', 'success'],
  [132812803333, 'Publish and verify complete executable assets', 'skipped'],
])
const ATTEMPTS = Object.freeze([
  { jobs: JOBS, log: { jobId: TARGET.jobId, size: 199769, sha256: TARGET.logSha256 } },
])
const ARTIFACTS = Object.freeze([
  [14160987115, 'bamboo-prepared-frontend-attempt-1', 4299471, '7e957827aaab9190b1c7e459a07715bbc8ef431ac7b7b3612654be9f48f0941f'],
  [14160974041, 'bamboo-executable-aarch64-apple-darwin-attempt-1', 78503193, '306eb02709a48502ae53831191d069e1cb12d9a2145a931e340617cd13742230'],
  [14161284515, 'bamboo-executable-aarch64-pc-windows-msvc-attempt-1', 71964453, '7fa3d435d1730e8499a1dfeff9a1b158031b31d5d0f24fa079f6a7be912f6d84'],
  [14161656996, 'bamboo-executable-aarch64-unknown-linux-gnu-attempt-1', 86014804, '26fdaa9dbf252fc0b219d0fa2632eb7e3bbd1eb7bd6720196e0493a350d536bb'],
  [14161218650, 'bamboo-executable-x86_64-pc-windows-msvc-attempt-1', 77155296, '538cc9e01dfcec704161a01f8111ae8a78f3709b19f22089d0cbc7059043fe85'],
  [14161672298, 'bamboo-executable-x86_64-unknown-linux-gnu-attempt-1', 84535899, '01c1ff7490c8e7863b96b1f883d22f904013f2b83a89af97e59dbed8d88e8160'],
])
const WORKFLOW = 'recover-crate-2026-11-22.yml'
const controller = createFrozenRecovery({
  target: TARGET, tail: TAIL, attempts: ATTEMPTS, artifacts: ARTIFACTS,
  stateKey: 'recovery20261122', workflow: WORKFLOW, publishedPrefix: 8, retryRegistry: true,
})
module.exports = { TARGET, TAIL, ATTEMPTS, ARTIFACTS, WORKFLOW, ...controller }
if (require.main === module) runRecoveryCli(controller)
