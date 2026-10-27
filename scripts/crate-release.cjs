#!/usr/bin/env node
// Publication policy and its small CLI share the same paths exercised by tests.
const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { isDeepStrictEqual } = require('node:util')

const RECEIPT_NAME = 'release-provenance.json'
const SHA = /^[0-9a-f]{40}$/
const DIGEST = /^[0-9a-f]{64}$/
const U64_MAX = 18446744073709551615n
const VERSION = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const AUTH_DOMAIN = 'bamboo-release-provenance/hmac-sha256/v1\0'
const FROZEN_DISPATCH = 'bamboo-frozen-source-dispatch/v1'
const MAIN_HISTORY_ANCHOR = 'd6325a732c7669f94962f604e4ce98966e374d73'

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

function assertSigningKey(key) {
  // Never put a secret in an assertion's actual/expected fields or logs.
  assert.ok(typeof key === 'string' && DIGEST.test(key), 'BAMBOO_RELEASE_SIGNING_KEY must be 32 bytes of lowercase hex')
}

function assertSigningConfiguration(key, expectedDigest) {
  assertSigningKey(key)
  assert.ok(typeof expectedDigest === 'string' && DIGEST.test(expectedDigest),
    'Missing or invalid Environment BAMBOO_RELEASE_SIGNING_KEY_SHA256')
  assert.ok(crypto.timingSafeEqual(Buffer.from(sha256(Buffer.from(key, 'hex')), 'hex'),
    Buffer.from(expectedDigest, 'hex')), 'Release signing key does not match the protected Environment configuration')
}

function receiptMac(receipt, key) {
  assertSigningKey(key)
  const { authentication, ...payload } = receipt
  return crypto.createHmac('sha256', Buffer.from(key, 'hex')).update(AUTH_DOMAIN).update(canonicalJson(payload)).digest()
}

function signReceipt(receipt, key) {
  receipt.authentication = { schemaVersion: 1, algorithm: 'hmac-sha256', mac: receiptMac(receipt, key).toString('hex') }
  return receipt
}

function authenticateReceipt(receipt, key) {
  const authentication = receipt?.authentication
  assert.ok(authentication?.schemaVersion === 1 && authentication.algorithm === 'hmac-sha256' &&
    typeof authentication.mac === 'string' && DIGEST.test(authentication.mac) &&
    Object.keys(authentication).sort().join(',') === 'algorithm,mac,schemaVersion', 'Missing or invalid release receipt authentication')
  assert.ok(crypto.timingSafeEqual(receiptMac(receipt, key), Buffer.from(authentication.mac, 'hex')),
    'Release receipt authentication failed')
  return receipt
}

function readAuthenticatedReceipt(release, key, { required = false } = {}) {
  let receipt
  try {
    receipt = readBodyReceipt(release)
  } catch (error) {
    if (required) throw error
    return null
  }
  // Required reservations never accept or re-sign unauthenticated bytes.
  if (receipt && Object.prototype.hasOwnProperty.call(receipt, 'authentication')) {
    return authenticateReceipt(receipt, key)
  }
  assert.ok(!required, 'Missing release receipt authentication')
  return null
}

function assertVersion(version) {
  const match = typeof version === 'string' && VERSION.exec(version)
  assert.ok(match && version !== '0.0.0' && match.slice(1, 4).every((part) =>
    part.length <= 20 && BigInt(part) <= U64_MAX) &&
    (!match[4] || match[4].split('.').every((part) => !/^\d+$/.test(part) || !/^0\d/.test(part))),
    'Pass a real, explicit publish version; source 0.0.0 is a placeholder')
  return version
}

function assertManualVersion(version, now = new Date()) {
  const stable = /^(\d+)\.(\d+)\.\d+$/.exec(assertVersion(version))
  if (stable) {
    const year = BigInt(now.getUTCFullYear())
    assert.ok(BigInt(stable[1]) < year || (BigInt(stable[1]) === year &&
      BigInt(stable[2]) <= BigInt(now.getUTCMonth() + 1)),
    'Manual stable version must not be later than the current UTC year/month')
  }
  return version
}

function validateSource({ eventName, event, repository, sourceRevision, sourceTree, validation }) {
  assert.match(sourceRevision, SHA)
  // A caller-supplied historical dispatch proof is not new publication authority.
  assert.equal(eventName, 'workflow_run', 'Unsupported release trigger')
  const run = event.workflow_run
  assert.equal(run?.name, 'CI')
  assert.equal(run?.event, 'push')
  assert.equal(run?.conclusion, 'success')
  assert.equal(run?.head_branch, 'main')
  assert.equal(run?.head_repository?.full_name, repository)
  assert.equal(run?.repository?.full_name, repository)
  assert.equal(sourceRevision, run?.head_sha, 'Release source must equal the successful main CI commit')
  if (validation !== undefined) {
    assert.equal(validation?.schemaVersion, 1)
    assert.equal(validation.kind, 'bamboo-main-release-admission/v1')
    assert.equal(validation.repository, repository)
    assert.equal(validation.sourceRevision, sourceRevision)
    assert.match(sourceTree, SHA)
    assert.equal(validation.sourceTree, sourceTree, 'Admission tree differs from publication checkout')
    for (const key of ['cutRevision', 'releaseHead', 'baseRevision']) assert.match(validation[key], SHA)
    assert.ok(Number.isSafeInteger(validation.releasePr) && validation.releasePr > 0)
    for (const key of ['id', 'run_attempt', 'name', 'event', 'head_sha', 'head_branch', 'conclusion']) {
      assert.deepEqual(validation.ciRun?.[key], run[key], 'Admission must retain the actual successful main CI identity')
    }
    assert.ok(Number.isSafeInteger(run.id) && run.id > 0 && Number.isSafeInteger(run.run_attempt) && run.run_attempt > 0)
    assert.equal(validation.ciRun.status, 'completed')
    assert.equal(validation.ciRun.repository?.full_name, repository)
    assert.equal(validation.ciRun.head_repository?.full_name, repository)
    assert.equal(validation.ciRun.html_url, `https://github.com/${repository}/actions/runs/${run.id}`)
    const pre = validation.premerge
    for (const key of ['runId', 'runAttempt', 'workflowId', 'checkSuiteId']) {
      assert.ok(Number.isSafeInteger(pre?.[key]) && pre[key] > 0, 'Missing full pre-merge CI identity')
    }
    assert.match(pre.snapshotRevision, SHA)
    assert.equal(pre.url, `https://github.com/${repository}/actions/runs/${pre.runId}`)
  }
  return true
}

function validateReceipt(receipt, identity, crates, version = receipt?.version) {
  assert.equal(receipt?.schemaVersion, 1, 'Missing or unsupported release provenance')
  assert.equal(receipt.version, assertVersion(version))
  assert.deepEqual(receipt.identity, identity, 'Release version belongs to a different source/frontend identity')
  assert.deepEqual(receipt.crates, crates, 'Release publish closure changed')
  assert.equal(typeof receipt.automatic, 'boolean')
  assert.equal(typeof receipt.completed, 'boolean')
  if (Object.hasOwn(receipt, 'sourceDispatch')) {
    const marker = receipt.sourceDispatch
    assert.equal(receipt.automatic, true, 'Source dispatch requires the canonical allocator')
    assert.deepEqual(marker, sourceDispatchContract(marker?.context, marker?.runId))
    assert.equal(marker.context.source.revision, identity.sourceRevision)
    assert.equal(identity.repository, 'bigduu/Bamboo-agent')
    assert.equal(receipt.ciRun, `https://github.com/bigduu/Bamboo-agent/actions/runs/${marker.context.feature.runId}`)
    assert.equal(receipt.validation, undefined, 'Historical and frozen-source admissions cannot be mixed')
    if (receipt.frontendOriginals) validateFrontendOriginals(receipt.frontendOriginals, receipt)
    for (const [name, state] of Object.entries(receipt.crateOriginals || {})) {
      assert.ok(crates.includes(name)); require('./release-crate-output.cjs').record(state,
        originalCrateInputs({ ...receipt, releaseId: state.inputs.releaseId }, name, state.inputs.pathInVcs))
      assert.equal(state.origin.controllerRevision, marker.context.controller.revision); assert.equal(state.origin.runId, Number(marker.runId))
      if (state.sealed) assert.equal(receipt.packageChecksums[name], state.sealed.proof.archive.sha256)
    }
  }
  if (Object.hasOwn(receipt, 'nativeOriginals')) {
    const marker = receipt.nativeOriginals
    assert.equal(receipt.automatic, true, 'Original native outputs require the canonical reservation')
    assert.deepEqual(marker, nativeOriginalContract({ workflowRevision: marker?.controllerRevision, runId: marker?.runId }))
  }
  assert.match(receipt.frontendBytes?.manifestSha256, DIGEST)
  assert.match(receipt.frontendBytes?.archiveSha256, DIGEST)
  assert.ok(receipt.packageChecksums && typeof receipt.packageChecksums === 'object')
  for (const [crate, checksum] of Object.entries(receipt.packageChecksums)) {
    assert.ok(crates.includes(crate), 'Unexpected crate in release provenance')
    assert.match(checksum, DIGEST)
  }
  if (receipt.completed) {
    assert.deepEqual(Object.keys(receipt.packageChecksums).sort(), [...crates].sort(),
      'Completed release must record every package checksum')
  }
  return receipt
}

// This discriminator is signed by the existing writer. Its context remains
// read-only data; constructing it never grants a factory or write capability.
function sourceDispatchContract(facts, runId) {
  assert.equal(facts?.schemaVersion, 2)
  assert.equal(facts.kind, 'bamboo-frozen-source-context/v2')
  assert.equal(facts.publicationAuthorized, false)
  assert.match(runId, /^[1-9][0-9]*$/)
  const context = JSON.parse(JSON.stringify(facts))
  assert.deepEqual(Object.keys(context).sort(), ['controller', 'feature', 'inputs', 'kind', 'publicationAuthorized', 'schemaVersion', 'source'])
  const { source, controller, feature } = context
  for (const name of ['revision', 'tree', 'acceptedMerge', 'mainHistoryAnchor']) assert.match(source?.[name], SHA)
  assert.equal(source.acceptedMerge, source.revision)
  assert.equal(source.releaseRef, `release/${source.revision}`)
  assert.match(controller?.revision, SHA)
  assert.equal(controller.workflowPath, '.github/workflows/publish-crate.yml')
  assert.equal(controller.event, 'workflow_dispatch'); assert.equal(controller.ref, 'refs/heads/main')
  for (const name of ['baseRevision', 'headRevision', 'snapshotRevision', 'workflowDefinitionRevision']) assert.match(feature?.[name], SHA)
  for (const name of ['pr', 'runId', 'logicalAttempt', 'workflowId', 'checkSuiteId']) {
    assert.ok(Number.isSafeInteger(feature[name]) && feature[name] > 0)
  }
  assert.ok(Number.isFinite(Date.parse(feature.mergedAt))); assert.match(feature.producerDigest, DIGEST)
  assert.ok(Array.isArray(feature.physicalJobs) && context.inputs && typeof context.inputs === 'object')
  delete source.devObservedBefore; delete source.devObservedAfter
  delete controller.mainObservedBefore; delete controller.mainObservedAfter
  assert.ok(Buffer.byteLength(JSON.stringify(context)) <= 48 * 1024, 'Source-dispatch provenance exceeds its bound')
  return { schemaVersion: 1, kind: FROZEN_DISPATCH, runId, context }
}

function assertSourceDispatchCompatibility(recorded, current) {
  if (recorded === undefined && current === undefined) return
  assert.ok(recorded && current, 'Source-dispatch reservation cannot adopt another admission contract')
  assert.deepEqual(recorded, current, 'Source-dispatch immutable source/controller facts changed')
}

// A trusted controller may opt a NEW reservation into this lifecycle. There is
// no input/env switch, upgrade of a signed receipt, or new source authority.
function nativeOriginalContract(identity) {
  assert.match(identity?.workflowRevision, SHA)
  assert.match(identity?.runId, /^[1-9][0-9]*$/)
  return { schemaVersion: 1, kind: 'bamboo-native-originals/v1',
    controllerRevision: identity.workflowRevision, runId: identity.runId }
}

// Preserve original signed acceptance when the same main CI is rerun.
function assertAdmissionCompatibility(recorded, current) {
  if (recorded === undefined && current === undefined) return true
  assert.ok(recorded && current, 'Both original and current source admission records are required')
  const previous = recorded.ciRun?.run_attempt, latest = current.ciRun?.run_attempt
  assert.ok(Number.isSafeInteger(previous) && previous >= 1 &&
    Number.isSafeInteger(latest) && latest >= previous, 'Source admission main CI attempt must not move backwards')
  // Only a rerun of the same main CI may advance. Source, premerge runtime
  // acceptance and every other field remain identical to the signed original.
  assert.deepEqual({ ...recorded, ciRun: { ...recorded.ciRun, run_attempt: latest } }, current,
    'Source admission changed beyond the same main CI run attempt')
  return true
}

function validatePreparation(preparation, identity, contractVersion) {
  assert.equal(preparation?.schemaVersion, 1)
  assert.equal(preparation.kind, 'bamboo-release-preparation/v1')
  assert.ok(contractVersion === 1 || contractVersion === 2)
  assert.equal(preparation.contractVersion, contractVersion)
  for (const key of ['repository', 'sourceRevision', 'sourceTree', 'workflowRevision', 'runId']) {
    assert.equal(preparation[key], identity[key], `Release preparation mismatch: ${key}`)
  }
  for (const key of ['sourceRevision', 'sourceTree', 'workflowRevision']) assert.match(preparation[key], SHA)
  assert.match(preparation.runId, /^[1-9][0-9]*$/)
  assert.ok(Number.isSafeInteger(preparation.runAttempt) && preparation.runAttempt >= 1 &&
    preparation.runAttempt <= identity.runAttempt, 'Preparation must belong to this run or an earlier successful attempt')
  assert.ok(Number.isSafeInteger(preparation.releaseId) && preparation.releaseId > 0)
  const receipt = preparation.receipt
  validateReceipt(receipt, receipt?.identity, receipt?.crates, identity.version)
  assertSourceDispatchCompatibility(preparation.sourceDispatch, identity.sourceDispatch)
  assertSourceDispatchCompatibility(receipt.sourceDispatch, identity.sourceDispatch)
  if (receipt.sourceDispatch) {
    assert.equal(contractVersion, 2)
    assert.equal(receipt.sourceDispatch.context.source.tree, identity.sourceTree)
    assert.equal(receipt.sourceDispatch.context.controller.revision, identity.workflowRevision)
    assert.equal(receipt.sourceDispatch.runId, identity.runId)
    assert.ok(receipt.nativeOriginals, 'Source dispatch requires original native outputs')
    validateFrontendOriginals(receipt.frontendOriginals, receipt, true)
  }
  if (receipt.nativeOriginals) {
    assert.equal(contractVersion, 2, 'Original native lifecycle requires the three-target contract')
    assert.deepEqual(receipt.nativeOriginals, nativeOriginalContract(identity), 'Original controller/run must remain unchanged')
  }
  assert.equal(receipt.identity.repository, identity.repository)
  assert.equal(receipt.identity.sourceRevision, identity.sourceRevision)
  assert.equal(receipt.automatic, true)
  assert.equal(receipt.ciRun, identity.ciRun)
  assertAdmissionCompatibility(receipt.validation, identity.validation)
  assert.ok(receipt.crates.includes('bamboo-agent'))
  assert.equal(receipt.authentication?.schemaVersion, 1)
  assert.equal(receipt.authentication.algorithm, 'hmac-sha256')
  assert.deepEqual(Object.keys(receipt.authentication).sort(), ['algorithm', 'mac', 'schemaVersion'])
  assert.match(receipt.authentication.mac, DIGEST, 'Preparation requires the existing signed reservation')
  return preparation
}

function createPreparation({ release, receipt }, identity, contractVersion) {
  assert.equal(release.tag_name, `v${identity.version}`)
  assert.equal(release.target_commitish, identity.sourceRevision)
  return validatePreparation({ schemaVersion: 1, kind: 'bamboo-release-preparation/v1',
    repository: identity.repository, sourceRevision: identity.sourceRevision, sourceTree: identity.sourceTree,
    workflowRevision: identity.workflowRevision, runId: identity.runId, runAttempt: identity.runAttempt,
    contractVersion, releaseId: release.id, receipt: JSON.parse(JSON.stringify(receipt)),
    ...(identity.sourceDispatch ? { sourceDispatch: identity.sourceDispatch } : {}) }, identity, contractVersion)
}

const SOURCE_PREPARATION_KIND = 'bamboo-source-preparation-file/v1'
const SOURCE_PREPARATION_FILE = 'prepared-release.json'
const MAX_SOURCE_PREPARATION = 192 * 1024
function preparationDirectory(env) {
  const root = fs.realpathSync(env.RUNNER_TEMP), directory = path.join(root, 'bamboo-prepared-frontend')
  const stat = fs.lstatSync(directory)
  assert.ok(stat.isDirectory() && !stat.isSymbolicLink() && fs.realpathSync(directory) === directory, 'Unsafe preparation directory')
  return directory
}
function preparationInput(env = process.env) {
  const locator = JSON.parse(env.PREPARED_RELEASE)
  if (locator.kind !== SOURCE_PREPARATION_KIND) {
    assert.equal(locator.receipt?.sourceDispatch, undefined, 'Source preparation must use its original file carrier')
    return locator // Historical full-JSON transport, unchanged.
  }
  assert.deepEqual(Object.keys(locator).sort(), ['kind', 'runAttempt', 'runId', 'schemaVersion', 'sha256'])
  assert.equal(locator.schemaVersion, 1); assert.equal(locator.runId, env.GITHUB_RUN_ID)
  assert.ok(Number.isSafeInteger(locator.runAttempt) && locator.runAttempt > 0 && locator.runAttempt <= Number(env.GITHUB_RUN_ATTEMPT))
  assert.match(locator.sha256, DIGEST)
  const file = path.join(preparationDirectory(env), SOURCE_PREPARATION_FILE), stat = fs.lstatSync(file)
  assert.ok(stat.isFile() && !stat.isSymbolicLink() && fs.realpathSync(file) === file && stat.size > 0 && stat.size <= MAX_SOURCE_PREPARATION)
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0))
  let bytes
  try {
    const opened = fs.fstatSync(fd), buffer = Buffer.alloc(MAX_SOURCE_PREPARATION + 1)
    assert.ok(opened.isFile() && opened.dev === stat.dev && opened.ino === stat.ino && opened.size === stat.size)
    const count = fs.readSync(fd, buffer, 0, buffer.length, 0), after = fs.fstatSync(fd)
    assert.equal(count, stat.size); assert.equal(after.size, stat.size); assert.equal(after.mtimeMs, opened.mtimeMs)
    bytes = buffer.subarray(0, count)
  } finally { fs.closeSync(fd) }
  assert.equal(sha256(bytes), locator.sha256, 'Preparation carrier hash differs')
  const value = JSON.parse(bytes)
  assert.deepEqual(Buffer.from(canonicalJson(value)), bytes, 'Preparation carrier is not canonical')
  assert.equal(value.runId, locator.runId); assert.equal(value.runAttempt, locator.runAttempt)
  assert.ok(value.receipt?.sourceDispatch, 'Source carrier cannot upgrade a historical reservation')
  return value // Data only. Private HMAC / official physical job-log checks follow.
}
function sourcePreparationOutput(preparation, env = process.env) {
  const bytes = Buffer.from(canonicalJson(preparation))
  assert.ok(bytes.length <= MAX_SOURCE_PREPARATION, 'Source preparation file exceeds its bound')
  const root = fs.realpathSync(env.RUNNER_TEMP)
  fs.mkdirSync(path.join(root, 'bamboo-prepared-frontend'))
  const directory = preparationDirectory(env)
  fs.writeFileSync(path.join(directory, SOURCE_PREPARATION_FILE), bytes, { flag: 'wx' })
  for (const [index, name] of FRONTEND_NAMES.entries()) {
    const data = fs.readFileSync(path.join(process.cwd(), 'crates/app/bamboo-server/frontend_package', name))
    assert.equal(sha256(data), preparation.receipt.frontendBytes[index ? 'archiveSha256' : 'manifestSha256'])
    fs.writeFileSync(path.join(directory, name), data, { flag: 'wx' })
  }
  return JSON.stringify({ schemaVersion: 1, kind: SOURCE_PREPARATION_KIND, runId: preparation.runId,
    runAttempt: preparation.runAttempt, sha256: sha256(bytes) })
}

async function resumePrepared(context, preparation, identity, contractVersion) {
  validatePreparation(preparation, identity, contractVersion)
  await context.assertCurrentMain()
  context.assertSigningKey()
  context.verifyReceipt(preparation.receipt)
  validateReceipt(preparation.receipt, context.identity, context.crates, identity.version)
  const release = (await context.releases()).find((entry) => entry.id === preparation.releaseId)
  assert.ok(release, 'Prepared Release disappeared; never allocate a replacement during fan-out')
  assert.equal(release.tag_name, `v${identity.version}`)
  assert.equal(release.target_commitish, identity.sourceRevision)
  const receipt = await context.readReceipt(release, { required: true })
  validateReceipt(receipt, context.identity, context.crates, identity.version)
  assertSourceDispatchCompatibility(receipt.sourceDispatch, context.sourceDispatch)
  assertSourceDispatchCompatibility(receipt.sourceDispatch, preparation.receipt.sourceDispatch)
  if (context.bindReservation) context.bindReservation(release, receipt)
  assert.equal(receipt.automatic, true)
  assert.equal(receipt.ciRun, preparation.receipt.ciRun)
  assert.deepEqual(receipt.validation, preparation.receipt.validation)
  assert.deepEqual(receipt.frontendBytes, preparation.receipt.frontendBytes, 'Prepared frontend bytes changed')
  assert.deepEqual(receipt.nativeOriginals, preparation.receipt.nativeOriginals, 'Signed native lifecycle changed')
  if (context.sourceDispatch) assert.deepEqual(receipt.frontendOriginals, preparation.receipt.frontendOriginals, 'Signed frontend original IDs changed')
  assert.ok(release.draft || receipt.completed, 'Public release has incomplete provenance')
  for (const [name, checksum] of Object.entries(preparation.receipt.packageChecksums)) {
    assert.equal(receipt.packageChecksums[name], checksum, 'Previously prepared package checksum changed')
  }
  if (preparation.receipt.completed) assert.equal(receipt.completed, true)
  await context.ensureTag(release, receipt)
  // Preparation sealed these bytes before either worker started. Missing assets
  // now fail closed rather than allowing the crate worker to race native readers.
  await context.ensureFrontend(release, receipt, { sealed: true })
  return { release, receipt }
}

function nextVersion(versions, now = new Date(), occupiedVersions = []) {
  const prefix = `${now.getUTCFullYear()}.${now.getUTCMonth() + 1}.`
  let maximum = 0n
  for (const version of versions) {
    if (!version.startsWith(prefix) || !/^\d+$/.test(version.slice(prefix.length))) continue
    const counter = BigInt(version.slice(prefix.length))
    assert.ok(counter <= U64_MAX, 'Published release counter exceeds Cargo u64 range')
    if (counter > maximum) maximum = counter
  }
  const occupied = new Set(occupiedVersions)
  // Unauthenticated names can occupy a candidate, but cannot raise the max or
  // push publication through an unbounded number of attacker-created tags.
  for (let attempt = 0; attempt < 100; attempt++) {
    assert.ok(++maximum <= U64_MAX, 'No automatic version remains in this UTC month: Cargo u64 limit')
    const candidate = `${prefix}${maximum.toString()}`
    if (!occupied.has(candidate)) return candidate
  }
  assert.fail('Too many unauthenticated release/tag collisions; refusing automatic publication')
}

function selectVersion({ identity, crates, receipts, versions, occupiedVersions = [], now }) {
  const matches = receipts.filter((receipt) => receipt.automatic &&
    isDeepStrictEqual(receipt.identity, identity))
  assert.ok(matches.length <= 1, 'Multiple automatic reservations for one source identity')
  if (matches.length) return validateReceipt(matches[0], identity, crates).version
  return nextVersion(versions, now, occupiedVersions)
}

async function plan(context) {
  assert.equal(context.automatic, true, 'Receipt controller is only for automatic main publication')
  const { identity, crates } = context
  await context.assertCurrentMain()
  context.assertSigningKey()
  const releases = await context.releases()
  const receipts = []
  for (const release of releases) {
    const receipt = await readReleaseReceipt(context, release, identity)
    if (receipt) receipts.push(receipt)
  }
  assert.equal(new Set(receipts.map((receipt) => receipt.version)).size, receipts.length,
    'Multiple authenticated reservations for one version')
  const versions = [...await context.versions(crates), ...receipts.map((receipt) => receipt.version)]
  const occupiedVersions = [
    ...releases.map((release) => release.tag_name.replace(/^v/, '')),
    ...await context.tags()]
  const version = selectVersion({ ...context, releases, receipts, versions, occupiedVersions })
  const release = releases.find((entry) => entry.tag_name === `v${version}`)
  if (release) {
    assert.equal(release.target_commitish, identity.sourceRevision, 'Reserved release target commit changed')
    const receipt = validateReceipt(receipts.find((entry) => entry.version === version), identity, crates, version)
    assertSourceDispatchCompatibility(receipt.sourceDispatch, context.sourceDispatch)
    if (context.bindReservation) context.bindReservation(release, receipt)
    assert.ok(release.draft || receipt.completed, 'Public release has incomplete provenance')
    await context.ensureTag(release, receipt)
    await context.ensureFrontend(release, receipt)
    return { receipt, release }
  }
  const receipt = { schemaVersion: 1, version, identity, crates,
    frontendBytes: context.frontendBytes, automatic: true, completed: false,
    ciRun: context.ciRun || null, packageChecksums: {},
    ...(context.validation ? { validation: context.validation } : {}),
    ...(context.sourceDispatch ? { sourceDispatch: context.sourceDispatch } : {}),
    ...(context.nativeOriginals ? { nativeOriginals: context.nativeOriginals } : {}) }
  validateReceipt(receipt, identity, crates, version)
  // Reserve before uploading any crate. An uncertain GitHub write is recovered
  // by reading this exact tag on a rerun, never by allocating another version.
  const reserved = await context.reserve(receipt)
  if (context.bindReservation) context.bindReservation(reserved, receipt)
  await context.ensureTag(reserved, receipt)
  await context.ensureFrontend(reserved, receipt)
  return { receipt, release: reserved }
}

async function publish(context, release, receipt) {
  assert.equal(context.automatic, true, 'Receipt controller is only for automatic main publication')
  assertSourceDispatchCompatibility(receipt.sourceDispatch, context.sourceDispatch)
  assert.equal(receipt.sourceDispatch, undefined, 'Source dispatch requires the original-output crate publisher and finalizer')
  await context.assertCurrentMain()
  context.assertSigningKey()
  context.verifyReceipt(receipt)
  validateReceipt(receipt, context.identity, context.crates)
  assert.equal(receipt.automatic, true, 'Manual history cannot authorize automatic receipt recovery')
  assert.equal(release.target_commitish, receipt.identity.sourceRevision)
  await assertAutomaticVersionOrder(context, release, receipt)
  for (const crate of receipt.crates) {
    let existing = await context.registry(crate, receipt.version)
    if (!existing) {
      const checksum = await context.package(crate, receipt.version)
      assert.match(checksum, DIGEST)
      if (receipt.packageChecksums[crate]) {
        assert.equal(checksum, receipt.packageChecksums[crate], `Previously reserved package bytes changed for ${crate}`)
      }
      receipt.packageChecksums[crate] = checksum
      // Persist the exact package checksum before cargo can make the upload
      // irreversible, including a crash between upload and registry visibility.
      await context.saveReceipt(release, receipt)
      for (let attempt = 1; attempt <= 25; attempt++) {
        const result = await context.cargoPublish(crate)
        if (result.status === 0 || /already exists on crates.io index/i.test(result.output)) break
        assert.ok(/429 Too Many Requests/i.test(result.output) && attempt < 25,
          `Failed to publish ${crate}: ${result.output}`)
        const advertised = result.output.match(/try again after ([A-Za-z0-9:, ]*GMT)/i)?.[1]
        const wait = advertised ? Math.ceil((Date.parse(advertised) - Date.now()) / 1000) + 10 : 120
        await context.wait(Math.max(15, Math.min(1200, Number.isFinite(wait) ? wait : 120)))
      }
      for (let attempt = 0; attempt < 30 && !existing; attempt++) {
        existing = await context.registry(crate, receipt.version)
        if (!existing) await context.wait(10)
      }
      assert.ok(existing, `Timed out waiting for ${crate}@${receipt.version}`)
    }
    const expected = receipt.packageChecksums[crate]
    assert.ok(expected, `Existing ${crate}@${receipt.version} has no reserved package checksum`)
    assert.equal(existing.checksum, expected, `Registry checksum mismatch for ${crate}`)
    await context.verifyArchive(crate, receipt, expected)
  }
  if (!receipt.completed || release.draft) {
    receipt.completed = true
    await context.saveReceipt(release, receipt)
    await context.complete(release, receipt, await shouldMakeLatest(context, release, receipt))
  }
}

async function readReleaseReceipt(context, release, identity) {
  // Public target SHAs do not establish receipt relevance without authentication.
  let required = false
  let receipt
  try {
    receipt = await context.readReceipt(release, { required })
  } catch (error) {
    if (required || !(error instanceof assert.AssertionError || error instanceof SyntaxError || error instanceof RangeError)) throw error
    return null
  }
  if (!receipt) return null
  required ||= receipt.identity?.sourceRevision === identity.sourceRevision &&
    release.tag_name === `v${receipt.version}`
  try {
    assert.ok(receipt.identity && Array.isArray(receipt.crates), 'Invalid release receipt shape')
    validateReceipt(receipt, receipt.identity, receipt.crates)
    assert.equal(receipt.identity.repository, identity.repository)
    assert.match(receipt.identity.sourceRevision, SHA)
    assert.equal(release.target_commitish, receipt.identity.sourceRevision)
    assert.equal(release.tag_name, `v${receipt.version}`)
  } catch (error) {
    if (required) throw error
    return null
  }
  const allowMissing = !receipt.completed && Object.keys(receipt.packageChecksums).length === 0
  // Transport failures must propagate; only a successfully read, invalid tag
  // placement can be ignored for unrelated history.
  const source = await context.tagSource(receipt.version, { allowMissing })
  if ((allowMissing && source === null) || source === receipt.identity.sourceRevision) return receipt
  assert.ok(!required, 'Automatic release tag points to different source')
  return null
}

async function automaticReceipts(context, currentRelease, receipt, { stableOnly = false } = {}) {
  context.assertSigningKey()
  const receipts = []
  for (const release of await context.releases()) {
    if (release.id === currentRelease.id || (stableOnly && (release.draft || release.prerelease))) continue
    const previous = await readReleaseReceipt(context, release, receipt.identity)
    if (!previous?.automatic || (stableOnly && !previous.completed)) continue
    receipts.push(previous)
  }
  return receipts
}

function compareAutomaticVersions(left, right) {
  const components = (version) => {
    assert.match(version, /^\d+\.\d+\.\d+$/, 'Automatic versions must be numeric')
    return version.split('.').map(BigInt)
  }
  const first = components(left)
  const second = components(right)
  const difference = first.findIndex((value, index) => value !== second[index])
  return difference < 0 ? 0 : first[difference] > second[difference] ? 1 : -1
}

async function assertAutomaticVersionOrder(context, release, receipt) {
  for (const previous of await automaticReceipts(context, release, receipt)) {
    if (previous.identity.sourceRevision !== receipt.identity.sourceRevision &&
        compareAutomaticVersions(receipt.version, previous.version) >= 0) {
      assert.ok(await context.isAncestor(previous.identity.sourceRevision, receipt.identity.sourceRevision),
        'Older or unproven main source cannot publish at or above a reserved newer source version')
    }
  }
  for (const previous of await context.registryFrontier()) {
    if (previous.sourceRevision !== receipt.identity.sourceRevision) {
      if (compareAutomaticVersions(receipt.version, previous.version) >= 0) {
        assert.ok(await context.isAncestor(previous.sourceRevision, receipt.identity.sourceRevision),
          'Older or unproven main source cannot overtake verified registry source')
      } else {
        assert.ok(!await context.isAncestor(previous.sourceRevision, receipt.identity.sourceRevision),
          'Newer main source cannot publish below verified registry version')
      }
    }
  }
}

async function shouldMakeLatest(context, currentRelease, receipt) {
  // A retry may finish after a newer main source. Release numbers describe
  // allocation time, so a late first attempt for old CI can have a larger one.
  for (const previous of await automaticReceipts(context, currentRelease, receipt, { stableOnly: true })) {
    if (previous.identity.sourceRevision === receipt.identity.sourceRevision) {
      if (compareAutomaticVersions(previous.version, receipt.version) > 0) return false
    } else if (!await context.isAncestor(previous.identity.sourceRevision, receipt.identity.sourceRevision)) {
      // Only a proven descendant can take latest from a completed auto source;
      // old recovery and divergent/rewritten history both retain the latest.
      return false
    }
  }
  for (const previous of await context.registryFrontier()) {
    if (previous.sourceRevision === receipt.identity.sourceRevision) {
      if (compareAutomaticVersions(previous.version, receipt.version) > 0) return false
    } else if (!await context.isAncestor(previous.sourceRevision, receipt.identity.sourceRevision)) return false
  }
  return true
}

function gitIsAncestor(ancestor, descendant, cwd) {
  for (const revision of [ancestor, descendant]) {
    assert.match(revision, SHA)
    const exists = spawnCommand(['git', 'cat-file', '-e', `${revision}^{commit}`], { cwd })
    if (exists.error) throw exists.error
    if (exists.status !== 0) command(['git', 'fetch', '--no-tags', 'origin', revision], { cwd })
  }
  const result = spawnCommand(['git', 'merge-base', '--is-ancestor', ancestor, descendant], { cwd })
  if (result.error) throw result.error
  assert.ok(result.status === 0 || result.status === 1, `Unable to compare release source ancestry: ${result.stderr}`)
  return result.status === 0
}

function childProcessEnv(args, env = process.env) {
  const githubAuth = new Set(['GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN', 'GITHUB_ENTERPRISE_TOKEN'])
  return Object.fromEntries(Object.entries(env).filter(([name]) => {
    if (/^BAMBOO_RELEASE_/i.test(name)) return false
    if (/^(GH|GITHUB)_.*(TOKEN|PAT)/i.test(name)) return args[0] === 'gh' && githubAuth.has(name)
    if (/^CARGO_(REGISTRY_TOKEN|REGISTRIES_.*_TOKEN)$/i.test(name)) {
      return args[0] === 'cargo' && args[1] === 'publish' && name === 'CARGO_REGISTRY_TOKEN'
    }
    return true
  }))
}

function spawnCommand(args, options = {}) {
  return spawnSync(args[0], args.slice(1), { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, ...options,
    env: childProcessEnv(args, options.env || process.env) })
}

function command(args, options = {}) {
  const result = spawnCommand(args, options)
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${args.join(' ')} failed: ${result.stderr || result.stdout}`)
  return result.stdout
}

function jsonCommand(args) { return JSON.parse(command(args)) }

function githubArgs(repository, route, payload, method = payload === undefined ? 'GET' : 'POST') {
  const args = ['gh', 'api', `repos/${repository}/${route}`]
  if (payload !== undefined) args.push('--method', method, '--input', '-')
  return args
}

function github(repository, route, payload, method, allowMissing = false) {
  const args = githubArgs(repository, route, payload, method)
  const result = spawnCommand(args, {
    input: payload === undefined ? undefined : JSON.stringify(payload) })
  if (result.error) throw result.error
  const parsed = result.stdout ? JSON.parse(result.stdout) : null
  if (allowMissing && result.status !== 0 && String(parsed?.status) === '404') return null
  assert.equal(result.status, 0, `GitHub request failed: ${result.stderr || result.stdout}`)
  return parsed
}

function receiptBody(receipt) {
  const { repository, sourceRevision, frontend } = receipt.identity
  return `${receipt.completed ? 'Published' : 'Reserved'} Bamboo ${receipt.version}.\n\n` +
    `Source: [${sourceRevision}](https://github.com/${repository}/commit/${sourceRevision})\n\n` +
    `Frontend: ${frontend.packageName}@${frontend.packageVersion}\n\n` +
    (receipt.ciRun ? `${receipt.sourceDispatch ? 'Feature acceptance CI' : 'Main publication CI'}: ${receipt.ciRun}\n\n` : '') +
    (receipt.sourceDispatch ? `Release controller: https://github.com/${repository}/actions/runs/${receipt.sourceDispatch.runId} ` +
      `(C ${receipt.sourceDispatch.context.controller.revision}).\n\n` : '') +
    (receipt.validation ? `Full pre-merge acceptance: ${receipt.validation.premerge.url} ` +
      `(attempt ${receipt.validation.premerge.runAttempt}, tested snapshot ${receipt.validation.premerge.snapshotRevision}).\n\n` : '') +
    'Provenance binds the source, preserved frontend bytes and each crate checksum.\n\n' +
    `<!-- bamboo-release-provenance\n${JSON.stringify(receipt)}\n-->`
}

function readBodyReceipt(release) {
  const encoded = release.body?.match(/<!-- bamboo-release-provenance\n([^\n]+)\n-->/)?.[1]
  return encoded ? JSON.parse(encoded) : null
}

function frontendIdentity(packageName, packageVersion, manifest, lock) {
  assert.equal(manifest.frontend_version, packageVersion)
  assert.match(manifest.bundle_hash, /^sha256:[0-9a-f]{64}$/)
  // Staging timestamps and zip mtimes are deliberately outside the immutable
  // identity. The original packaged bytes are stored once as draft assets.
  return { packageName, packageVersion, bundleHash: manifest.bundle_hash,
    lock: packageName === '@bigduu/lotus-next' ? lock : null }
}

function verifyPackageArchive(bytes, receipt, name, expected, entry) {
  assert.equal(verifyRegistrySource(bytes, expected, entry), receipt.identity.sourceRevision,
    `Published ${name} source revision mismatch`)
  if (name === 'bamboo-server') {
    assert.equal(sha256(entry('frontend_package/frontend-manifest.json')), receipt.frontendBytes.manifestSha256)
    assert.equal(sha256(entry('frontend_package/lotus-frontend.zip')), receipt.frontendBytes.archiveSha256)
  }
}

function verifyRegistrySource(bytes, checksum, entry) {
  assert.match(checksum, DIGEST)
  assert.equal(sha256(bytes), checksum, 'Downloaded package checksum mismatch')
  const revision = JSON.parse(entry('.cargo_vcs_info.json').toString('utf8')).git?.sha1
  assert.match(revision, SHA, 'Registry archive has no valid source revision')
  return revision
}

function highestStableVersion(versions) {
  return versions.filter((version) => /^\d+\.\d+\.\d+$/.test(version))
    .reduce((highest, version) => highest === null || compareAutomaticVersions(version, highest) > 0 ? version : highest, null)
}

function verifyPreservedFrontend(staged, restored) {
  const semanticManifest = (bytes) => {
    const source = bytes.toString('utf8')
    const manifest = JSON.parse(source)
    assert.equal(source, `${JSON.stringify(manifest, null, 2)}\n`, 'Preserved frontend manifest must be canonical')
    assert.equal(new Date(manifest.built_at).toISOString(), manifest.built_at)
    const { built_at, ...semantic } = manifest
    return semantic
  }
  assert.deepEqual(semanticManifest(restored[0]), semanticManifest(staged[0]),
    'Preserved frontend manifest differs from the verified pinned package')
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-release-frontend-'))
  try {
    for (const [prefix, bytes] of [['staged', staged], ['restored', restored]]) {
      fs.writeFileSync(path.join(directory, `${prefix}.json`), bytes[0])
      fs.writeFileSync(path.join(directory, `${prefix}.zip`), bytes[1])
    }
    command(['python3', path.join(__dirname, 'crate-release-frontend.py'), directory])
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

async function preserveFrontend({ receipt, stagedBytes, assets, readAsset, verifyFiles, writeFiles, saveReceipt, upload, sealed = false }) {
  if (assets.every(Boolean)) {
    const bytes = await Promise.all(assets.map(readAsset))
    if (sha256(bytes[0]) === receipt.frontendBytes.manifestSha256 &&
        sha256(bytes[1]) === receipt.frontendBytes.archiveSha256) {
      await verifyFiles(bytes)
      await writeFiles(bytes)
      return
    }
  }
  assert.equal(sealed, false, 'Prepared release has lost or changed its sealed frontend assets')
  assert.deepEqual(receipt.packageChecksums, {}, 'Partial release has lost or changed its preserved frontend assets')
  // Only before the first package is reserved can interrupted initial asset
  // uploads adopt freshly staged bytes of the same verified immutable bundle.
  receipt.frontendBytes = stagedBytes
  await saveReceipt(receipt)
  await upload()
}

const FRONTEND_NAMES = ['frontend-manifest.json', 'lotus-frontend.zip']
function validateFrontendOriginals(value, receipt, complete = false) {
  assert.deepEqual(Object.keys(value || {}).sort(), ['assets', 'complete', 'schemaVersion'])
  assert.equal(value.schemaVersion, 1); assert.equal(typeof value.complete, 'boolean')
  assert.ok(Array.isArray(value.assets) && value.assets.length <= 2)
  assert.equal(new Set(value.assets.map(asset => asset.name)).size, value.assets.length)
  for (const asset of value.assets) {
    assert.deepEqual(Object.keys(asset).sort(), ['assetId', 'name', 'sha256', 'sizeBytes'])
    assert.ok(FRONTEND_NAMES.includes(asset.name) && Number.isSafeInteger(asset.assetId) && asset.assetId > 0)
    assert.ok(Number.isSafeInteger(asset.sizeBytes) && asset.sizeBytes > 0 && asset.sizeBytes <= 256 * 1024 * 1024)
    assert.equal(asset.sha256, receipt.frontendBytes[asset.name === FRONTEND_NAMES[0] ? 'manifestSha256' : 'archiveSha256'])
  }
  if (value.complete || complete) { assert.equal(value.complete, true); assert.equal(value.assets.length, 2) }
  return value
}
async function preserveFrontendOriginals({ receipt, bytes, listAssets, readAsset, upload, saveReceipt, sealed = false }) {
  assert.ok(receipt.sourceDispatch, 'Only the authenticated Source contract selects this lifecycle')
  if (!receipt.frontendOriginals) {
    assert.equal(sealed, false, 'Sealed frontend originals are missing')
    receipt.frontendOriginals = { schemaVersion: 1, complete: false, assets: [] }; await saveReceipt()
  }
  const record = validateFrontendOriginals(receipt.frontendOriginals, receipt, sealed)
  for (const [index, name] of FRONTEND_NAMES.entries()) {
    const known = record.assets.find(asset => asset.name === name)
    let matches = (await listAssets()).filter(asset => asset.name === name)
    assert.ok(matches.length <= 1, 'Ambiguous original frontend asset')
    if (!matches.length) {
      assert.ok(!known && !record.complete && !sealed, 'Recorded frontend original disappeared')
      let error
      try { await upload(name, bytes[index]) } catch (failure) { error = failure }
      matches = (await listAssets()).filter(asset => asset.name === name)
      if (!matches.length && error) throw error
    }
    assert.equal(matches.length, 1, 'Frontend upload is unconfirmed')
    const asset = matches[0], expected = { assetId: asset.id, name, sizeBytes: asset.size, sha256: asset.digest?.replace(/^sha256:/, '') }
    assert.equal(asset.state, 'uploaded'); assert.equal(expected.sizeBytes, bytes[index].length)
    assert.equal(expected.sha256, sha256(bytes[index]))
    if (known) assert.deepEqual(expected, known, 'Original frontend asset ID or bytes changed')
    assert.deepEqual(await readAsset(asset.id, asset.size), bytes[index], 'Original frontend asset bytes changed')
    if (!known) { record.assets.push(expected); await saveReceipt() }
  }
  if (!record.complete) { record.complete = true; await saveReceipt() }
  validateFrontendOriginals(record, receipt, true)
}
function originalCrateInputs(receipt, name, pathInVcs) {
  return { repository: receipt.identity.repository, releaseId: receipt.releaseId, sourceRevision: receipt.identity.sourceRevision,
    sourceTree: receipt.sourceDispatch.context.source.tree, version: receipt.version, name, pathInVcs, frontendBytes: receipt.frontendBytes }
}
async function publishOriginalCrates(context) {
  const original = require('./release-crate-output.cjs'), sandbox = require('./release-cargo-sandbox.cjs')
  let metadata
  for (const name of context.crates) {
    const fresh = await context.currentReservation(), previous = fresh.receipt.crateOriginals?.[name]
    if (!previous && !metadata) metadata = jsonCommand(['cargo', 'metadata', '--format-version', '1', '--no-deps', '--locked'])
    const manifest = metadata?.packages.find(value => value.name === name)?.manifest_path
    assert.ok(previous || typeof manifest === 'string', 'Cargo metadata omitted the selected crate')
    const vcs = previous?.inputs.pathInVcs ?? path.relative(process.cwd(), path.dirname(manifest)).split(path.sep).join('/')
    const expected = originalCrateInputs({ ...fresh.receipt, releaseId: fresh.release.id }, name, vcs)
    await original.publishCrate(expected, {
      environment: process.env, read: context.read, readAsset: context.readAsset, guard: context.assertSourceCurrent,
      load: async () => (await context.currentReservation()).receipt.crateOriginals?.[name] || null,
      save: async (next, before) => {
        const current = await context.currentReservation(), receipt = current.receipt
        assert.deepEqual(receipt.crateOriginals?.[name] || null, before, 'Concurrent crate checkpoint changed')
        receipt.crateOriginals = { ...receipt.crateOriginals, [name]: next }
        if (next.sealed) receipt.packageChecksums[name] = next.sealed.proof.archive.sha256
        await context.saveReceipt(current.release, receipt)
      },
      prepare: () => original.prepareFromSandbox(JSON.parse(fs.readFileSync(process.env.BAMBOO_CARGO_SANDBOX)), expected),
      uploadAsset: (assetName, bytes) => context.uploadOriginal(fresh.release, assetName, bytes),
      registry: context.registry, registryArchive: context.registryArchive,
      upload: (bytes, info) => require('./release-cargo-upload.cjs').uploadArchive(bytes, info, process.env.CARGO_REGISTRY_TOKEN), wait: context.wait,
    })
  }
  const fresh = await context.currentReservation()
  for (const name of context.crates) assert.equal(fresh.receipt.crateOriginals?.[name]?.phase, 'published')
  // This physical owner is still running. The whole-release finalizer separately
  // rereads every original ZIP/registry byte and requires completed producer jobs.
  fresh.receipt.completed = true; await context.saveReceipt(fresh.release, fresh.receipt)
  return fresh
}
async function readOriginalCrateClosure(context, fresh) {
  const expected = context.crates.map(name => originalCrateInputs({ ...fresh.receipt, releaseId: fresh.release.id }, name,
    fresh.receipt.crateOriginals?.[name]?.inputs.pathInVcs))
  return require('./release-crate-output.cjs').readVerifiedClosure(expected, context.crates.map(name => fresh.receipt.crateOriginals?.[name]),
    { read: context.read, readAsset: context.readAsset, registry: context.registry, registryArchive: context.registryArchive })
}

async function registryFetch(url, allowMissing = false) {
  const response = await fetch(url, { headers: { 'User-Agent': 'bamboo-publish-crate/2.0 (+https://github.com/bigduu/Bamboo-agent)' } })
  if (allowMissing && response.status === 404) return null
  assert.ok(response.ok, `Registry request failed (${response.status}): ${url}`)
  return response
}

// A successful queued run does not authorize publication after main advances.
// Re-read the fixed repository immediately before each external write; failures
// stop before the write. Historical recovery keeps its existing authority path.
async function assertCurrentMain(repository, sourceRevision, readBranch = () => github(repository, 'branches/main')) {
  assert.equal(repository, 'bigduu/Bamboo-agent')
  assert.match(sourceRevision, /^[0-9a-f]{40}$/)
  const branch = await readBranch()
  assert.equal(branch.name, 'main')
  assert.equal(branch.commit?.sha, sourceRevision, 'Publication source is no longer current main')
}

async function currentMainWrite(guard, write) {
  await guard()
  return write()
}

const SUITE_QUERY = 'query($suite:ID!){node(id:$suite){... on CheckSuite{databaseId ' +
  'repository{databaseId nameWithOwner} app{databaseId slug} commit{oid} ' +
  'workflowRun{databaseId event runAttempt workflow{databaseId name} ' +
  'file{path repositoryName repositoryFileUrl}}}}}'
const FROZEN_CONTROLLER_FILES = Object.freeze([
  'crate-release.cjs', 'frozen-source-admission.cjs', 'compute-publish-order.py',
  'crate-release-frontend.py', 'crate-release-lock.py', 'release-cargo-sandbox.cjs',
  'release-cargo-upload.cjs', 'release-crate-output.cjs', 'release-frontend-policy.cjs',
  'frontend-package.cjs', 'lotus-next-artifact.cjs', 'sidecar-release.cjs',
  'sidecar-build.cjs', 'sidecar-browser-host.py', 'native-originals.cjs', 'native-originals-zip.py',
])

function frozenSourceReads() {
  const read = (args, maximum = 24 * 1024 * 1024) => command(['gh', 'api', '--hostname', 'github.com', ...args],
    { maxBuffer: maximum, timeout: 30000 })
  return {
    json: async route => {
      assert.ok(typeof route === 'string' && /^[A-Za-z0-9_./?=&%+-]+$/.test(route) &&
        !route.startsWith('/') && !route.split('/').includes('..'), 'Invalid read-only source route')
      return JSON.parse(read(['--method', 'GET', `repos/bigduu/Bamboo-agent/${route}`]))
    },
    checkSuite: async id => {
      assert.match(id, /^[A-Za-z0-9_=-]{1,128}$/)
      const result = JSON.parse(read(['graphql', '-f', `query=${SUITE_QUERY}`, '-f', `suite=${id}`]))
      assert.ok(!result.errors, 'Official check-suite query failed')
      return result.data?.node
    },
    jobLog: async id => {
      assert.ok(Number.isSafeInteger(id) && id > 0)
      return read(['--method', 'GET', `repos/bigduu/Bamboo-agent/actions/jobs/${id}/logs`], 262144)
    },
  }
}
async function paginatedAssets(read, releaseId) {
  const values = [], ids = new Set()
  for (let page = 1; page <= 20; page++) {
    const rows = await read(`releases/${releaseId}/assets?per_page=100&page=${page}`)
    assert.ok(Array.isArray(rows) && rows.length <= 100)
    for (const asset of rows) { assert.ok(!ids.has(asset.id)); ids.add(asset.id); values.push(asset) }
    if (rows.length < 100) return values
  }
  assert.fail('Original asset inventory is truncated')
}

function frozenDispatchInput() {
  const env = process.env
  assert.equal(env.GITHUB_EVENT_NAME, 'workflow_dispatch')
  assert.ok(!env.GH_HOST || env.GH_HOST === 'github.com', 'Source dispatch requires the fixed GitHub origin')
  assert.match(env.GITHUB_RUN_ID, /^[1-9][0-9]*$/)
  assert.match(env.GITHUB_RUN_ATTEMPT, /^[1-9][0-9]*$/)
  const event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8'))
  assert.equal(event.repository?.full_name, 'bigduu/Bamboo-agent')
  assert.equal(event.repository.id, 1164779771)
  const input = event.inputs
  assert.equal(input?.release_source_contract, 'frozen-source-v2', 'Missing explicit source-dispatch contract')
  assert.ok(input.dry_run === undefined || input.dry_run === false || input.dry_run === 'false')
  assert.ok(!input.version, 'Frozen-source versions come only from the existing allocator')
  assert.ok(!input.frontend_package || input.frontend_package === '@bigduu/lotus-next')
  assert.ok(!input.lotus_version || input.lotus_version === 'from_lock')
  for (const key of ['feature_pr', 'accepted_ci_run']) assert.match(input[key], /^[1-9][0-9]*$/)
  const featurePr = Number(input.feature_pr), ciRun = Number(input.accepted_ci_run)
  assert.ok(Number.isSafeInteger(featurePr) && Number.isSafeInteger(ciRun))
  return { sourceRevision: input.expected_source_sha, releaseRef: input.release_ref, featurePr, ciRun }
}

// Public read-only entrypoint. No injected policy, JSON proof, signer or write
// adapter is accepted. Tests may mock transport/module boundaries, not install a
// fixture producer into this production call.
async function readFrozenSourceContext() {
  assert.equal(arguments.length, 0, 'Source context accepts only the actual dispatch event')
  const input = frozenDispatchInput(), reads = frozenSourceReads()
  const facts = await require('./frozen-source-admission.cjs').resolveFrozenSource(input, reads,
    { mainHistoryAnchor: MAIN_HISTORY_ANCHOR })
  const run = await reads.json(`actions/runs/${process.env.GITHUB_RUN_ID}`)
  assert.equal(run.id, Number(process.env.GITHUB_RUN_ID))
  assert.equal(run.run_attempt, Number(process.env.GITHUB_RUN_ATTEMPT))
  for (const repo of [run.repository, run.head_repository]) {
    assert.equal(repo?.id, 1164779771); assert.equal(repo.full_name, 'bigduu/Bamboo-agent')
  }
  assert.equal(run.name, 'Publish Crate'); assert.equal(run.path, '.github/workflows/publish-crate.yml')
  assert.equal(run.event, 'workflow_dispatch'); assert.equal(run.head_branch, 'main')
  assert.equal(run.head_sha, process.env.GITHUB_WORKFLOW_SHA)
  assert.equal(run.status, 'in_progress'); assert.equal(run.conclusion, null)
  sourceDispatchContract(facts, process.env.GITHUB_RUN_ID)
  return facts
}
async function readFrozenCheckout() {
  assert.equal(arguments.length, 0)
  const facts = await readFrozenSourceContext(); assertFrozenController(facts); return facts
}

function assertFrozenController(facts) {
  const root = fs.realpathSync(path.resolve(__dirname, '..')), product = fs.realpathSync(process.cwd())
  assert.notEqual(root, product, 'Trusted C helpers must be installed outside the product checkout S')
  assert.equal(command(['git', 'rev-parse', 'HEAD'], { cwd: root }).trim(), facts.controller.revision)
  assert.equal(command(['git', 'rev-parse', 'HEAD']).trim(), facts.source.revision)
  assert.equal(command(['git', 'rev-parse', 'HEAD^{tree}']).trim(), facts.source.tree)
  for (const name of FROZEN_CONTROLLER_FILES) {
    const file = path.join(root, 'scripts', name), stat = fs.lstatSync(file)
    assert.ok(stat.isFile() && !stat.isSymbolicLink() && fs.realpathSync(file).startsWith(root + path.sep),
      'Controller helper must be a regular file inside C')
    const entry = command(['git', 'ls-tree', facts.controller.revision, '--', `scripts/${name}`], { cwd: root }).trim()
    assert.match(entry, new RegExp(`^100(?:644|755) blob [0-9a-f]{40}\\tscripts/${name.replaceAll('.', '\\.')}$`))
    const original = command(['git', 'show', `${facts.controller.revision}:scripts/${name}`], { cwd: root, encoding: null })
    assert.equal(sha256(fs.readFileSync(file)), sha256(original), 'Controller helper bytes differ from actual C')
  }
  // Read S only as verified data. Do not parse a candidate-controlled symlink or
  // differing JSON before checking its immutable source identity. Cargo.lock is
  // separately refreshed by the existing temporary stamping/lock contract.
  for (const name of ['frontendLock', 'frontendManifest', 'frontendZip', 'nativeContract']) {
    const input = facts.inputs[name], file = path.join(product, input.path), stat = fs.lstatSync(file)
    assert.ok(stat.isFile() && !stat.isSymbolicLink() && fs.realpathSync(file).startsWith(product + path.sep),
      'Frozen source input must be a regular file inside S')
    assert.equal(sha256(fs.readFileSync(file)), input.sha256, 'Frozen source input bytes changed')
  }
}

// The only new privileged factory calls the real read-only verifier itself.
// It extends the existing allocator/HMAC writer; old factories/guards stay on
// their historical workflow_run or recovery contracts.
async function makeFrozenSourceContext() {
  assert.equal(arguments.length, 0, 'No caller-supplied source authorization')
  const facts = await readFrozenCheckout(), env = process.env
  const verificationEnv = Object.fromEntries(['PATH', 'HOME'].filter(name => env[name] !== undefined).map(name => [name, env[name]]))
  verificationEnv.TMPDIR = env.RUNNER_TEMP
  verificationEnv.LOTUS_SOURCE = 'committed'
  verificationEnv.LOTUS_PACKAGE_NAME = '@bigduu/lotus-next'
  command([process.execPath, path.join(__dirname, 'frontend-package.cjs'), 'verify', '--source-root', fs.realpathSync(process.cwd())],
    { env: verificationEnv, timeout: 60000, maxBuffer: 1024 * 1024 })
  const sourceDispatch = sourceDispatchContract(facts, env.GITHUB_RUN_ID)
  let context, bound, justReserved
  const guard = async () => {
    const current = await readFrozenSourceContext()
    assertSourceDispatchCompatibility(sourceDispatch, sourceDispatchContract(current, env.GITHUB_RUN_ID))
    assertFrozenController(facts)
    if (!bound) return
    const release = github('bigduu/Bamboo-agent', `releases/${bound.id}`)
    assert.equal(release.id, bound.id); assert.equal(release.tag_name, `v${bound.version}`)
    assert.equal(release.target_commitish, facts.source.revision)
    const receipt = readAuthenticatedReceipt(release, env.BAMBOO_RELEASE_SIGNING_KEY, { required: true })
    validateReceipt(receipt, context.identity, context.crates, bound.version)
    assertSourceDispatchCompatibility(receipt.sourceDispatch, sourceDispatch)
    assert.deepEqual(receipt.nativeOriginals, context.nativeOriginals)
    assert.deepEqual(receipt.frontendBytes, context.frontendBytes)
    assert.deepEqual(receipt.frontendBytes, bound.frontendBytes)
    if (prepared) assert.deepEqual(receipt.frontendOriginals, prepared.receipt.frontendOriginals, 'Reserved frontend original IDs changed')
    assert.ok(release.draft || receipt.completed, 'Reserved release became public without completion')
    const tag = github('bigduu/Bamboo-agent', `git/ref/tags/v${bound.version}`, undefined, undefined, bound.uncreatedTag)
    if (!tag) assert.ok(bound.uncreatedTag, 'Reserved version tag disappeared')
    else { assert.equal(tag.object?.type, 'commit'); assert.equal(tag.object.sha, facts.source.revision) }
    await assertAutomaticVersionOrder(context, release, receipt)
  }
  let prepared
  if (env.PREPARED_RELEASE) {
    prepared = preparationInput(env)
    authenticateReceipt(prepared.receipt, env.BAMBOO_RELEASE_SIGNING_KEY)
    assertSourceDispatchCompatibility(prepared.receipt.sourceDispatch, sourceDispatch)
    assert.equal(env.TARGET_VERSION, prepared.receipt.version)
  } else assert.ok(!env.GITHUB_JOB || env.GITHUB_JOB === 'automatic', 'A worker/finalizer requires the original signed preparation')
  context = makeVerifiedContext({ ...env, FRONTEND_PACKAGE: facts.inputs.frontend.packageName,
    FRONTEND_VERSION: facts.inputs.frontend.packageVersion }, 'bigduu/Bamboo-agent', facts.source.revision,
    { html_url: `https://github.com/bigduu/Bamboo-agent/actions/runs/${facts.feature.runId}` }, true, false,
    { guard, crates: prepared?.receipt.crates })
  assert.deepEqual(context.identity.frontend.lock, facts.inputs.frontend, 'Publication frontend lock differs from tested S')
  assert.equal(context.frontendBytes.manifestSha256, facts.inputs.frontendManifest.sha256)
  assert.equal(context.frontendBytes.archiveSha256, facts.inputs.frontendZip.sha256)
  context.sourceDispatch = sourceDispatch
  context.nativeOriginals = nativeOriginalContract({ workflowRevision: facts.controller.revision, runId: env.GITHUB_RUN_ID })
  context.assertSourceCurrent = guard
  context.bindReservation = (release, receipt) => {
    assert.ok(Number.isSafeInteger(release.id) && release.id > 0)
    context.verifyReceipt(receipt)
    validateReceipt(receipt, context.identity, context.crates)
    assertSourceDispatchCompatibility(receipt.sourceDispatch, sourceDispatch)
    assert.deepEqual(receipt.nativeOriginals, context.nativeOriginals)
    assert.deepEqual(receipt.frontendBytes, context.frontendBytes)
    if (prepared) assert.deepEqual(receipt.frontendOriginals, prepared.receipt.frontendOriginals, 'Reserved frontend original IDs changed')
    const next = { id: release.id, version: receipt.version, frontendBytes: { ...receipt.frontendBytes },
      uncreatedTag: justReserved === release.id }
    if (bound) assert.deepEqual({ ...next, uncreatedTag: bound.uncreatedTag }, bound, 'Reservation identity changed')
    bound ||= next
  }
  const checkRequest = (release, receipt, version = bound?.version) => {
    validateReceipt(receipt, context.identity, context.crates, version)
    assertSourceDispatchCompatibility(receipt.sourceDispatch, sourceDispatch)
    assert.deepEqual(receipt.nativeOriginals, context.nativeOriginals)
    assert.deepEqual(receipt.frontendBytes, context.frontendBytes)
    if (prepared) assert.deepEqual(receipt.frontendOriginals, prepared.receipt.frontendOriginals, 'Reserved frontend original IDs changed')
    if (release) {
      assert.ok(bound, 'Missing bound Source-dispatch reservation')
      assert.equal(release.id, bound.id); assert.equal(release.tag_name, `v${bound.version}`)
      assert.equal(release.target_commitish, facts.source.revision)
      assert.deepEqual(receipt.frontendBytes, bound.frontendBytes)
      for (const state of Object.values(receipt.crateOriginals || {})) assert.equal(state.inputs.releaseId, release.id)
    }
  }
  const reserve = context.reserve, ensureTag = context.ensureTag, saveReceipt = context.saveReceipt
  context.reserve = async receipt => {
    assert.equal(bound, undefined, 'Never allocate another Source-dispatch reservation')
    checkRequest(undefined, receipt, receipt.version)
    const release = await reserve(receipt); justReserved = release.id; return release
  }
  context.ensureTag = async (release, receipt) => {
    checkRequest(release, receipt)
    await guard(); await ensureTag(release, receipt)
    assert.ok(bound, 'Missing bound reservation'); bound.uncreatedTag = false
  }
  context.saveReceipt = async (release, receipt) => {
    checkRequest(release, receipt)
    const previous = await context.readReceipt(github(context.identity.repository, `releases/${release.id}`), { required: true })
    if (previous.frontendOriginals) for (const asset of previous.frontendOriginals.assets) {
      assert.deepEqual(receipt.frontendOriginals?.assets.find(value => value.name === asset.name), asset, 'Frontend original cannot be replaced')
    }
    if (previous.frontendOriginals?.complete) assert.equal(receipt.frontendOriginals.complete, true)
    return saveReceipt(release, receipt)
  }
  context.read = frozenSourceReads().json
  context.listAssets = async id => paginatedAssets(context.read, id)
  context.readAsset = async (id, maximum = 68 * 1024 * 1024) => command(['gh', 'api', '--hostname', 'github.com',
    `repos/bigduu/Bamboo-agent/releases/assets/${id}`, '-H', 'Accept: application/octet-stream'], { encoding: null, maxBuffer: maximum })
  context.uploadOriginal = async (release, name, bytes) => {
    assert.equal(path.basename(name), name); await guard()
    const directory = fs.mkdtempSync(path.join(path.dirname(context.receiptFile), 'original-')), file = path.join(directory, name)
    fs.writeFileSync(file, bytes, { flag: 'wx' })
    return command(['gh', 'release', 'upload', `v${bound.version}`, file, '--repo', context.identity.repository])
  }
  context.ensureFrontend = async (release, receipt, options = {}) => preserveFrontendOriginals({ receipt,
    bytes: FRONTEND_NAMES.map(name => fs.readFileSync(path.join(process.cwd(), 'crates/app/bamboo-server/frontend_package', name))),
    listAssets: () => context.listAssets(release.id), readAsset: context.readAsset,
    upload: (name, bytes) => context.uploadOriginal(release, name, bytes), saveReceipt: () => context.saveReceipt(release, receipt), ...options })
  context.currentReservation = async () => {
    assert.ok(bound, 'Source reservation has not been bound')
    const release = await context.read(`releases/${bound.id}`), receipt = await context.readReceipt(release, { required: true })
    checkRequest(release, receipt); return { release, receipt }
  }
  context.publishMetadata = async (release, receipt, latest) => {
    assert.equal(env.GITHUB_JOB, 'publish-executables'); checkRequest(release, receipt)
    assert.equal(receipt.completed, true); await guard()
    return github(context.identity.repository, `releases/${release.id}`, { tag_name: `v${receipt.version}`,
      draft: false, make_latest: latest ? 'true' : 'false', body: receiptBody(receipt) }, 'PATCH')
  }
  if (prepared) {
    validatePreparation(prepared, preparationIdentity(context, prepared.receipt.version), 2)
    const release = await context.read(`releases/${prepared.releaseId}`), receipt = await context.readReceipt(release, { required: true })
    context.bindReservation(release, receipt); await guard()
  }
  // Wiring the original crate outputs and sole whole-release completion is a
  // separate reviewed lifecycle step. This factory can never fall back to the
  // old package/publish/complete implementation merely by enabling a producer.
  for (const name of ['package', 'cargoPublish', 'complete']) context[name] = async () => {
    assert.fail('Source dispatch requires the original-output publisher/finalizer adapter')
  }
  return context
}

function makeContext(env = process.env) {
  const repository = env.GITHUB_REPOSITORY
  assert.equal(repository, 'bigduu/Bamboo-agent')
  const sourceRevision = command(['git', 'rev-parse', 'HEAD']).trim()
  const event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8'))
  const validation = env.SOURCE_VALIDATION ? JSON.parse(env.SOURCE_VALIDATION) : undefined
  const automatic = validateSource({ eventName: env.GITHUB_EVENT_NAME, event, repository, sourceRevision,
    sourceTree: command(['git', 'rev-parse', 'HEAD^{tree}']).trim(), validation })
  const context = makeVerifiedContext(env, repository, sourceRevision, validation?.ciRun || event.workflow_run, automatic, true)
  if (validation) context.validation = validation
  return context
}

function makeRecoveryContext(env = process.env) {
  const source = command(['git', 'rev-parse', 'HEAD']).trim()
  const run = require('./crate-release-recover.cjs').authorize(env, undefined, source)
  return makeVerifiedContext(env, env.GITHUB_REPOSITORY, source, run, true)
}

function makeVerifiedContext(env, repository, sourceRevision, run, automatic, requireCurrentMain = false, authority) {
  if (!authority) assert.ok(env.CARGO_REGISTRY_TOKEN, 'Missing CARGO_REGISTRY_TOKEN')
  assertSigningConfiguration(env.BAMBOO_RELEASE_SIGNING_KEY, env.BAMBOO_RELEASE_SIGNING_KEY_SHA256)
  const frontendDirectory = authority ? path.join(process.cwd(), 'crates/app/bamboo-server/frontend_package') :
    'crates/app/bamboo-server/frontend_package'
  const manifest = JSON.parse(fs.readFileSync(`${frontendDirectory}/frontend-manifest.json`, 'utf8'))
  const identity = { repository, sourceRevision, frontend: frontendIdentity(
    env.FRONTEND_PACKAGE, env.FRONTEND_VERSION, manifest,
    JSON.parse(fs.readFileSync(authority ? path.join(process.cwd(), 'scripts/frontend-package-lock.json') : 'scripts/frontend-package-lock.json', 'utf8')),
  ) }
  const stagedFrontend = [fs.readFileSync(`${frontendDirectory}/frontend-manifest.json`),
    fs.readFileSync(`${frontendDirectory}/lotus-frontend.zip`)]
  const frontendBytes = {
    manifestSha256: sha256(stagedFrontend[0]),
    archiveSha256: sha256(stagedFrontend[1]),
  }
  assert.ok(['@bigduu/lotus-next', '@bigduu/lotus'].includes(identity.frontend.packageName))
  assertVersion(identity.frontend.packageVersion)
  const crates = authority?.crates || command(['python3', authority ? path.join(__dirname, 'compute-publish-order.py') : 'scripts/compute-publish-order.py'],
    authority ? { cwd: process.cwd() } : {}).trim().split('\n')
  assert.ok(crates.length && crates.includes('bamboo-agent'))
  for (const crate of crates) assert.match(crate, /^[a-zA-Z0-9_-]+$/)
  assert.equal(new Set(crates).size, crates.length)
  const directory = path.join(env.RUNNER_TEMP || os.tmpdir(), `bamboo-release-${env.GITHUB_RUN_ID}`)
  fs.mkdirSync(directory, { recursive: true })
  const receiptFile = path.join(directory, RECEIPT_NAME)
  const verifiedSources = new Map()
  const paginated = (route) => jsonCommand(['gh', 'api', '--paginate', '--slurp', `repos/${repository}/${route}`]).flat()
  const registryVersions = async (name) => {
    const response = await registryFetch(`https://crates.io/api/v1/crates/${name}`, true)
    return response ? (await response.json()).versions.map((version) => version.num) : []
  }
  const downloadArchive = async (name, version) => {
    const response = await registryFetch(`https://crates.io/api/v1/crates/${name}/${version}/download`)
    const bytes = Buffer.from(await response.arrayBuffer())
    const archive = path.join(directory, `${name}-${version}.crate`)
    fs.writeFileSync(archive, bytes)
    return { bytes, entry: (file) => command(['tar', '-xOf', archive, `${name}-${version}/${file}`], { encoding: null }) }
  }
  const guard = authority?.guard || (requireCurrentMain ? () => assertCurrentMain(repository, sourceRevision) : async () => {})
  const write = (action) => currentMainWrite(guard, action)
  const context = { automatic, identity, frontendBytes, crates, receiptFile,
    assertCurrentMain: guard,
    ciRun: run.html_url,
    releases: async () => paginated('releases?per_page=100'),
    tags: async () => paginated('tags?per_page=100').map((tag) => tag.name.replace(/^v/, '')),
    versions: async (names) => {
      const versions = []
      for (const name of names) {
        versions.push(...await registryVersions(name))
      }
      return versions
    },
    registryFrontier: async () => {
      const frontier = []
      // Refresh each list at both publication boundaries. Only immutable
      // archive/version/checksum proofs are cached, never a moving latest.
      for (const name of crates) {
        const version = highestStableVersion(await registryVersions(name))
        if (version === null) continue
        const metadata = await context.registry(name, version)
        assert.ok(metadata, 'Registry frontier version disappeared')
        const key = `${name}@${version}:${metadata.checksum}`
        if (!verifiedSources.has(key)) {
          const { bytes, entry } = await downloadArchive(name, version)
          verifiedSources.set(key, verifyRegistrySource(bytes, metadata.checksum, entry))
        }
        frontier.push({ version, sourceRevision: verifiedSources.get(key) })
      }
      return frontier
    },
    readReceipt: async (release, options) => readAuthenticatedReceipt(release, env.BAMBOO_RELEASE_SIGNING_KEY, options),
    verifyReceipt: (receipt) => authenticateReceipt(receipt, env.BAMBOO_RELEASE_SIGNING_KEY),
    assertSigningKey: () => assertSigningConfiguration(env.BAMBOO_RELEASE_SIGNING_KEY, env.BAMBOO_RELEASE_SIGNING_KEY_SHA256),
    isAncestor: async (ancestor, descendant) => gitIsAncestor(ancestor, descendant),
    tagSource: async (version, { allowMissing = false } = {}) => {
      const tag = github(repository, `git/ref/tags/v${assertVersion(version)}`, undefined, undefined, allowMissing)
      if (!tag) return null
      return tag.object?.type === 'commit' ? tag.object.sha : undefined
    },
    reserve: async (receipt) => write(() => github(repository, 'releases', {
      tag_name: `v${receipt.version}`, target_commitish: sourceRevision,
      name: `Bamboo v${receipt.version}`, draft: true,
      body: receiptBody(signReceipt(receipt, env.BAMBOO_RELEASE_SIGNING_KEY)),
    })),
    ensureTag: async (_release, receipt) => {
      const ref = `tags/v${receipt.version}`
      let tag = github(repository, `git/ref/${ref}`, undefined, undefined, true)
      // Tag creation checks repository/workflow write authority before the
      // first crates.io upload. Queued historical source must never discover
      // a release-token permission failure only after publishing all crates.
      if (!tag) tag = await write(() => github(repository, 'git/refs', { ref: `refs/${ref}`, sha: sourceRevision }))
      assert.equal(tag.object.type, 'commit', 'Reserved release tag must be a direct commit ref')
      assert.equal(tag.object.sha, sourceRevision, 'Reserved release tag points to different source')
    },
    saveReceipt: async (release, receipt) => {
      signReceipt(receipt, env.BAMBOO_RELEASE_SIGNING_KEY)
      fs.writeFileSync(receiptFile, `${JSON.stringify(receipt, null, 2)}\n`)
      await write(() => github(repository, `releases/${release.id}`, { tag_name: `v${receipt.version}`, body: receiptBody(receipt) }, 'PATCH'))
    },
    ensureFrontend: async (release, receipt, { sealed = false } = {}) => {
      const current = github(repository, `releases/${release.id}`)
      const names = ['frontend-manifest.json', 'lotus-frontend.zip']
      const assets = names.map((name) => current.assets.find((asset) => asset.name === name))
      await preserveFrontend({ receipt, stagedBytes: frontendBytes, assets, sealed,
        readAsset: async (asset) => command(['gh', 'api', `repos/${repository}/releases/assets/${asset.id}`,
          '-H', 'Accept: application/octet-stream'], { encoding: null }),
        verifyFiles: async (bytes) => verifyPreservedFrontend(stagedFrontend, bytes),
        writeFiles: async (bytes) => bytes.forEach((content, index) => fs.writeFileSync(path.join(frontendDirectory, names[index]), content)),
        saveReceipt: async () => context.saveReceipt(release, receipt),
        upload: async () => write(() => command(['gh', 'release', 'upload', `v${receipt.version}`,
          ...names.map((name) => path.join(frontendDirectory, name)), '--clobber', '--repo', repository])),
      })
    },
    registry: async (name, version) => {
      const response = await registryFetch(`https://crates.io/api/v1/crates/${name}/${version}`, true)
      return response ? (await response.json()).version : null
    },
    registryArchive: async (name, version) => (await downloadArchive(name, version)).bytes,
    package: async (name, version) => {
      command(['cargo', 'package', '--locked', '--allow-dirty', '--no-verify', '-p', name])
      const target = jsonCommand(['cargo', 'metadata', '--format-version', '1', '--no-deps']).target_directory
      return sha256(fs.readFileSync(path.join(target, 'package', `${name}-${version}.crate`)))
    },
    cargoPublish: async (name) => {
      const result = await write(() => spawnCommand(['cargo', 'publish', '--locked', '--allow-dirty', '-p', name], {
        encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
      }))
      if (result.error) throw result.error
      const output = `${result.stdout || ''}${result.stderr || ''}`
      process.stdout.write(output)
      return { status: result.status, output }
    },
    verifyArchive: async (name, receipt, checksum) => {
      const { bytes, entry } = await downloadArchive(name, receipt.version)
      verifyPackageArchive(bytes, receipt, name, checksum, entry)
    },
    wait: async (seconds) => new Promise((resolve) => setTimeout(resolve, seconds * 1000)),
    complete: async (release, receipt, makeLatest) => {
      await write(() => command(['gh', 'release', 'upload', `v${receipt.version}`, receiptFile, '--clobber', '--repo', repository]))
      return write(() => github(repository, `releases/${release.id}`, {
        tag_name: `v${receipt.version}`, draft: false, make_latest: makeLatest ? 'true' : 'false', body: receiptBody(receipt),
      }, 'PATCH'))
    },
  }
  return context
}

async function main() {
  const context = process.env.GITHUB_EVENT_NAME === 'workflow_dispatch' ? await makeFrozenSourceContext() : makeContext()
  if (process.argv[2] === 'plan') {
    const result = await plan(context)
    const version = result.receipt.version
    fs.writeFileSync(context.receiptFile, `${JSON.stringify(result.receipt, null, 2)}\n`)
    context.verifyReceipt(result.receipt)
    const identity = preparationIdentity(context, version)
    const prepared = createPreparation(result, identity, Number(process.env.BAMBOO_EXECUTABLE_CONTRACT_VERSION))
    const output = context.sourceDispatch ? sourcePreparationOutput(prepared) : JSON.stringify(prepared)
    assert.ok(Buffer.byteLength(output) <= 64 * 1024, 'Release preparation locator/legacy JSON exceeds its job-output bound')
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `version=${version}\nreceipt_file=${context.receiptFile}\nprepared_release=${output}\n`)
    console.log(`Resolved automatic main release: ${version}`)
  } else if (process.argv[2] === 'resume') {
    const prepared = preparationInput()
    const result = await resumePrepared(context, prepared, preparationIdentity(context, process.env.TARGET_VERSION),
      Number(process.env.BAMBOO_EXECUTABLE_CONTRACT_VERSION))
    fs.writeFileSync(context.receiptFile, `${JSON.stringify(result.receipt, null, 2)}\n`)
  } else if (process.argv[2] === 'publish-originals') {
    assert.ok(context.sourceDispatch); await publishOriginalCrates(context)
  } else {
    assert.equal(process.argv[2], 'publish')
    const receipt = JSON.parse(fs.readFileSync(context.receiptFile, 'utf8'))
    const release = (await context.releases()).find((entry) => entry.tag_name === `v${receipt.version}`)
    assert.ok(release, 'Reserved draft Release disappeared')
    await publish(context, release, receipt)
  }
}

function preparationIdentity(context, version, env = process.env) {
  return { ...context.identity, version, sourceTree: command(['git', 'rev-parse', 'HEAD^{tree}']).trim(),
    workflowRevision: env.GITHUB_WORKFLOW_SHA, runId: env.GITHUB_RUN_ID, runAttempt: Number(env.GITHUB_RUN_ATTEMPT),
    ciRun: context.ciRun, ...(context.validation ? { validation: context.validation } : {}),
    ...(context.sourceDispatch ? { sourceDispatch: context.sourceDispatch } : {}) }
}

module.exports = { assertVersion, assertManualVersion, validateSource, validateReceipt, nextVersion, selectVersion,
  plan, publish, makeContext, makeRecoveryContext, assertCurrentMain, currentMainWrite, sha256, githubArgs, receiptBody, readBodyReceipt,
  frontendIdentity, verifyPackageArchive, preserveFrontend, verifyPreservedFrontend, shouldMakeLatest, gitIsAncestor,
  canonicalJson, signReceipt, authenticateReceipt, readAuthenticatedReceipt, assertSigningKey, assertSigningConfiguration,
  verifyRegistrySource, highestStableVersion, spawnCommand, command, createPreparation, validatePreparation, resumePrepared,
  assertAdmissionCompatibility, nativeOriginalContract, sourceDispatchContract, readFrozenSourceContext,
  makeFrozenSourceContext, assertSourceDispatchCompatibility, validateFrontendOriginals, preserveFrontendOriginals,
  readFrozenCheckout, preparationIdentity, paginatedAssets, originalCrateInputs, publishOriginalCrates, readOriginalCrateClosure,
  SOURCE_PREPARATION_KIND, SOURCE_PREPARATION_FILE, MAX_SOURCE_PREPARATION, preparationInput, sourcePreparationOutput }
if (require.main === module) main().catch((error) => { console.error(error); process.exitCode = 1 })
