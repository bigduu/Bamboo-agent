'use strict'
// Original-output lifecycle only. Source authorization stays with the trusted
// caller; this module cannot turn the inactive frozen-source context into it.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const core = require('./crate-release.cjs')
const sidecar = require('./sidecar-release.cjs')
const TARGETS = sidecar.TARGETS
const KIND = 'bamboo-native-original/v1'
const HOST_KIND = 'bamboo-native-host-original/v1'
const PLAN_KIND = 'bamboo-native-original-plan/v1'
const WORKFLOW = '.github/workflows/publish-crate.yml'
const BUILD = 'Build and verify the standard versioned Bamboo executable'
const UPLOAD = "Transport this platform's verified executable within the same run"
const RECORD = 'Record original native transport'
const SELECT = 'Select original native output'
const PREPARE = 'Prepare and verify original native outputs'
const TRANSPORT_PREFIX = 'Native original transport: '
const PLAN_PREFIX = 'Native original plan: '
const SELECTION_PREFIX = 'Native original selection: '
const HOST_CHECKPOINT = 'bamboo-native-host-original-v1.json'
const HOST_INTENT = 'bamboo-native-host-start-v1.json'
const MAX_ZIP = sidecar.MAX_BINARY + 4 * 1024 * 1024
const MAX_NATIVE_LOG = 64 * 1024 * 1024
const DIGEST = /^[0-9a-f]{64}$/
const positive = value => Number.isSafeInteger(value) && value > 0
const clone = value => JSON.parse(JSON.stringify(value))
const digest = value => core.sha256(Buffer.from(core.canonicalJson(value)))
const jobName = target => `Complete executable (${target})`
const archiveName = target => `bamboo-native-original-${target}.zip`
const checkpointName = target => `bamboo-native-original-${target}.json`
const transportName = (target, attempt) => `bamboo-executable-${target}-attempt-${attempt}`

function binding(preparation) {
  const receipt = preparation.receipt
  // Package progress may advance, but these reserved inputs never do.
  return digest({ releaseId: preparation.releaseId, version: receipt.version,
    identity: receipt.identity, crates: receipt.crates, ciRun: receipt.ciRun,
    frontendBytes: receipt.frontendBytes, nativeOriginals: receipt.nativeOriginals,
    ...(receipt.sourceDispatch ? { sourceDispatch: receipt.sourceDispatch, frontendOriginals: receipt.frontendOriginals } : {}),
    ...(receipt.validation ? { validation: receipt.validation } : {}) })
}

function contract(preparation, identity) {
  core.validatePreparation(preparation, identity, 2)
  assert.deepEqual(preparation.receipt.nativeOriginals, core.nativeOriginalContract(identity),
    'A new authenticated original-output contract is required; historical receipts are not upgraded')
  return binding(preparation)
}

function step(job, name) {
  assert.ok(Array.isArray(job.steps), 'Missing native steps')
  const rows = job.steps.filter(value => value.name === name)
  assert.ok(rows.length <= 1, 'Duplicate native step')
  return rows[0]
}

function executionKey(job) {
  assert.equal(job.status, 'completed')
  assert.equal(job.conclusion, 'success')
  const start = Date.parse(job.started_at), end = Date.parse(job.completed_at), numbers = new Set()
  assert.ok(Number.isFinite(start) && Number.isFinite(end) && start <= end, 'Invalid native execution time')
  assert.ok(Array.isArray(job.steps) && job.steps.length, 'Missing original native steps')
  const steps = job.steps.map(value => {
    assert.ok(positive(value.number) && !numbers.has(value.number) && typeof value.name === 'string' && value.name)
    numbers.add(value.number)
    assert.equal(value.status, 'completed')
    assert.ok(['success', 'skipped', 'neutral'].includes(value.conclusion))
    if (!(value.conclusion === 'skipped' && value.started_at === null && value.completed_at === null)) {
      const first = Date.parse(value.started_at), last = Date.parse(value.completed_at)
      assert.ok(Number.isFinite(first) && Number.isFinite(last) && start <= first && first <= last && last <= end)
    }
    return { number: value.number, name: value.name, status: value.status, conclusion: value.conclusion,
      started_at: value.started_at, completed_at: value.completed_at }
  })
  return core.canonicalJson({ started_at: job.started_at, completed_at: job.completed_at, steps })
}

function history(rows, identity, target, { beforeAttempt = identity.runAttempt + 1, final = false } = {}) {
  assert.ok(TARGETS.includes(target))
  const matching = rows.filter(job => job.name === jobName(target))
  for (const job of matching) {
    assert.ok(positive(job.id) && positive(job.run_attempt) && job.run_attempt <= identity.runAttempt)
    assert.equal(job.run_id, Number(identity.runId)); assert.equal(job.head_sha, identity.workflowRevision)
  }
  const family = matching.filter(job => job.run_attempt < beforeAttempt)
  for (const job of family) assert.equal(job.status, 'completed', 'Previous native execution is still active or unknown')
  assert.equal(new Set(family.map(job => job.run_attempt)).size, family.length, 'Ambiguous same-attempt native jobs')
  const latest = [...family].sort((a, b) => b.run_attempt - a.run_attempt)[0]
  if (final) assert.ok(latest?.conclusion === 'success', 'Latest logical native job must succeed')
  const built = family.filter(job => step(job, BUILD)?.conclusion === 'success')
  const origins = new Map()
  for (const job of built) {
    const key = executionKey(job)
    assert.equal(step(job, UPLOAD)?.conclusion, 'success', 'Successful build has no original transport; never rebuild it')
    assert.equal(step(job, RECORD)?.conclusion, 'success', 'Successful build has no independently recorded transport')
    if (!origins.has(key) || job.run_attempt < origins.get(key).run_attempt) origins.set(key, job)
  }
  assert.ok(origins.size <= 1, 'A second physical native build cannot replace the original')
  for (const job of family.filter(value => !built.includes(value))) {
    const build = step(job, BUILD)
    assert.ok(!build || build.conclusion === 'skipped', 'A real failed/unknown native build cannot be hidden by retained success')
    if (job.conclusion === 'success') {
      assert.equal(step(job, SELECT)?.conclusion, 'success', 'A skipped build is not native success')
    } else {
      // Fixed controller steps only. A source/compiler failure is not an
      // infrastructure retry, and a selector failure can only revalidate bytes.
      const failures = job.steps.filter(value => !['success', 'skipped', 'neutral'].includes(value.conclusion))
      assert.ok(failures.length && failures.every(value => [SELECT, 'Set up job',
        'Setup Node.js', 'Setup Python', 'Setup Rust', 'Restore compatible native release dependencies',
        "Restore the original preparation attempt's sealed frontend bytes"].includes(value.name)),
      'Unclassified native failure cannot authorize a build')
    }
  }
  return { latest, physical: [...origins.values()][0] }
}

function logRecord(log, prefix) {
  assert.ok(typeof log === 'string' && Buffer.byteLength(log) <= MAX_NATIVE_LOG, 'Missing/oversized original log')
  const records = log.split(/\r?\n/).filter(line => /^\d{4}-\d\d-\d\dT\S+Z /.test(line) && line.slice(line.indexOf(' ') + 1).startsWith(prefix))
  assert.equal(records.length, 1, 'Missing/duplicate original output record')
  const encoded = records[0].slice(records[0].indexOf(prefix) + prefix.length)
  assert.ok(/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) && encoded.length <= 128 * 1024)
  const bytes = Buffer.from(encoded, 'base64')
  assert.equal(bytes.toString('base64'), encoded)
  const value = JSON.parse(bytes)
  assert.equal(JSON.stringify(value), bytes.toString('utf8'), 'Noncanonical original output record')
  return value
}

function emit(prefix, value, write = console.log) {
  write(prefix + Buffer.from(JSON.stringify(value)).toString('base64'))
}

function transportRecord(identity, target, receiptBytes, artifact) {
  const build = sidecar.validateBuildReceipt(JSON.parse(receiptBytes), identity, target)
  assert.equal(build.runAttempt, identity.runAttempt, 'Record the physical build attempt')
  assert.ok(positive(artifact.id)); assert.match(artifact.sha256, DIGEST)
  return { schemaVersion: 1, target, runId: identity.runId, runAttempt: identity.runAttempt,
    controllerRevision: identity.workflowRevision, receiptSha256: core.sha256(receiptBytes),
    sha256: build.sha256, sizeBytes: build.sizeBytes, artifactId: artifact.id, artifactSha256: artifact.sha256 }
}

async function refresh(context) {
  const { identity, preparation, signingKey } = context
  const reserved = contract(preparation, identity)
  assert.notEqual(preparation.receipt.validation?.kind, 'bamboo-frozen-source-context/v2',
    'Read-only frozen-source context is not publication authority')
  assert.equal(typeof context.guard, 'function', 'Publication requires its existing source freshness guard')
  await context.guard()
  const run = await context.readRun(Number(identity.runId))
  assert.equal(run.id, Number(identity.runId)); assert.equal(run.name, 'Publish Crate')
  assert.equal(run.path, WORKFLOW); assert.equal(run.head_sha, identity.workflowRevision)
  assert.equal(run.run_attempt, identity.runAttempt); assert.equal(run.status, 'in_progress')
  assert.equal(run.repository?.full_name, identity.repository)
  assert.equal(run.head_repository?.full_name, identity.repository)
  assert.equal(run.event, preparation.receipt.sourceDispatch ? 'workflow_dispatch' : 'workflow_run')
  const release = await context.readRelease(preparation.releaseId)
  assert.equal(release.id, preparation.releaseId); assert.equal(release.tag_name, `v${identity.version}`)
  assert.equal(release.target_commitish, identity.sourceRevision); assert.equal(release.prerelease, false)
  const tag = await context.readTag(identity.version)
  assert.equal(tag.object?.type, 'commit'); assert.equal(tag.object.sha, identity.sourceRevision)
  const receipt = core.readAuthenticatedReceipt(release, signingKey, { required: true })
  core.validateReceipt(receipt, preparation.receipt.identity, preparation.receipt.crates, identity.version)
  assert.equal(binding({ ...preparation, receipt }), reserved, 'Reserved native inputs changed')
  const assets = await context.listAssets(release.id)
  assert.equal(new Set(assets.map(asset => asset.name)).size, assets.length, 'Duplicate native assets')
  return { release, receipt, assets, reserved }
}

function assetIdentity(asset, expectedName, maximum = MAX_ZIP) {
  assert.ok(positive(asset?.id) && Number.isSafeInteger(asset.size) && asset.size > 0 && asset.size <= maximum)
  assert.equal(asset.name, expectedName); assert.equal(asset.state, 'uploaded')
  assert.match(asset.digest, /^sha256:[0-9a-f]{64}$/)
  return { assetId: asset.id, name: asset.name, sha256: asset.digest.slice(7), sizeBytes: asset.size }
}

async function assetFile(context, asset, name, maximum) {
  const info = assetIdentity(asset, name, maximum)
  const file = await context.readAsset(asset, maximum)
  assert.deepEqual(sidecar.fileIdentity(file, maximum), { sha256: info.sha256, sizeBytes: info.sizeBytes }, 'Stored original bytes changed')
  return { file, info }
}

async function saveAsset(context, file, name, maximum) {
  let { assets } = await refresh(context)
  const expected = sidecar.fileIdentity(file, maximum)
  let asset = assets.find(value => value.name === name)
  if (!asset) {
    try { await context.upload(file, name) } catch (error) {
      // Unknown write outcome: read back this exact name/bytes; never clobber.
      assets = (await refresh(context)).assets
      asset = assets.find(value => value.name === name)
      if (!asset) throw error
    }
    if (!asset) asset = (await refresh(context)).assets.find(value => value.name === name)
  }
  const stored = await assetFile(context, asset, name, maximum)
  assert.deepEqual({ sha256: stored.info.sha256, sizeBytes: stored.info.sizeBytes }, expected, 'Refusing original asset replacement')
  return stored.info
}

function unpack(file, target, directory) {
  fs.mkdirSync(directory)
  sidecar.execute(process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3'),
    [path.join(__dirname, 'native-originals-zip.py'), file, directory, sidecar.artifactName(target)])
  return directory
}

function verifyPayload(context, target, archive, record, directory) {
  unpack(archive, target, directory)
  const receiptFile = path.join(directory, 'build-receipt.json')
  assert.equal(sidecar.fileIdentity(receiptFile, 1024 * 1024).sha256, record.receiptSha256)
  const build = sidecar.validateBuildReceipt(JSON.parse(fs.readFileSync(receiptFile)), context.identity, target)
  assert.equal(build.runAttempt, record.runAttempt)
  assert.equal(build.sha256, record.sha256); assert.equal(build.sizeBytes, record.sizeBytes)
  assert.deepEqual(build.frontendBytes, context.preparation.receipt.frontendBytes)
  const binary = path.join(directory, sidecar.artifactName(target))
  assert.deepEqual(sidecar.fileIdentity(binary, sidecar.MAX_BINARY, 65536), { sha256: build.sha256, sizeBytes: build.sizeBytes })
  sidecar.binaryArchitecture(binary, target)
  return { build, binary }
}

async function restore(context, target, checkpoint, directory) {
  const fresh = await refresh(context)
  const stored = await assetFile(context, checkpoint, checkpointName(target), 1024 * 1024)
  const proof = core.authenticateReceipt(JSON.parse(fs.readFileSync(stored.file)), context.signingKey)
  assert.equal(proof.schemaVersion, 1); assert.equal(proof.kind, KIND); assert.equal(proof.target, target)
  assert.equal(proof.reservationSha256, fresh.reserved); assert.equal(proof.releaseId, fresh.release.id)
  assert.equal(proof.controllerRevision, context.identity.workflowRevision); assert.equal(proof.runId, context.identity.runId)
  const asset = fresh.assets.find(value => value.id === proof.archive?.assetId)
  assert.ok(asset, 'Previously successful original disappeared; never rebuild or replace it')
  const archive = await assetFile(context, asset, archiveName(target), MAX_ZIP)
  assert.deepEqual(archive.info, proof.archive, 'Original asset identity changed')
  const payload = verifyPayload(context, target, archive.file, proof.transport, directory)
  assert.deepEqual(payload.build, proof.build, 'Stored build receipt changed')
  return { proof, checkpoint: stored.info, archive: archive.info, ...payload }
}

async function retain(context, target, physical) {
  const fresh = await refresh(context)
  const previous = fresh.assets.find(value => value.name === checkpointName(target))
  if (previous) return restore(context, target, previous, context.directory(target, 'retained'))
  assert.ok(!fresh.assets.some(value => value.name === archiveName(target)),
    'Original ZIP has no sealed checkpoint; explicit recovery is required, never replace it')
  assert.ok(physical, 'Missing original physical native execution')
  const direct = await context.readJob(physical.id)
  assert.deepEqual(direct, physical, 'Original physical job changed')
  const record = logRecord(await context.readLog(physical.id), TRANSPORT_PREFIX)
  assert.equal(record.schemaVersion, 1); assert.equal(record.target, target)
  assert.equal(record.runId, context.identity.runId); assert.equal(record.runAttempt, physical.run_attempt)
  assert.equal(record.controllerRevision, context.identity.workflowRevision)
  const artifact = await context.readArtifactMetadata(record.artifactId)
  assert.equal(artifact.id, record.artifactId); assert.equal(artifact.name, transportName(target, physical.run_attempt))
  assert.equal(artifact.workflow_run?.id, Number(context.identity.runId))
  assert.equal(artifact.workflow_run.head_sha, context.identity.workflowRevision)
  assert.equal(artifact.digest, `sha256:${record.artifactSha256}`)
  assert.equal(artifact.expired, false, 'Successful original expired before preservation; never rebuild it')
  assert.ok(Number.isSafeInteger(artifact.size_in_bytes) && artifact.size_in_bytes > 0 && artifact.size_in_bytes <= MAX_ZIP)
  const zip = await context.readArtifact(artifact)
  assert.deepEqual(sidecar.fileIdentity(zip, MAX_ZIP), { sha256: record.artifactSha256, sizeBytes: artifact.size_in_bytes })
  const payload = verifyPayload(context, target, zip, record, context.directory(target, 'transport'))
  const archive = await saveAsset(context, zip, archiveName(target), MAX_ZIP)
  const proof = core.signReceipt({ schemaVersion: 1, kind: KIND, reservationSha256: fresh.reserved,
    releaseId: fresh.release.id, controllerRevision: context.identity.workflowRevision, runId: context.identity.runId,
    target, physical: { jobId: physical.id, attempt: physical.run_attempt, name: physical.name,
      startedAt: physical.started_at, completedAt: physical.completed_at, stepsSha256: digest(executionKey(physical)) },
    artifact: { id: artifact.id, name: artifact.name, sha256: record.artifactSha256, sizeBytes: artifact.size_in_bytes },
    archive, transport: record, build: payload.build }, context.signingKey)
  const file = context.file(checkpointName(target))
  fs.writeFileSync(file, `${JSON.stringify(proof, null, 2)}\n`, { flag: 'wx' })
  const checkpoint = await saveAsset(context, file, checkpointName(target), 1024 * 1024)
  return { proof, checkpoint, archive, ...payload }
}

async function host(context) {
  const fresh = await refresh(context)
  const previous = fresh.assets.find(value => value.name === HOST_CHECKPOINT)
  if (previous) {
    const stored = await assetFile(context, previous, HOST_CHECKPOINT, 1024 * 1024)
    const proof = core.authenticateReceipt(JSON.parse(fs.readFileSync(stored.file)), context.signingKey)
    assert.equal(proof.schemaVersion, 1); assert.equal(proof.kind, HOST_KIND)
    assert.equal(proof.reservationSha256, fresh.reserved); assert.equal(proof.releaseId, fresh.release.id)
    const intent = fresh.assets.find(value => value.id === proof.intent?.assetId)
    assert.ok(intent, 'Prepared host intent disappeared')
    const originalIntent = await assetFile(context, intent, HOST_INTENT, 1024 * 1024)
    assert.deepEqual(originalIntent.info, proof.intent)
    const started = core.authenticateReceipt(JSON.parse(fs.readFileSync(originalIntent.file)), context.signingKey)
    assert.equal(started.kind, `${HOST_KIND}/started`); assert.equal(started.reservationSha256, fresh.reserved)
    const asset = fresh.assets.find(value => value.id === proof.archive?.assetId)
    assert.ok(asset, 'Previously prepared browser host disappeared')
    const archive = await assetFile(context, asset, sidecar.HOST_ASSET, sidecar.MAX_HOST)
    assert.deepEqual(archive.info, proof.archive)
    return { proof, checkpoint: stored.info, file: archive.file }
  }
  assert.ok(!fresh.assets.some(value => [HOST_INTENT, sidecar.HOST_ASSET].includes(value.name)),
    'Previously started/prepared host lost its checkpoint; never repackage it')
  assert.equal(typeof context.buildHost, 'function', 'Host is packaged only by preparation')
  const started = core.signReceipt({ schemaVersion: 1, kind: `${HOST_KIND}/started`,
    reservationSha256: fresh.reserved, releaseId: fresh.release.id }, context.signingKey)
  const intentFile = context.file(HOST_INTENT)
  fs.writeFileSync(intentFile, `${JSON.stringify(started, null, 2)}\n`, { flag: 'wx' })
  const intent = await saveAsset(context, intentFile, HOST_INTENT, 1024 * 1024)
  const built = await context.buildHost()
  const archive = await saveAsset(context, built.file, sidecar.HOST_ASSET, sidecar.MAX_HOST)
  assert.equal(built.manifest.sha256, archive.sha256); assert.equal(built.manifest.sizeBytes, archive.sizeBytes)
  const proof = core.signReceipt({ schemaVersion: 1, kind: HOST_KIND, releaseId: fresh.release.id,
    reservationSha256: fresh.reserved, intent, archive, manifest: built.manifest }, context.signingKey)
  const file = context.file(HOST_CHECKPOINT)
  fs.writeFileSync(file, `${JSON.stringify(proof, null, 2)}\n`, { flag: 'wx' })
  return { proof, checkpoint: await saveAsset(context, file, HOST_CHECKPOINT, 1024 * 1024), file: built.file }
}

async function plan(context, preparationJobId) {
  assert.ok(positive(preparationJobId), 'Missing current physical preparation job')
  const current = await context.readJob(preparationJobId)
  assert.equal(current.run_id, Number(context.identity.runId)); assert.equal(current.head_sha, context.identity.workflowRevision)
  assert.equal(current.run_attempt, context.identity.runAttempt); assert.equal(current.name, 'automatic')
  assert.equal(current.status, 'in_progress'); assert.equal(step(current, PREPARE)?.status, 'in_progress')
  const fresh = await refresh(context), rows = await context.jobs()
  const targets = []
  // Authenticate EVERY retained successful target before permitting ANY build.
  for (const target of TARGETS) {
    const selected = history(rows, context.identity, target, { beforeAttempt: context.identity.runAttempt })
    const checkpoint = fresh.assets.find(value => value.name === checkpointName(target))
    if (selected.physical || checkpoint) {
      const retained = checkpoint ? await restore(context, target, checkpoint, context.directory(target, 'plan'))
        : await retain(context, target, selected.physical)
      assert.ok(selected.physical, 'Successful original has no authenticated physical job')
      assert.equal(retained.proof.physical.jobId, selected.physical.id)
      assert.equal(retained.proof.physical.attempt, selected.physical.run_attempt)
      assert.equal(retained.proof.physical.stepsSha256, digest(executionKey(selected.physical)))
      targets.push({ target, action: 'retain', checkpoint: retained.checkpoint,
        original: { jobId: selected.physical.id, attempt: selected.physical.run_attempt } })
    } else {
      assert.ok(!selected.latest || selected.latest.conclusion !== 'success', 'Successful target has no retained original')
      targets.push({ target, action: 'build' })
    }
  }
  const preparedHost = await host(context)
  await refresh(context)
  const relevant = values => values.filter(value => TARGETS.some(target => value.name === jobName(target)))
  assert.deepEqual(relevant(await context.jobs()), relevant(rows), 'Native execution history changed during preparation')
  return core.signReceipt({ schemaVersion: 1, kind: PLAN_KIND, reservationSha256: fresh.reserved,
    runId: context.identity.runId, runAttempt: context.identity.runAttempt,
    controllerRevision: context.identity.workflowRevision, preparationJobId, targets,
    ...(context.preparation.receipt.sourceDispatch ? { preparationSha256: digest(context.preparation) } : {}),
    hostCheckpoint: preparedHost.checkpoint }, context.signingKey)
}

function select(planValue, identity, preparation, target, currentJob, { selecting = true } = {}) {
  // This MUST precede cache restore, toolchain setup, stamping and Cargo. A
  // failed-only rerun with copied automatic outputs cannot reuse a stale plan.
  assert.equal(planValue?.runAttempt, identity.runAttempt, 'Native plan must belong to the current physical attempt; rerun the full workflow')
  assert.ok(TARGETS.includes(target))
  assert.equal(planValue.schemaVersion, 1); assert.equal(planValue.kind, PLAN_KIND)
  assert.equal(planValue.reservationSha256, contract(preparation, identity))
  if (preparation.receipt.sourceDispatch) assert.equal(planValue.preparationSha256, digest(preparation), 'Plan differs from the original preparation file')
  assert.equal(planValue.runId, identity.runId); assert.equal(planValue.controllerRevision, identity.workflowRevision)
  assert.ok(positive(planValue.preparationJobId))
  assert.deepEqual(planValue.targets?.map(value => value.target), TARGETS)
  assert.ok(planValue.targets.every(value => ['build', 'retain'].includes(value.action)))
  assert.equal(planValue.authentication?.schemaVersion, 1)
  assert.equal(planValue.authentication.algorithm, 'hmac-sha256'); assert.match(planValue.authentication.mac, DIGEST)
  assert.ok(positive(currentJob?.id) && currentJob.run_id === Number(identity.runId))
  assert.equal(currentJob.run_attempt, identity.runAttempt); assert.equal(currentJob.head_sha, identity.workflowRevision)
  assert.equal(currentJob.name, jobName(target)); assert.equal(currentJob.status, 'in_progress')
  assert.equal(step(currentJob, SELECT)?.status, selecting ? 'in_progress' : 'completed', 'Selector must run in the actual current native job')
  if (!selecting) assert.equal(step(currentJob, SELECT).conclusion, 'success')
  return planValue.targets.find(value => value.target === target)
}

async function selectCurrent(context, planValue, target, currentJob, options) {
  const selected = select(planValue, context.identity, context.preparation, target, currentJob, options)
  const actual = await context.readJob(currentJob.id)
  assert.deepEqual(select(planValue, context.identity, context.preparation, target, actual, options), selected)
  const job = await context.readJob(planValue.preparationJobId)
  assert.equal(job.run_id, Number(context.identity.runId)); assert.equal(job.head_sha, context.identity.workflowRevision)
  assert.equal(job.run_attempt, context.identity.runAttempt); assert.equal(job.name, 'automatic')
  executionKey(job)
  assert.equal(step(job, PREPARE)?.conclusion, 'success')
  assert.deepEqual(logRecord(await context.readLog(job.id), PLAN_PREFIX), planValue, 'Plan differs from the actual current preparation log')
  return selected
}

// Read-only worker adapter. It deliberately has no Release reader, HMAC key,
// upload method or version allocator. Draft verification belongs to preparation.
async function workerSelection(identity, preparation, planValue, target, selecting) {
  assert.equal(planValue?.runAttempt, identity.runAttempt,
    'Native plan must belong to the current physical attempt; rerun the full workflow')
  const get = route => JSON.parse(sidecar.execute('gh', ['api', `repos/${identity.repository}/${route}`]))
  const rows = [], seen = new Set(); let total
  for (let page = 1; page <= 20; page++) {
    const body = get(`actions/runs/${identity.runId}/attempts/${identity.runAttempt}/jobs?per_page=100&page=${page}`)
    assert.ok(Array.isArray(body.jobs) && Number.isSafeInteger(body.total_count) && body.total_count >= 0)
    if (total === undefined) total = body.total_count
    assert.equal(body.total_count, total)
    for (const value of body.jobs) { assert.ok(positive(value.id) && !seen.has(value.id)); seen.add(value.id); rows.push(value) }
    assert.ok(rows.length <= total)
    if (rows.length === total) break
    assert.equal(body.jobs.length, 100)
  }
  assert.equal(rows.length, total, 'Incomplete current native job inventory')
  const current = rows.filter(value => value.name === jobName(target) && value.run_attempt === identity.runAttempt && value.status === 'in_progress')
  assert.equal(current.length, 1, 'Missing/duplicate current physical native job')
  return selectCurrent({ identity, preparation,
    readJob: async id => get(`actions/jobs/${id}`),
    readLog: async id => sidecar.execute('gh', ['api', `repos/${identity.repository}/actions/jobs/${id}/logs`], { maxBuffer: MAX_NATIVE_LOG }),
  }, planValue, target, current[0], { selecting })
}

async function collect(context) {
  const rows = await context.jobs(), kept = [], errors = []
  for (const target of TARGETS) {
    try {
      const selected = history(rows, context.identity, target)
      if (selected.physical) kept.push(await retain(context, target, selected.physical))
    } catch (error) { errors.push(error) }
  }
  // Persist valid siblings even when another target failed or lost transport.
  assert.equal(errors.length, 0, 'Native retention is incomplete; successful siblings were preserved')
  return kept
}

async function restoreAll(context, outputDirectory) {
  const fresh = await refresh(context), rows = await context.jobs(), builds = [], originals = []
  if (outputDirectory) {
    if (!fs.existsSync(outputDirectory)) fs.mkdirSync(outputDirectory)
    assert.ok(fs.lstatSync(outputDirectory).isDirectory() && !fs.lstatSync(outputDirectory).isSymbolicLink())
    assert.deepEqual(fs.readdirSync(outputDirectory), [], 'Final output must be fresh')
  }
  for (const target of TARGETS) {
    const selected = history(rows, context.identity, target, { final: true })
    const checkpoint = fresh.assets.find(value => value.name === checkpointName(target))
    assert.ok(checkpoint && selected.physical, 'Finalization requires every original native output')
    const restored = await restore(context, target, checkpoint, context.directory(target, 'final'))
    assert.equal(restored.proof.physical.jobId, selected.physical.id)
    assert.equal(restored.proof.physical.stepsSha256, digest(executionKey(selected.physical)))
    if (outputDirectory) fs.copyFileSync(restored.binary, path.join(outputDirectory, sidecar.artifactName(target)), fs.constants.COPYFILE_EXCL)
    builds.push(restored.build); originals.push({ target, checkpoint: restored.checkpoint, archive: restored.archive })
  }
  // Restore only. Finalization never invokes a host archive builder/packager.
  const preparedHost = await host({ ...context, buildHost: undefined })
  if (outputDirectory) fs.copyFileSync(preparedHost.file, path.join(outputDirectory, sidecar.HOST_ASSET), fs.constants.COPYFILE_EXCL)
  await refresh(context)
  return { builds, originals, preparedHost }
}

async function assemble(context, outputDirectory, restored) {
  const { builds, originals, preparedHost } = restored || await restoreAll(context, outputDirectory)
  const manifest = sidecar.createManifest({ ...context.identity, artifacts: builds, browserHost: preparedHost.proof.manifest })
  const bytes = sidecar.manifestBytes(manifest)
  fs.writeFileSync(path.join(outputDirectory, sidecar.MANIFEST), bytes, { flag: 'wx' })
  const proof = core.signReceipt({ schemaVersion: 2, kind: 'bamboo-complete-executables/v2', ...context.identity,
    runAttempt: Math.max(...builds.map(value => value.runAttempt)), manifestSha256: core.sha256(bytes),
    manifestBytesBase64: bytes.toString('base64'), builds,
    nativeOriginals: { schemaVersion: 1, originals, hostCheckpoint: preparedHost.checkpoint } }, context.signingKey)
  fs.writeFileSync(path.join(outputDirectory, sidecar.PROVENANCE), `${JSON.stringify(proof, null, 2)}\n`, { flag: 'wx' })
  await refresh(context)
  return manifest
}

function completion(identity, release, assets, manifestBytes) {
  assert.equal(identity.repository, sidecar.REPOSITORY)
  assert.ok(positive(release.id)); assert.equal(release.tag_name, `v${identity.version}`)
  assert.equal(release.draft, false); assert.equal(release.prerelease, false)
  assert.equal(release.target_commitish, identity.sourceRevision)
  assert.equal(new Set(assets.map(value => value.name)).size, assets.length)
  const names = [...TARGETS.map(sidecar.artifactName), sidecar.HOST_ASSET, sidecar.PROVENANCE, sidecar.MANIFEST]
  const selected = names.map(name => assetIdentity(assets.find(value => value.name === name), name,
    name.startsWith('bamboo-sidecars-') || name.startsWith('bamboo-executables-') ? 1024 * 1024 : MAX_ZIP))
  assert.equal(selected.at(-1).sha256, core.sha256(manifestBytes))
  return { schemaVersion: 1, kind: 'bamboo-native-completion/v1', repository: identity.repository,
    sourceRevision: identity.sourceRevision, version: identity.version, releaseId: release.id,
    controllerRevision: identity.workflowRevision, runId: identity.runId, physicalAttempt: identity.runAttempt,
    manifestSha256: core.sha256(manifestBytes), assets: selected }
}
const WHOLE_PREFIX = 'Bamboo whole release completion: '
const WHOLE_STEP = 'Verify all original outputs and publish whole release'
const WHOLE_JOB = 'Publish and verify complete executable assets'
const WHOLE_STEPS = { automatic: ['Authorize the frozen accepted dev source', 'Reserve or resume the exact release identity', PREPARE],
  'Publish and verify the crate closure': ['Publish verified original crates'],
  'Retain original native outputs': ['Retain and verify original native outputs'],
  ...Object.fromEntries(TARGETS.map(target => [jobName(target), [SELECT]])) }
const jobView = job => core.canonicalJson([job.id, job.run_id, job.run_attempt, job.name, job.head_sha, job.head_branch,
  job.status, job.conclusion, job.started_at, job.completed_at, job.steps])
async function wholeJobs(context) {
  const latest = await context.latestJobs(), all = await context.jobs(), result = []
  for (const [name, required] of Object.entries(WHOLE_STEPS)) {
    const selected = latest.filter(job => job.name === name); assert.equal(selected.length, 1)
    const logical = selected[0], family = all.filter(job => job.name === name)
    assert.ok(family.length, 'Required whole-release job has no execution history')
    assert.equal(new Set(family.map(job => job.run_attempt)).size, family.length)
    for (const job of family) {
      assert.equal(job.run_id, Number(context.identity.runId)); assert.equal(job.head_sha, context.identity.workflowRevision)
      assert.equal(job.head_branch, 'main'); assert.ok(positive(job.id) && positive(job.run_attempt) && job.run_attempt <= context.identity.runAttempt)
    }
    const newest = family.sort((a, b) => b.run_attempt - a.run_attempt)[0]
    assert.equal(jobView(newest), jobView(logical))
    const key = executionKey(logical)
    for (const label of required) assert.equal(step(logical, label)?.conclusion, 'success', 'Required whole-release step did not pass')
    const physical = family.filter(job => job.status === 'completed' && job.conclusion === 'success' && executionKey(job) === key)
      .sort((a, b) => a.run_attempt - b.run_attempt)[0]
    assert.equal(jobView(await context.readJob(physical.id)), jobView(physical))
    result.push({ name, logical: { id: logical.id, attempt: logical.run_attempt },
      physical: { id: physical.id, attempt: physical.run_attempt }, stepsSha256: core.sha256(Buffer.from(key)) })
  }
  return result
}
function wholeCompletion({ context, fresh, restored, manifestBytes, crateClosure, receiptAsset, jobs, run, finalizer }) {
  const { identity } = context, marker = fresh.receipt.sourceDispatch
  assert.ok(marker); assert.equal(fresh.receipt.completed, true)
  assert.equal(crateClosure.kind, 'bamboo-verified-crate-closure/v1'); assert.equal(crateClosure.publicationAuthorized, false)
  assert.deepEqual(crateClosure.crates.map(value => value.name), fresh.receipt.crates)
  assert.deepEqual(jobs.map(value => value.name), Object.keys(WHOLE_STEPS))
  assert.deepEqual(restored.originals.map(value => value.target), TARGETS)
  for (const value of crateClosure.crates) assert.equal(value.registryChecksum, value.archive.sha256)
  const native = completion(identity, fresh.release, fresh.assets, manifestBytes)
  assert.ok(positive(finalizer.id)); assert.equal(finalizer.name, WHOLE_JOB); assert.equal(finalizer.status, 'in_progress')
  assert.equal(finalizer.run_attempt, identity.runAttempt); assert.equal(finalizer.head_sha, identity.workflowRevision)
  assert.equal(finalizer.run_id, Number(identity.runId)); assert.equal(step(finalizer, WHOLE_STEP)?.status, 'in_progress')
  for (const key of ['workflow_id', 'check_suite_id']) assert.ok(positive(run[key]))
  assert.ok(typeof run.check_suite_node_id === 'string' && run.check_suite_node_id)
  const frame = { schemaVersion: 1, kind: 'bamboo-whole-release-completion/v1', publicationAuthorized: false,
    repository: { id: 1164779771, fullName: identity.repository },
    source: { revision: identity.sourceRevision, tree: identity.sourceTree, releaseRef: marker.context.source.releaseRef,
      featurePr: marker.context.feature.pr, featureRunId: marker.context.feature.runId, sourceDispatchSha256: digest(marker) },
    controller: { revision: identity.workflowRevision, workflowPath: WORKFLOW, workflowId: run.workflow_id,
      runId: Number(identity.runId), attempt: identity.runAttempt, checkSuiteId: run.check_suite_id,
      checkSuiteNodeId: run.check_suite_node_id, finalizerJobId: finalizer.id },
    release: { id: fresh.release.id, tag: fresh.release.tag_name, version: identity.version,
      targetCommit: identity.sourceRevision, receiptAsset, receiptSha256: digest(fresh.receipt) },
    frontend: fresh.receipt.frontendOriginals.assets,
    crates: crateClosure.crates.map(value => ({ name: value.name, archiveSha256: value.archive.sha256,
      zipAssetRef: { assetId: value.asset.id, name: value.asset.name, sha256: value.asset.digest.slice(7), sizeBytes: value.asset.size },
      origin: { jobId: value.origin.jobId, attempt: value.origin.attempt },
      verificationSha256: digest(value.verification), registryChecksum: value.registryChecksum })),
    native: { manifestSha256: native.manifestSha256, assets: native.assets,
      originals: restored.originals.map(value => ({ target: value.target, zipAssetRef: value.archive, checkpointRef: value.checkpoint })),
      hostCheckpointRef: restored.preparedHost.checkpoint }, jobs }
  const bytes = Buffer.from(core.canonicalJson(frame)); assert.ok(bytes.length <= 64 * 1024, 'Whole completion frame is oversized')
  return { frame, line: WHOLE_PREFIX + bytes.toString('base64') }
}

function createContext(identity, preparation, env = process.env) {
  assert.equal(preparation.receipt.sourceDispatch, undefined, 'Source writers require the real Source factory')
  return nativeContext(identity, preparation, env, () => core.assertCurrentMain(identity.repository, identity.sourceRevision))
}
async function createFrozenContext(preparation) {
  const authority = await core.makeFrozenSourceContext(), identity = core.preparationIdentity(authority, process.env.TARGET_VERSION)
  core.authenticateReceipt(preparation.receipt, process.env.BAMBOO_RELEASE_SIGNING_KEY)
  delete identity.frontend
  contract(preparation, identity)
  const context = nativeContext(identity, preparation, process.env, authority.assertSourceCurrent)
  context.authority = authority; return context
}
function nativeContext(identity, preparation, env, guard) {
  assert.equal(env.GITHUB_REPOSITORY, sidecar.REPOSITORY)
  assert.equal(env.GITHUB_SHA, identity.workflowRevision, 'Actual controller SHA must not be relabeled as product source')
  assert.equal(env.GITHUB_WORKFLOW_SHA, identity.workflowRevision); assert.equal(env.GITHUB_RUN_ID, identity.runId)
  assert.equal(Number(env.GITHUB_RUN_ATTEMPT), identity.runAttempt)
  core.assertSigningConfiguration(env.BAMBOO_RELEASE_SIGNING_KEY, env.BAMBOO_RELEASE_SIGNING_KEY_SHA256)
  contract(preparation, identity)
  const root = fs.mkdtempSync(path.join(path.resolve(env.RUNNER_TEMP), 'bamboo-native-originals-'))
  let sequence = 0
  const get = route => core.github(identity.repository, route)
  const paged = (route, key) => {
    const rows = [], seen = new Set(); let total
    for (let page = 1; page <= 20; page++) {
      const body = get(`${route}${route.includes('?') ? '&' : '?'}per_page=100&page=${page}`)
      const values = key ? body[key] : body
      assert.ok(Array.isArray(values) && values.length <= 100)
      if (key) { if (total === undefined) total = body.total_count; assert.equal(body.total_count, total); assert.ok(Number.isSafeInteger(total) && total >= 0) }
      for (const value of values) { assert.ok(positive(value.id) && !seen.has(value.id)); seen.add(value.id); rows.push(value) }
      if (key) { assert.ok(rows.length <= total); if (rows.length === total) return rows }
      else if (values.length < 100) return rows
      assert.equal(values.length, 100)
    }
    assert.fail('Original output API pagination is incomplete')
  }
  const context = { identity, preparation, signingKey: env.BAMBOO_RELEASE_SIGNING_KEY,
    file: name => { assert.equal(path.basename(name), name); return path.join(root, `${++sequence}-${name}`) },
    directory: (target, phase) => path.join(root, `${++sequence}-${phase}-${target}`),
    guard,
    readRun: id => get(`actions/runs/${id}`), readJob: id => get(`actions/jobs/${id}`),
    jobs: () => paged(`actions/runs/${identity.runId}/jobs?filter=all`, 'jobs'),
    latestJobs: () => paged(`actions/runs/${identity.runId}/jobs?filter=latest`, 'jobs'),
    readLog: id => sidecar.execute('gh', ['api', `repos/${identity.repository}/actions/jobs/${id}/logs`], { maxBuffer: MAX_NATIVE_LOG }),
    readRelease: id => get(`releases/${id}`), readTag: version => get(`git/ref/tags/v${core.assertVersion(version)}`),
    listAssets: id => paged(`releases/${id}/assets`),
    readArtifactMetadata: id => get(`actions/artifacts/${id}`),
    readArtifact: artifact => {
      const file = context.file('original.zip')
      fs.writeFileSync(file, sidecar.execute('gh', ['api', `repos/${identity.repository}/actions/artifacts/${artifact.id}/zip`],
        { encoding: null, maxBuffer: MAX_ZIP }), { flag: 'wx' }); return file
    },
    readAsset: (asset, maximum) => {
      const file = context.file('asset')
      fs.writeFileSync(file, sidecar.assetBytes(asset, maximum), { flag: 'wx' }); return file
    },
    upload: async (file, name) => {
      await context.guard()
      const directory = path.join(root, `upload-${++sequence}`); fs.mkdirSync(directory)
      const exactName = path.join(directory, name); fs.copyFileSync(file, exactName, fs.constants.COPYFILE_EXCL)
      sidecar.execute('gh', ['release', 'upload', `v${identity.version}`, exactName, '--repo', identity.repository])
    },
    buildHost: () => {
      const directory = path.join(root, `host-${++sequence}`); fs.mkdirSync(directory)
      const manifest = sidecar.createBrowserHost({ sourceRoot: process.cwd(), outputDirectory: directory })
      return { file: path.join(directory, sidecar.HOST_ASSET), manifest }
    } }
  return context
}

function preparedIdentity(preparation, env = process.env) {
  assert.equal(env.GITHUB_SHA, env.GITHUB_WORKFLOW_SHA, 'Actual controller SHA must not be relabeled as product source')
  const identity = { repository: env.GITHUB_REPOSITORY, version: core.assertVersion(env.TARGET_VERSION),
    sourceRevision: env.SOURCE_REVISION, sourceTree: sidecar.git(['rev-parse', 'HEAD^{tree}']),
    workflowRevision: env.GITHUB_WORKFLOW_SHA, runId: env.GITHUB_RUN_ID,
    runAttempt: Number(env.GITHUB_RUN_ATTEMPT), ciRun: preparation.receipt.ciRun,
    ...(env.SOURCE_VALIDATION ? { validation: JSON.parse(env.SOURCE_VALIDATION) } : {}) }
  assert.equal(sidecar.git(['rev-parse', 'HEAD']), identity.sourceRevision)
  // Deliberately the existing source authority, never a boolean or fixture
  // producer map. Frozen-source v2 dispatch cannot enter this staged lifecycle.
  core.validateSource({ eventName: env.GITHUB_EVENT_NAME,
    event: JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH)), repository: identity.repository,
    sourceRevision: identity.sourceRevision, sourceTree: identity.sourceTree, validation: identity.validation })
  contract(preparation, identity)
  return identity
}

async function main() {
  const command = process.argv[2]
  assert.ok(['prepare', 'collect'].includes(command))
  const preparation = core.preparationInput()
  const context = preparation.receipt.sourceDispatch ? await createFrozenContext(preparation) :
    createContext(preparedIdentity(preparation), preparation)
  const identity = context.identity
  if (command === 'prepare') {
    assert.equal(process.env.GITHUB_JOB, 'automatic')
    require('./sidecar-build.cjs').sourceState(new Set([
      'crates/app/bamboo-server/frontend_package/frontend-manifest.json',
      'crates/app/bamboo-server/frontend_package/lotus-frontend.zip']))
    const active = (await context.jobs()).filter(job => job.name === 'automatic' && job.status === 'in_progress' && job.run_attempt === identity.runAttempt)
    assert.equal(active.length, 1)
    const value = await plan(context, active[0].id)
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `native_original_plan=${JSON.stringify(value)}\n`)
    emit(PLAN_PREFIX, value)
  } else {
    assert.equal(process.env.GITHUB_JOB, 'retain-native-originals')
    await collect(context)
  }
}

module.exports = { TARGETS, KIND, PLAN_KIND, HOST_CHECKPOINT, HOST_INTENT, BUILD, UPLOAD, RECORD, SELECT, PREPARE,
  TRANSPORT_PREFIX, PLAN_PREFIX, SELECTION_PREFIX, MAX_ZIP, jobName, archiveName, checkpointName, transportName,
  binding, contract, executionKey, history, logRecord, emit, transportRecord, refresh, unpack,
  retain, restore, host, plan, select, selectCurrent, workerSelection, collect, restoreAll, assemble, completion, createContext, createFrozenContext, preparedIdentity,
  saveAsset, wholeJobs, wholeCompletion, WHOLE_PREFIX, WHOLE_STEP, WHOLE_JOB, WHOLE_STEPS }
if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1 })
