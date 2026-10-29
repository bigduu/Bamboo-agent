#!/usr/bin/env node
// #2371 only: recover retained bytes and the proven-unstarted tail of one run.
// Never allocate, finalize, change publisherRunId, or rebuild native artifacts.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const core = require('./crate-release.cjs')
const source = require('./release-source.cjs')

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
const clone = value => JSON.parse(JSON.stringify(value))
const digest = value => core.sha256(Buffer.from(core.canonicalJson(value)))
const SHA = /^[0-9a-f]{40}$/
const HEX = /^[0-9a-f]{64}$/
const id = /^[1-9][0-9]*$/
const api = route => JSON.parse(core.command(['gh', 'api', '--hostname', 'github.com',
  'repos/' + TARGET.repository + '/' + route], { timeout: 30000, maxBuffer: 8 * 1024 * 1024 }))
const binary = route => core.command(['gh', 'api', '--hostname', 'github.com', '--allow-escape-sequences',
  'repos/' + TARGET.repository + '/' + route], { encoding: null,
  timeout: /^actions\/artifacts\/[0-9]+\/zip$/.test(route) ? 120000 : 30000, maxBuffer: 64 * 1024 * 1024 })

function request(env) {
  assert.equal(env.GITHUB_REPOSITORY, TARGET.repository)
  assert.equal(env.GITHUB_EVENT_NAME, 'workflow_dispatch')
  assert.equal(env.GITHUB_REF, 'refs/heads/dev'); assert.equal(env.GITHUB_REF_TYPE, 'branch')
  assert.equal(env.GITHUB_REF_PROTECTED, 'true')
  assert.match(env.EXPECTED_CONTROLLER_SHA || '', SHA)
  assert.equal(env.GITHUB_SHA, env.EXPECTED_CONTROLLER_SHA)
  assert.equal(env.GITHUB_WORKFLOW_SHA, env.EXPECTED_CONTROLLER_SHA)
  assert.equal(env.GITHUB_WORKFLOW_REF, TARGET.repository + '/.github/workflows/recover-frozen-crate-release.yml@refs/heads/dev')
  assert.match(env.GITHUB_RUN_ID || '', id); assert.match(env.GITHUB_RUN_ATTEMPT || '', id)
  assert.notEqual(env.GITHUB_RUN_ID, String(TARGET.runId), 'Keep the real recovery execution identity')
  return { runId: env.GITHUB_RUN_ID, runAttempt: Number(env.GITHUB_RUN_ATTEMPT), controllerRevision: env.GITHUB_WORKFLOW_SHA }
}

function originalEvidence(read = api, now = Date.now()) {
  const run = read('actions/runs/' + TARGET.runId)
  for (const [key, value] of Object.entries({ id: TARGET.runId, run_attempt: 1, workflow_id: TARGET.workflowId,
    check_suite_id: TARGET.suiteId, name: 'Publish Crate', path: '.github/workflows/publish-crate.yml',
    event: 'workflow_dispatch', head_branch: 'dev', head_sha: TARGET.source, status: 'completed', conclusion: 'failure',
    html_url: 'https://github.com/' + TARGET.repository + '/actions/runs/' + TARGET.runId })) assert.equal(run[key], value, 'Original publisher changed: ' + key)
  for (const repo of ['repository', 'head_repository']) assert.equal(run[repo].full_name, TARGET.repository)
  const suite = read('check-suites/' + TARGET.suiteId)
  assert.equal(suite.app.id, 15368); assert.equal(suite.head_sha, TARGET.source)
  assert.equal(suite.status, 'completed'); assert.equal(suite.conclusion, 'failure')
  assert.equal(read('actions/workflows/publish-crate.yml').id, TARGET.workflowId)
  const data = read('actions/runs/' + TARGET.runId + '/attempts/1/jobs?per_page=100')
  assert.equal(data.total_count, JOBS.length); assert.equal(data.jobs.length, JOBS.length)
  for (const [jobId, name, conclusion] of JOBS) {
    const matches = data.jobs.filter(job => job.id === jobId && job.name === name)
    assert.equal(matches.length, 1, 'Missing or ambiguous original job')
    const [job] = matches
    for (const [key, value] of Object.entries({ run_id: TARGET.runId, run_attempt: 1, head_sha: TARGET.source,
      status: 'completed', conclusion })) assert.equal(job[key], value, 'Original job changed: ' + key)
    assert.ok(Array.isArray(job.steps))
    if (conclusion === 'skipped') assert.equal(job.steps.length, 0)
    if (jobId === TARGET.jobId) {
      const publish = job.steps.filter(step => step.name === 'Publish verified crates and complete the GitHub Release')
      assert.equal(publish.length, 1); assert.equal(publish[0].number, 11)
      assert.equal(publish[0].status, 'completed'); assert.equal(publish[0].conclusion, 'failure')
      for (let number = 1; number <= 10; number++) {
        const step = job.steps.filter(entry => entry.number === number)
        assert.equal(step.length, 1); assert.equal(step[0].status, 'completed'); assert.equal(step[0].conclusion, 'success')
      }
    }
  }
  const artifacts = read('actions/runs/' + TARGET.runId + '/artifacts?per_page=100')
  assert.equal(artifacts.total_count, ARTIFACTS.length); assert.equal(artifacts.artifacts.length, ARTIFACTS.length)
  for (const [artifactId, name, size, checksum] of ARTIFACTS) {
    const found = artifacts.artifacts.filter(item => item.id === artifactId && item.name === name)
    assert.equal(found.length, 1, 'Original artifact missing or ambiguous')
    const [artifact] = found
    assert.equal(artifact.size_in_bytes, size); assert.equal(artifact.digest, 'sha256:' + checksum)
    assert.equal(artifact.expired, false); assert.ok(Date.parse(artifact.expires_at) > now, 'Original artifact expired')
    for (const [key, value] of Object.entries({ id: TARGET.runId, head_sha: TARGET.source, head_branch: 'dev', repository_id: 1164779771, head_repository_id: 1164779771 })) {
      assert.equal(artifact.workflow_run[key], value, 'Original artifact provenance changed: ' + key)
    }
  }
  return run
}

function authorize(env = process.env, read = api, log = binary) {
  request(env)
  const validation = source.authorizeFrozenRecovery(env, read, jobId => log('actions/jobs/' + jobId + '/logs').toString('utf8'))
  assert.equal(validation.sourceRevision, TARGET.source); assert.equal(validation.sourceTree, TARGET.tree)
  originalEvidence(read)
  const bytes = log('actions/jobs/' + TARGET.jobId + '/logs')
  // Never interpolate original log text in an assertion or error.
  assert.ok(Buffer.isBuffer(bytes) && bytes.length === TARGET.logBytes && core.sha256(bytes) === TARGET.logSha256,
    'Original completed failure log is missing or changed; failure cause remains transport/response-unknown')
  return validation
}

function baseline(receipt) {
  const original = clone(receipt)
  delete original.authentication; delete original.recovery2371
  for (const name of TAIL) delete original.packageChecksums[name]
  assert.equal(digest(original), TARGET.baselineSha256, 'Original signed release identity or first 27 checksums changed')
  assert.equal(digest(original.packageChecksums), TARGET.checksumsSha256)
  core.validateReceipt(receipt, original.identity, original.crates, TARGET.version)
  assert.equal(receipt.publisherRunId, String(TARGET.runId)); assert.equal(receipt.completed, false)
  return original
}
function audit(record) {
  assert.deepEqual(Object.keys(record).sort(), ['controllerRevision', 'runAttempt', 'runId'])
  assert.match(record.runId, id); assert.notEqual(record.runId, String(TARGET.runId))
  assert.ok(Number.isSafeInteger(record.runAttempt) && record.runAttempt > 0)
  assert.match(record.controllerRevision, SHA)
}
function stateOf(receipt, allowPending = false) {
  baseline(receipt)
  const state = receipt.recovery2371
  const tail = TAIL.filter(name => Object.hasOwn(receipt.packageChecksums, name))
  if (!state) { assert.equal(tail.length, 0, 'Unattested tail checksum'); return null }
  assert.deepEqual(Object.keys(state).sort(), ['completed', 'logSha256', 'originalJobId', 'originalRunId',
    'schemaVersion', 'sealed', 'startedBy', ...(state.pending ? ['pending'] : []), ...(state.completedBy ? ['completedBy'] : [])].sort())
  assert.equal(state.schemaVersion, 1); assert.equal(state.originalRunId, String(TARGET.runId))
  assert.equal(state.originalJobId, TARGET.jobId); assert.equal(state.logSha256, TARGET.logSha256)
  assert.equal(typeof state.completed, 'boolean'); audit(state.startedBy)
  assert.ok(Array.isArray(state.sealed) && state.sealed.length <= TAIL.length)
  assert.deepEqual(tail, TAIL.slice(0, state.sealed.length), 'Tail progress must be a contiguous signed prefix')
  state.sealed.forEach((entry, index) => {
    assert.deepEqual(Object.keys(entry).sort(), ['checksum', 'name', 'producer'])
    assert.equal(entry.name, TAIL[index]); assert.match(entry.checksum, HEX); audit(entry.producer)
    assert.equal(receipt.packageChecksums[entry.name], entry.checksum)
  })
  if (state.pending) {
    assert.equal(state.completed, false); assert.equal(allowPending, true, 'Ambiguous in-progress compilation; never rebuild')
    assert.deepEqual(Object.keys(state.pending).sort(), ['name', 'producer'])
    assert.equal(state.pending.name, TAIL[state.sealed.length]); audit(state.pending.producer)
  }
  if (state.completed) {
    assert.equal(state.pending, undefined); assert.equal(state.sealed.length, TAIL.length); audit(state.completedBy)
  } else assert.equal(state.completedBy, undefined)
  return state
}
function validateReservation(release, receipt, { allowPending = false, pendingOriginals = new Map() } = {}) {
  assert.equal(release.id, TARGET.releaseId); assert.equal(release.tag_name, 'v' + TARGET.version)
  assert.equal(release.target_commitish, TARGET.source); assert.equal(release.draft, true); assert.equal(release.prerelease, false)
  const original = baseline(receipt), state = stateOf(receipt, allowPending)
  const expected = { ...receipt.packageChecksums }
  const crateAssets = release.assets.filter(asset => asset.name.endsWith('.crate'))
  if (state?.pending && pendingOriginals.has(state.pending.name)) {
    const filename = state.pending.name + '-' + TARGET.version + '.crate'
    if (crateAssets.some(asset => asset.name === filename)) expected[state.pending.name] = pendingOriginals.get(state.pending.name)
  }
  assert.equal(crateAssets.length, Object.keys(expected).length, 'Missing or unrecorded retained original; do not rebuild')
  for (const [name, checksum] of Object.entries(expected)) {
    const found = crateAssets.filter(asset => asset.name === name + '-' + TARGET.version + '.crate')
    assert.equal(found.length, 1); assert.equal(found[0].state, 'uploaded')
    assert.equal(found[0].digest, 'sha256:' + checksum, 'Retained original checksum changed')
    assert.ok(Number.isSafeInteger(found[0].size) && found[0].size > 0 && found[0].size <= 64 * 1024 * 1024)
  }
  for (const [name, checksum] of [['frontend-manifest.json', original.frontendBytes.manifestSha256], ['lotus-frontend.zip', original.frontendBytes.archiveSha256]]) {
    const found = release.assets.filter(asset => asset.name === name)
    assert.equal(found.length, 1); assert.equal(found[0].state, 'uploaded'); assert.equal(found[0].digest, 'sha256:' + checksum)
  }
  return receipt
}

function newState(producer) {
  audit(producer)
  return { schemaVersion: 1, originalRunId: String(TARGET.runId), originalJobId: TARGET.jobId,
    logSha256: TARGET.logSha256, startedBy: clone(producer), sealed: [], completed: false }
}
async function beginCompilation(receipt, name, producer, save) {
  const current = stateOf(receipt)
  assert.ok(TAIL.includes(name), 'Only the never-started tail may compile')
  assert.equal(name, TAIL[current?.sealed.length || 0]); assert.equal(receipt.packageChecksums[name], undefined)
  receipt.recovery2371 ||= newState(producer)
  assert.equal(receipt.recovery2371.completed, false)
  receipt.recovery2371.pending = { name, producer: clone(producer) }
  await save(receipt) // Signed write AND authenticated readback precede Cargo.
}
function sealProgress(receipt, producer) {
  baseline(receipt)
  receipt.recovery2371 ||= newState(producer)
  const state = receipt.recovery2371, pending = state.pending
  if (pending && receipt.packageChecksums[pending.name]) {
    assert.deepEqual(pending.producer, producer)
    state.sealed.push({ name: pending.name, checksum: receipt.packageChecksums[pending.name], producer: clone(producer) })
    delete state.pending
  }
  stateOf(receipt, true)
}

function makeAuthority(env = process.env, read = api, download = binary) {
  core.assertSigningConfiguration(env.BAMBOO_RELEASE_SIGNING_KEY, env.BAMBOO_RELEASE_SIGNING_KEY_SHA256)
  const producer = request(env), validation = authorize(env, read, download), pendingOriginals = new Map()
  const readSigned = () => {
    const release = read('releases/' + TARGET.releaseId)
    const receipt = core.readAuthenticatedReceipt(release, env.BAMBOO_RELEASE_SIGNING_KEY, { required: true })
    validateReservation(release, receipt, { allowPending: !!activePending, pendingOriginals })
    return { release, receipt }
  }
  let activePending = false
  let current = readSigned()
  assert.deepEqual(current.receipt.validation, validation)
  const receipt = clone(current.receipt)
  let uploadFailure = 'generic'
  const authority = { receipt, release: current.release,
    guard() {
      source.frozenRecoveryGuard(validation, env, read); originalEvidence(read)
      const tag = read('git/ref/tags/v' + TARGET.version)
      assert.equal(tag.ref, 'refs/tags/v' + TARGET.version); assert.equal(tag.object.type, 'commit'); assert.equal(tag.object.sha, TARGET.source)
      const fresh = readSigned()
      assert.deepEqual(fresh.receipt, current.receipt, 'Signed reservation advanced; stop rather than overwrite')
    },
    async beforeCompile(name, reservation, context) {
      assert.deepEqual(reservation, current.receipt)
      assert.equal(await context.registry(name, TARGET.version), null, 'Unreserved registry package appeared; never rebuild')
      await beginCompilation(receipt, name, producer, async () => {
        activePending = true
        await context.saveReceipt(current.release, receipt)
      })
      assert.deepEqual(current.receipt.recovery2371.pending, { name, producer })
      authority.guard()
      assert.equal(await context.registry(name, TARGET.version), null, 'Registry advanced after the write-ahead marker')
    },
    prepareSave(value) { assert.equal(value, receipt); sealProgress(value, producer) },
    readback(expected) {
      const fresh = readSigned()
      assert.deepEqual(fresh.receipt, expected, 'Receipt write readback is unknown; no further compilation or upload')
      current = fresh
      activePending = !!fresh.receipt.recovery2371?.pending
    },
    verifyOriginal(name, bytes, file) {
      const checksum = core.sha256(bytes)
      core.verifyPackageArchive(bytes, receipt, name, checksum, entry => core.command(['tar', '-xOf', file,
        name + '-' + TARGET.version + '/' + entry], { encoding: null }))
      if (TAIL.includes(name) && !receipt.packageChecksums[name]) {
        assert.equal(receipt.recovery2371?.pending?.name, name)
        pendingOriginals.set(name, checksum)
      }
    },
    async uploadFetch(...args) {
      uploadFailure = 'invalid-response'
      try { return await fetch(...args) } catch (error) {
        uploadFailure = error?.name === 'TimeoutError' || error?.name === 'AbortError' ? 'timeout'
          : error instanceof TypeError ? 'network' : 'generic'
        throw new Error('Recovery upload failed')
      }
    },
    uploadResult(result) {
      return safeUploadResult(result, uploadFailure)
    },
    finish() {
      assert.equal(stateOf(receipt)?.sealed.length, TAIL.length)
      receipt.recovery2371.completed = true
      receipt.recovery2371.completedBy = clone(producer)
    },
  }
  authority.guard()
  return authority
}
function safeUploadResult(result, category) {
  if (result.status !== 0 && result.output === 'crates.io upload transport or response validation failed') {
    const safe = ['timeout', 'network', 'invalid-response'].includes(category) ? category : 'generic'
    return { status: 1, output: 'Recovery crates.io upload failed (' + safe + ')' }
  }
  return result
}
function verifyRegistryMetadata(receipt, name, version, entry) {
  if (version !== TARGET.version) return entry
  if (receipt.crates.slice(0, 26).includes(name)) assert.ok(entry, 'Original published prefix disappeared; do not republish')
  if (entry) {
    assert.equal(entry.yanked, false, 'Registry package must be explicitly unyanked')
    const expected = receipt.packageChecksums[name]
    assert.ok(expected, 'Registry tail has no reserved package checksum; stop')
    assert.equal(entry.checksum, expected, 'Registry checksum changed')
  }
  return entry
}

const ZIP_READER = String.raw`
import io,json,stat,sys,zipfile
data=sys.stdin.buffer.read(8*1024*1024+1)
assert 0<len(data)<=8*1024*1024
with zipfile.ZipFile(io.BytesIO(data)) as archive:
    files=archive.infolist()
    assert sorted(f.filename for f in files)==['frontend-manifest.json','lotus-frontend.zip']
    result={}
    for f in files:
        assert not f.is_dir() and not f.flag_bits&1 and f.file_size<=8*1024*1024
        assert stat.S_IFMT(f.external_attr>>16) in (0,stat.S_IFREG)
        result[f.filename]=archive.read(f).hex()
    print(json.dumps(result))
`
function restoredFrontend(archive, receipt) {
  assert.ok(Buffer.isBuffer(archive) && archive.length === ARTIFACTS[0][2]
    && core.sha256(archive) === ARTIFACTS[0][3], 'Original frontend artifact bytes changed')
  const parsed = JSON.parse(core.command(['python3', '-I', '-c', ZIP_READER], { input: archive, maxBuffer: 20 * 1024 * 1024 }))
  const files = Object.fromEntries(Object.entries(parsed).map(([name, hex]) => [name, Buffer.from(hex, 'hex')]))
  return verifyFrontendFiles(files, receipt)
}
function verifyFrontendFiles(files, receipt) {
  assert.deepEqual(Object.keys(files).sort(), ['frontend-manifest.json', 'lotus-frontend.zip'])
  assert.ok(Object.values(files).every(Buffer.isBuffer))
  assert.equal(core.sha256(files['frontend-manifest.json']), receipt.frontendBytes.manifestSha256)
  assert.equal(core.sha256(files['lotus-frontend.zip']), receipt.frontendBytes.archiveSha256)
  return files
}
function restore(env = process.env) {
  const authority = makeAuthority(env)
  assert.equal(core.command(['git', 'rev-parse', 'HEAD']).trim(), TARGET.source)
  const files = restoredFrontend(binary('actions/artifacts/' + ARTIFACTS[0][0] + '/zip'), authority.receipt)
  authority.guard()
  let directory = process.cwd()
  for (const part of ['crates', 'app', 'bamboo-server', 'frontend_package']) {
    directory = path.join(directory, part)
    assert.ok(fs.lstatSync(directory).isDirectory() && !fs.lstatSync(directory).isSymbolicLink(), 'Unsafe frontend directory')
  }
  for (const [name, bytes] of Object.entries(files)) {
    const destination = path.join(directory, name), stat = fs.lstatSync(destination, { throwIfNoEntry: false })
    assert.ok(!stat || stat.isFile() && !stat.isSymbolicLink(), 'Unsafe frontend file')
    fs.writeFileSync(destination, bytes)
  }
  console.log('Restored only the original publisher attempt-1 frontend manifest and zip')
}

async function recoveryPublish(context, authority) {
  // No reserve/plan/finalize path: old publisher owns completion and native reuse.
  const registry = context.registry.bind(context)
  context.registry = async (name, version) => verifyRegistryMetadata(authority.receipt, name, version, await registry(name, version))
  await core.publishCrates(context, authority.release, authority.receipt)
  if (!stateOf(authority.receipt)?.completed) {
    authority.finish()
    await context.saveReceipt(authority.release, authority.receipt)
  }
  authority.guard() // Authenticated final remote readback, not a local success flag.
  assert.equal(stateOf(authority.receipt).completed, true)
  assert.equal(authority.receipt.recovery2371.pending, undefined)
  assert.equal(authority.receipt.completed, false)
  assert.equal(authority.receipt.publisherRunId, String(TARGET.runId))
  console.log('Recovery sealed and read back all 30 crates; original publisher run 40613036533 alone must finalize')
}
async function main() {
  if (process.argv[2] === 'authorize') {
    authorize()
    assert.equal(core.command(['git', 'rev-parse', 'HEAD']).trim(), TARGET.source)
    assert.equal(core.command(['git', 'status', '--porcelain', '--untracked-files=all']), '')
    console.log('Authorized the fixed failed publisher, exact recovery controller and original feature acceptance')
  } else if (process.argv[2] === 'restore') restore()
  else {
    assert.equal(process.argv[2], 'publish')
    const { context, recovery } = core.makeFrozenRecoveryContext()
    const result = context.cargoPublish
    context.cargoPublish = async name => {
      const classified = recovery.uploadResult(await result(name))
      if (/^Recovery crates\.io upload failed \((?:timeout|network|invalid-response|generic)\)$/.test(classified.output)) console.error(classified.output)
      return classified
    }
    await recoveryPublish(context, recovery)
  }
}
module.exports = { TARGET, TAIL, JOBS, ARTIFACTS, request, originalEvidence, authorize, baseline, stateOf,
  validateReservation, newState, beginCompilation, sealProgress, makeAuthority, restoredFrontend, verifyFrontendFiles,
  safeUploadResult, verifyRegistryMetadata, restore, recoveryPublish }
if (require.main === module) main().catch(() => { console.error('Frozen recovery stopped; preserve originals and signed reservation for diagnosis'); process.exitCode = 1 })
