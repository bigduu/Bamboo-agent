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

function validateSource({ eventName, event, repository, sourceRevision, workflowRevision }) {
  assert.match(sourceRevision, SHA)
  if (eventName === 'workflow_dispatch') {
    assert.equal(sourceRevision, workflowRevision, 'Manual release must use its exact dispatched commit')
    return false
  }
  assert.equal(eventName, 'workflow_run', 'Unsupported release trigger')
  const run = event.workflow_run
  assert.equal(run?.name, 'CI')
  assert.equal(run?.event, 'push')
  assert.equal(run?.conclusion, 'success')
  assert.equal(run?.head_branch, 'main')
  assert.equal(run?.head_repository?.full_name, repository)
  assert.equal(run?.repository?.full_name, repository)
  assert.equal(sourceRevision, run?.head_sha, 'Release source must equal the successful main CI commit')
  return true
}

function validateReceipt(receipt, identity, crates, version = receipt?.version) {
  assert.equal(receipt?.schemaVersion, 1, 'Missing or unsupported release provenance')
  assert.equal(receipt.version, assertVersion(version))
  assert.deepEqual(receipt.identity, identity, 'Release version belongs to a different source/frontend identity')
  assert.deepEqual(receipt.crates, crates, 'Release publish closure changed')
  assert.equal(typeof receipt.automatic, 'boolean')
  assert.equal(typeof receipt.completed, 'boolean')
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

function selectVersion({ automatic, requestedVersion, sourceVersion, identity, crates,
  releases, receipts, versions, occupiedVersions = [], now }) {
  if (automatic) {
    const matches = receipts.filter((receipt) => receipt.automatic &&
      isDeepStrictEqual(receipt.identity, identity))
    assert.ok(matches.length <= 1, 'Multiple automatic reservations for one source identity')
    if (matches.length) return validateReceipt(matches[0], identity, crates).version
    return nextVersion(versions, now, occupiedVersions)
  }
  const version = assertManualVersion(!requestedVersion || requestedVersion === 'latest'
    ? sourceVersion : requestedVersion, now)
  const release = releases.find((entry) => entry.tag_name === `v${version}`)
  if (release) {
    const receipt = receipts.find((entry) => entry.version === version)
    validateReceipt(receipt, identity, crates, version)
  } else {
    assert.ok(!versions.includes(version) && !occupiedVersions.includes(version),
      `Version ${version} is already occupied without matching provenance`)
  }
  return version
}

async function plan(context) {
  const { automatic, identity, crates, requestedVersion, sourceVersion, dryRun } = context
  const manualVersion = automatic ? null : assertManualVersion(
    !requestedVersion || requestedVersion === 'latest' ? sourceVersion : requestedVersion, context.now)
  if (dryRun) {
    assert.ok(!automatic, 'Automatic publication cannot be a dry run')
    return { version: manualVersion, dryRun: true }
  }
  context.assertSigningKey()
  const releases = await context.releases()
  const receipts = []
  for (const release of releases) {
    const version = automatic ? null : !requestedVersion || requestedVersion === 'latest' ? sourceVersion : requestedVersion
    const receipt = await readReleaseReceipt(context, release, identity, version)
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
    assert.ok(release.draft || receipt.completed, 'Public release has incomplete provenance')
    await context.ensureTag(release, receipt)
    await context.ensureFrontend(release, receipt)
    return { receipt, release }
  }
  const receipt = { schemaVersion: 1, version, identity, crates,
    frontendBytes: context.frontendBytes, automatic, completed: false,
    ciRun: context.ciRun || null, packageChecksums: {} }
  // Reserve before uploading any crate. An uncertain GitHub write is recovered
  // by reading this exact tag on a rerun, never by allocating another version.
  const reserved = await context.reserve(receipt)
  await context.ensureTag(reserved, receipt)
  await context.ensureFrontend(reserved, receipt)
  return { receipt, release: reserved }
}

async function publish(context, release, receipt) {
  context.assertSigningKey()
  context.verifyReceipt(receipt)
  validateReceipt(receipt, context.identity, context.crates)
  if (!context.automatic) assertManualVersion(receipt.version, context.now)
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

async function readReleaseReceipt(context, release, identity, version) {
  // Public target SHAs do not establish receipt relevance without authentication.
  let required = Boolean(!context.automatic && version && release.tag_name === `v${version}`)
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
    const previous = await readReleaseReceipt(context, release, receipt.identity, receipt.version)
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
  if (!context.automatic) return
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
  if (!context.automatic) return false
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
    (receipt.ciRun ? `Validated main CI: ${receipt.ciRun}\n\n` : '') +
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

async function preserveFrontend({ receipt, stagedBytes, assets, readAsset, verifyFiles, writeFiles, saveReceipt, upload }) {
  if (assets.every(Boolean)) {
    const bytes = await Promise.all(assets.map(readAsset))
    if (sha256(bytes[0]) === receipt.frontendBytes.manifestSha256 &&
        sha256(bytes[1]) === receipt.frontendBytes.archiveSha256) {
      await verifyFiles(bytes)
      await writeFiles(bytes)
      return
    }
  }
  assert.deepEqual(receipt.packageChecksums, {}, 'Partial release has lost or changed its preserved frontend assets')
  // Only before the first package is reserved can interrupted initial asset
  // uploads adopt freshly staged bytes of the same verified immutable bundle.
  receipt.frontendBytes = stagedBytes
  await saveReceipt(receipt)
  await upload()
}

async function registryFetch(url, allowMissing = false) {
  const response = await fetch(url, { headers: { 'User-Agent': 'bamboo-publish-crate/2.0 (+https://github.com/bigduu/Bamboo-agent)' } })
  if (allowMissing && response.status === 404) return null
  assert.ok(response.ok, `Registry request failed (${response.status}): ${url}`)
  return response
}

function makeContext(env = process.env) {
  const repository = env.GITHUB_REPOSITORY
  assert.equal(repository, 'bigduu/Bamboo-agent')
  const sourceRevision = command(['git', 'rev-parse', 'HEAD']).trim()
  const event = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8'))
  const automatic = validateSource({ eventName: env.GITHUB_EVENT_NAME, event, repository,
    sourceRevision, workflowRevision: env.GITHUB_SHA })
  if (env.DRY_RUN !== 'true') assert.ok(env.CARGO_REGISTRY_TOKEN, 'Missing CARGO_REGISTRY_TOKEN')
  if (env.DRY_RUN !== 'true') assertSigningConfiguration(env.BAMBOO_RELEASE_SIGNING_KEY, env.BAMBOO_RELEASE_SIGNING_KEY_SHA256)
  const frontendDirectory = 'crates/app/bamboo-server/frontend_package'
  const manifest = JSON.parse(fs.readFileSync(`${frontendDirectory}/frontend-manifest.json`, 'utf8'))
  const identity = { repository, sourceRevision, frontend: frontendIdentity(
    env.FRONTEND_PACKAGE, env.FRONTEND_VERSION, manifest,
    JSON.parse(fs.readFileSync('scripts/frontend-package-lock.json', 'utf8')),
  ) }
  const stagedFrontend = [fs.readFileSync(`${frontendDirectory}/frontend-manifest.json`),
    fs.readFileSync(`${frontendDirectory}/lotus-frontend.zip`)]
  const frontendBytes = {
    manifestSha256: sha256(stagedFrontend[0]),
    archiveSha256: sha256(stagedFrontend[1]),
  }
  assert.ok(['@bigduu/lotus-next', '@bigduu/lotus'].includes(identity.frontend.packageName))
  assertVersion(identity.frontend.packageVersion)
  const crates = command(['python3', 'scripts/compute-publish-order.py']).trim().split('\n')
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
  const context = { automatic, identity, frontendBytes, crates, receiptFile,
    requestedVersion: env.PUBLISH_VERSION || '',
    sourceVersion: fs.readFileSync('Cargo.toml', 'utf8').match(/^version = "([^"]+)"$/m)?.[1],
    dryRun: env.DRY_RUN === 'true', ciRun: automatic ? event.workflow_run.html_url : null,
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
    reserve: async (receipt) => github(repository, 'releases', {
      tag_name: `v${receipt.version}`, target_commitish: sourceRevision,
      name: `Bamboo v${receipt.version}`, draft: true,
      body: receiptBody(signReceipt(receipt, env.BAMBOO_RELEASE_SIGNING_KEY)),
    }),
    ensureTag: async (_release, receipt) => {
      const ref = `tags/v${receipt.version}`
      let tag = github(repository, `git/ref/${ref}`, undefined, undefined, true)
      // Tag creation checks repository/workflow write authority before the
      // first crates.io upload. Queued historical source must never discover
      // a release-token permission failure only after publishing all crates.
      if (!tag) tag = github(repository, 'git/refs', { ref: `refs/${ref}`, sha: sourceRevision })
      assert.equal(tag.object.type, 'commit', 'Reserved release tag must be a direct commit ref')
      assert.equal(tag.object.sha, sourceRevision, 'Reserved release tag points to different source')
    },
    saveReceipt: async (release, receipt) => {
      signReceipt(receipt, env.BAMBOO_RELEASE_SIGNING_KEY)
      fs.writeFileSync(receiptFile, `${JSON.stringify(receipt, null, 2)}\n`)
      github(repository, `releases/${release.id}`, { body: receiptBody(receipt) }, 'PATCH')
    },
    ensureFrontend: async (release, receipt) => {
      const current = github(repository, `releases/${release.id}`)
      const names = ['frontend-manifest.json', 'lotus-frontend.zip']
      const assets = names.map((name) => current.assets.find((asset) => asset.name === name))
      await preserveFrontend({ receipt, stagedBytes: frontendBytes, assets,
        readAsset: async (asset) => command(['gh', 'api', `repos/${repository}/releases/assets/${asset.id}`,
          '-H', 'Accept: application/octet-stream'], { encoding: null }),
        verifyFiles: async (bytes) => verifyPreservedFrontend(stagedFrontend, bytes),
        writeFiles: async (bytes) => bytes.forEach((content, index) => fs.writeFileSync(path.join(frontendDirectory, names[index]), content)),
        saveReceipt: async () => context.saveReceipt(release, receipt),
        upload: async () => command(['gh', 'release', 'upload', `v${receipt.version}`,
          ...names.map((name) => path.join(frontendDirectory, name)), '--clobber', '--repo', repository]),
      })
    },
    registry: async (name, version) => {
      const response = await registryFetch(`https://crates.io/api/v1/crates/${name}/${version}`, true)
      return response ? (await response.json()).version : null
    },
    package: async (name, version) => {
      command(['cargo', 'package', '--locked', '--allow-dirty', '--no-verify', '-p', name])
      const target = jsonCommand(['cargo', 'metadata', '--format-version', '1', '--no-deps']).target_directory
      return sha256(fs.readFileSync(path.join(target, 'package', `${name}-${version}.crate`)))
    },
    cargoPublish: async (name) => {
      const result = spawnCommand(['cargo', 'publish', '--locked', '--allow-dirty', '-p', name], {
        encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
      })
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
      command(['gh', 'release', 'upload', `v${receipt.version}`, receiptFile, '--clobber', '--repo', repository])
      return github(repository, `releases/${release.id}`, {
        draft: false, make_latest: makeLatest ? 'true' : 'false', body: receiptBody(receipt),
      }, 'PATCH')
    },
  }
  return context
}

async function main() {
  const context = makeContext()
  if (process.argv[2] === 'plan') {
    const result = await plan(context)
    const version = result.receipt?.version || result.version
    if (result.receipt) fs.writeFileSync(context.receiptFile, `${JSON.stringify(result.receipt, null, 2)}\n`)
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `version=${version}\nreceipt_file=${context.receiptFile}\n`)
    console.log(`Resolved ${context.automatic ? 'automatic main' : 'manual'} release: ${version}`)
  } else {
    assert.equal(process.argv[2], 'publish')
    const receipt = JSON.parse(fs.readFileSync(context.receiptFile, 'utf8'))
    const release = (await context.releases()).find((entry) => entry.tag_name === `v${receipt.version}`)
    assert.ok(release, 'Reserved draft Release disappeared')
    await publish(context, release, receipt)
  }
}

module.exports = { assertVersion, validateSource, validateReceipt, nextVersion, selectVersion,
  plan, publish, makeContext, sha256, githubArgs, receiptBody, readBodyReceipt,
  frontendIdentity, verifyPackageArchive, preserveFrontend, verifyPreservedFrontend, shouldMakeLatest, gitIsAncestor,
  canonicalJson, signReceipt, authenticateReceipt, readAuthenticatedReceipt, assertSigningKey, assertSigningConfiguration,
  verifyRegistrySource, highestStableVersion, spawnCommand, command }
if (require.main === module) main().catch((error) => { console.error(error); process.exitCode = 1 })
