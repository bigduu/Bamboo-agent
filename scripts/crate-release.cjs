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
const VERSION = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$/
const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

function assertVersion(version) {
  assert.ok(typeof version === 'string' && VERSION.test(version) && version !== '0.0.0',
    'Pass a real, explicit publish version; source 0.0.0 is a placeholder')
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

function nextVersion(versions, now = new Date()) {
  const prefix = `${now.getUTCFullYear()}.${now.getUTCMonth() + 1}.`
  let maximum = 0
  for (const version of versions) {
    if (!version.startsWith(prefix) || !/^\d+$/.test(version.slice(prefix.length))) continue
    const counter = Number(version.slice(prefix.length))
    assert.ok(Number.isSafeInteger(counter), 'Published release counter exceeds safe integer range')
    maximum = Math.max(maximum, counter)
  }
  assert.ok(Number.isSafeInteger(maximum + 1))
  return `${prefix}${maximum + 1}`
}

function selectVersion({ automatic, requestedVersion, sourceVersion, identity, crates,
  releases, receipts, versions, now }) {
  if (automatic) {
    const matches = receipts.filter((receipt) => receipt.automatic &&
      isDeepStrictEqual(receipt.identity, identity))
    assert.ok(matches.length <= 1, 'Multiple automatic reservations for one source identity')
    if (matches.length) return validateReceipt(matches[0], identity, crates).version
    return nextVersion(versions, now)
  }
  const version = assertVersion(!requestedVersion || requestedVersion === 'latest'
    ? sourceVersion : requestedVersion)
  const release = releases.find((entry) => entry.tag_name === `v${version}`)
  if (release) {
    const receipt = receipts.find((entry) => entry.version === version)
    validateReceipt(receipt, identity, crates, version)
  } else {
    assert.ok(!versions.includes(version), `Version ${version} is already occupied without matching provenance`)
  }
  return version
}

async function plan(context) {
  const { automatic, identity, crates, requestedVersion, sourceVersion, dryRun } = context
  if (dryRun) {
    assert.ok(!automatic, 'Automatic publication cannot be a dry run')
    return { version: assertVersion(!requestedVersion || requestedVersion === 'latest'
      ? sourceVersion : requestedVersion), dryRun: true }
  }
  const releases = await context.releases()
  const receipts = []
  for (const release of releases) {
    if ((automatic && release.target_commitish === identity.sourceRevision) ||
        (!automatic && release.tag_name === `v${!requestedVersion || requestedVersion === 'latest' ? sourceVersion : requestedVersion}`)) {
      // An existing version may be resumed only with a trusted, matching receipt.
      const receipt = await context.readReceipt(release)
      if (receipt) receipts.push(receipt)
    }
  }
  const versions = [...await context.versions(crates),
    ...releases.map((release) => release.tag_name.replace(/^v/, '')),
    ...await context.tags()]
  const version = selectVersion({ ...context, releases, receipts, versions })
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
  validateReceipt(receipt, context.identity, context.crates)
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

async function completedAutomaticReceipts(context, currentRelease, receipt, { stableOnly = false } = {}) {
  const receipts = []
  for (const release of await context.releases()) {
    if (release.id === currentRelease.id || (stableOnly && (release.draft || release.prerelease))) continue
    const previous = await context.readReceipt(release)
    if (!previous?.automatic || !previous.completed) continue
    validateReceipt(previous, previous.identity, previous.crates)
    assert.equal(previous.identity.repository, receipt.identity.repository)
    assert.match(previous.identity.sourceRevision, SHA)
    assert.equal(release.target_commitish, previous.identity.sourceRevision)
    assert.equal(release.tag_name, `v${previous.version}`)
    assert.equal(await context.tagSource(previous.version), previous.identity.sourceRevision,
      'Completed automatic release tag points to different source')
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
  if (!receipt.automatic) return
  for (const previous of await completedAutomaticReceipts(context, release, receipt)) {
    if (previous.identity.sourceRevision !== receipt.identity.sourceRevision &&
        compareAutomaticVersions(receipt.version, previous.version) >= 0) {
      assert.ok(await context.isAncestor(previous.identity.sourceRevision, receipt.identity.sourceRevision),
        'Older or unproven main source cannot publish at or above a completed newer source version')
    }
  }
}

async function shouldMakeLatest(context, currentRelease, receipt) {
  if (!receipt.automatic) return false
  // A retry may finish after a newer main source. Release numbers describe
  // allocation time, so a late first attempt for old CI can have a larger one.
  for (const previous of await completedAutomaticReceipts(context, currentRelease, receipt, { stableOnly: true })) {
    if (previous.identity.sourceRevision === receipt.identity.sourceRevision) {
      if (compareAutomaticVersions(previous.version, receipt.version) > 0) return false
    } else if (!await context.isAncestor(previous.identity.sourceRevision, receipt.identity.sourceRevision)) {
      // Only a proven descendant can take latest from a completed auto source;
      // old recovery and divergent/rewritten history both retain the latest.
      return false
    }
  }
  return true
}

function gitIsAncestor(ancestor, descendant, cwd) {
  for (const revision of [ancestor, descendant]) {
    assert.match(revision, SHA)
    const exists = spawnSync('git', ['cat-file', '-e', `${revision}^{commit}`], { cwd, encoding: 'utf8' })
    if (exists.error) throw exists.error
    if (exists.status !== 0) command(['git', 'fetch', '--no-tags', 'origin', revision], { cwd })
  }
  const result = spawnSync('git', ['merge-base', '--is-ancestor', ancestor, descendant], { cwd, encoding: 'utf8' })
  if (result.error) throw result.error
  assert.ok(result.status === 0 || result.status === 1, `Unable to compare release source ancestry: ${result.stderr}`)
  return result.status === 0
}

function command(args, options = {}) {
  const result = spawnSync(args[0], args.slice(1), { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, ...options })
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
  const result = spawnSync(args[0], args.slice(1), { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
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
  assert.equal(sha256(bytes), expected, `Downloaded package checksum mismatch for ${name}`)
  const vcs = JSON.parse(entry('.cargo_vcs_info.json').toString('utf8'))
  assert.equal(vcs.git.sha1, receipt.identity.sourceRevision, `Published ${name} source revision mismatch`)
  if (name === 'bamboo-server') {
    assert.equal(sha256(entry('frontend_package/frontend-manifest.json')), receipt.frontendBytes.manifestSha256)
    assert.equal(sha256(entry('frontend_package/lotus-frontend.zip')), receipt.frontendBytes.archiveSha256)
  }
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
  const paginated = (route) => jsonCommand(['gh', 'api', '--paginate', '--slurp', `repos/${repository}/${route}`]).flat()
  const context = { automatic, identity, frontendBytes, crates, receiptFile,
    requestedVersion: env.PUBLISH_VERSION || '',
    sourceVersion: fs.readFileSync('Cargo.toml', 'utf8').match(/^version = "([^"]+)"$/m)?.[1],
    dryRun: env.DRY_RUN === 'true', ciRun: automatic ? event.workflow_run.html_url : null,
    releases: async () => paginated('releases?per_page=100'),
    tags: async () => paginated('tags?per_page=100').map((tag) => tag.name.replace(/^v/, '')),
    versions: async (names) => {
      const versions = []
      for (const name of names) {
        const response = await registryFetch(`https://crates.io/api/v1/crates/${name}`, true)
        if (response) versions.push(...(await response.json()).versions.map((version) => version.num))
      }
      return versions
    },
    readReceipt: async (release) => readBodyReceipt(release),
    isAncestor: async (ancestor, descendant) => gitIsAncestor(ancestor, descendant),
    tagSource: async (version) => {
      const tag = github(repository, `git/ref/tags/v${assertVersion(version)}`)
      assert.equal(tag.object.type, 'commit', 'Completed automatic release tag must be a direct commit ref')
      return tag.object.sha
    },
    reserve: async (receipt) => github(repository, 'releases', {
      tag_name: `v${receipt.version}`, target_commitish: sourceRevision,
      name: `Bamboo v${receipt.version}`, draft: true, body: receiptBody(receipt),
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
      const result = spawnSync('cargo', ['publish', '--locked', '--allow-dirty', '-p', name], {
        encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
      })
      if (result.error) throw result.error
      const output = `${result.stdout || ''}${result.stderr || ''}`
      process.stdout.write(output)
      return { status: result.status, output }
    },
    verifyArchive: async (name, receipt, checksum) => {
      const response = await registryFetch(`https://crates.io/api/v1/crates/${name}/${receipt.version}/download`)
      const bytes = Buffer.from(await response.arrayBuffer())
      const archive = path.join(directory, `${name}-${receipt.version}.crate`)
      fs.writeFileSync(archive, bytes)
      const entry = (file) => command(['tar', '-xOf', archive, `${name}-${receipt.version}/${file}`], { encoding: null })
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
  frontendIdentity, verifyPackageArchive, preserveFrontend, verifyPreservedFrontend, shouldMakeLatest, gitIsAncestor }
if (require.main === module) main().catch((error) => { console.error(error); process.exitCode = 1 })
