#!/usr/bin/env node
'use strict'

const { spawn } = require('node:child_process')
const { createHash } = require('node:crypto')
const { once } = require('node:events')
const { constants, createWriteStream } = require('node:fs')
const { lstat, mkdir, mkdtemp, open, rename, rm } = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const { Readable, Transform } = require('node:stream')
const { pipeline } = require('node:stream/promises')
const manifest = require('../bamboo-sidecars-v3.json')
const { version } = require('../package.json')

const targets = {
  'darwin-arm64': 'aarch64-apple-darwin',
  'linux-arm64': 'aarch64-unknown-linux-gnu',
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'win32-arm64': 'aarch64-pc-windows-msvc',
  'win32-x64': 'x86_64-pc-windows-msvc',
}

function cacheRoot() {
  const override = process.env.BAMBOO_NPM_CACHE_DIR
  if (override) {
    if (!path.isAbsolute(override)) throw new Error('BAMBOO_NPM_CACHE_DIR must be absolute')
    return override
  }
  if (process.platform === 'win32') {
    const local = process.env.LOCALAPPDATA
    return path.join(local && path.isAbsolute(local) ? local : path.join(os.homedir(), 'AppData', 'Local'), 'Bamboo', 'npm')
  }
  const xdg = process.env.XDG_CACHE_HOME
  if (xdg && path.isAbsolute(xdg)) return path.join(xdg, 'bamboo-agent', 'npm')
  return path.join(os.homedir(), process.platform === 'darwin' ? 'Library/Caches' : '.cache', 'bamboo-agent', 'npm')
}

async function matchesPublishedBinary(file, entry) {
  let stat
  try {
    stat = await lstat(file)
  } catch (error) {
    if (error.code === 'ENOENT') return false
    throw error
  }
  if (!stat.isFile() || stat.size !== entry.sizeBytes) return false
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0))
  try {
    const hash = createHash('sha256')
    for await (const chunk of handle.createReadStream({ autoClose: false })) hash.update(chunk)
    return hash.digest('hex') === entry.sha256
  } finally {
    await handle.close()
  }
}

function verifyDownload(entry) {
  const hash = createHash('sha256')
  let bytes = 0
  return new Transform({
    transform(chunk, encoding, done) {
      bytes += chunk.length
      if (bytes > entry.sizeBytes) return done(new Error('Download exceeds the published binary size'))
      hash.update(chunk)
      done(null, chunk)
    },
    flush(done) {
      if (bytes !== entry.sizeBytes || hash.digest('hex') !== entry.sha256) {
        return done(new Error('Download does not match the published binary SHA-256 and size'))
      }
      done()
    },
  })
}

async function ensureExecutable(entry) {
  const root = path.join(cacheRoot(), version, entry.target)
  const destination = path.join(root, entry.assetName)
  await mkdir(root, { recursive: true, mode: 0o700 })
  if (await matchesPublishedBinary(destination, entry)) return destination

  const temporary = await mkdtemp(path.join(root, '.download-'))
  const file = path.join(temporary, entry.assetName)
  try {
    process.stderr.write('Downloading Bamboo ' + version + ' for ' + entry.target + '\n')
    const url = 'https://github.com/bigduu/Bamboo-agent/releases/download/v' + version + '/' + entry.assetName
    const response = await fetch(url, { signal: AbortSignal.timeout(10 * 60 * 1000) })
    if (!response.ok || !response.body) throw new Error('GitHub Release download failed (HTTP ' + response.status + ')')
    await pipeline(Readable.fromWeb(response.body), verifyDownload(entry), createWriteStream(file, { flags: 'wx', mode: 0o700 }))
    try {
      await rename(file, destination)
    } catch (error) {
      if (!await matchesPublishedBinary(destination, entry)) throw error
    }
    return destination
  } finally {
    await rm(temporary, { recursive: true, force: true })
  }
}

async function main() {
  if (manifest.schemaVersion !== 3 || manifest.version !== version) {
    throw new Error('The included Bamboo release manifest does not match this package version')
  }
  const target = targets[process.platform + '-' + process.arch]
  const entry = manifest.artifacts.find((candidate) => candidate.target === target)
  if (!entry) throw new Error('Bamboo ' + version + ' has no executable for ' + process.platform + '/' + process.arch)
  const executable = await ensureExecutable(entry)
  const env = {
    ...process.env,
    BAMBOO_BROWSER_HOST_SCRIPT: process.env.BAMBOO_BROWSER_HOST_SCRIPT || path.join(__dirname, '..', 'browser-runtime', 'host.cjs'),
    BAMBOO_BROWSER_NODE: process.env.BAMBOO_BROWSER_NODE || process.execPath,
  }
  const child = spawn(executable, process.argv.slice(2), { stdio: 'inherit', env })
  const signals = ['SIGINT', 'SIGTERM']
  const handlers = signals.map((signal) => () => child.kill(signal))
  signals.forEach((signal, index) => process.on(signal, handlers[index]))
  try {
    const [code, signal] = await once(child, 'exit')
    process.exitCode = code ?? 128 + (os.constants.signals[signal] ?? 1)
  } finally {
    signals.forEach((signal, index) => process.off(signal, handlers[index]))
  }
}

main().catch((error) => {
  process.stderr.write('Bamboo: ' + error.message + '\n')
  process.exitCode = 1
})
