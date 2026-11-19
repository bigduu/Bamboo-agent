const assert = require('node:assert/strict')
const { spawnSync } = require('node:child_process')
const { createHash } = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')

const root = path.resolve(__dirname, '..')

test('the npm launcher uses the pinned binary, passes arguments and reuses the verified cache', {
  skip: !['darwin', 'linux'].includes(process.platform) || !['x64', 'arm64'].includes(process.arch) ||
    (process.platform === 'darwin' && process.arch !== 'arm64'),
}, (t) => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-npm-launcher-')))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  const bin = path.join(dir, 'bin')
  const cache = path.join(dir, 'cache')
  const launcher = path.join(bin, 'bamboo.cjs')
  const preload = path.join(dir, 'fetch.cjs')
  const requests = path.join(dir, 'requests')
  const version = '2026.11.22'
  const target = process.platform === 'darwin' ? 'aarch64-apple-darwin'
    : process.arch === 'arm64' ? 'aarch64-unknown-linux-gnu' : 'x86_64-unknown-linux-gnu'
  const assetName = 'bamboo-' + target
  const executable = Buffer.from('#!/bin/sh\nprintf "%s\\n" "$1" "$BAMBOO_BROWSER_HOST_SCRIPT"\nexit 7\n')
  const sha256 = createHash('sha256').update(executable).digest('hex')
  fs.mkdirSync(bin)
  fs.copyFileSync(path.join(root, 'npm/bin/bamboo.cjs'), launcher)
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ version }))
  fs.writeFileSync(path.join(dir, 'bamboo-sidecars-v3.json'), JSON.stringify({
    schemaVersion: 3, version, artifacts: [{ target, assetName, sha256, sizeBytes: executable.length }],
  }))
  fs.writeFileSync(preload, [
    'const assert = require("node:assert/strict")',
    'const fs = require("node:fs")',
    'globalThis.fetch = async (url) => {',
    '  assert.equal(url, process.env.BAMBOO_TEST_URL)',
    '  fs.appendFileSync(process.env.BAMBOO_TEST_REQUESTS, "download\\n")',
    '  return new Response(Buffer.from(process.env.BAMBOO_TEST_BYTES, "base64"), { status: 200 })',
    '}',
  ].join('\n'))

  const run = (bytes) => spawnSync(process.execPath, ['--require', preload, launcher, '--version'], {
    encoding: 'utf8',
    timeout: 10_000,
    env: {
      ...process.env,
      BAMBOO_BROWSER_HOST_SCRIPT: '',
      BAMBOO_NPM_CACHE_DIR: cache,
      BAMBOO_TEST_URL: 'https://github.com/bigduu/Bamboo-agent/releases/download/v' + version + '/' + assetName,
      BAMBOO_TEST_REQUESTS: requests,
      BAMBOO_TEST_BYTES: bytes.toString('base64'),
    },
  })
  const first = run(executable), second = run(executable)
  assert.equal(first.status, 7, first.stderr)
  assert.equal(second.status, 7, second.stderr)
  assert.equal(first.stdout, '--version\n' + path.join(dir, 'browser-runtime', 'host.cjs') + '\n')
  assert.equal(second.stdout, first.stdout)
  assert.equal(fs.readFileSync(requests, 'utf8'), 'download\n')

  const cached = path.join(cache, version, target, assetName)
  const corrupted = Buffer.alloc(executable.length)
  fs.writeFileSync(cached, corrupted)
  const invalid = run(corrupted)
  assert.equal(invalid.status, 1)
  assert.match(invalid.stderr, /does not match the published binary SHA-256 and size/)
  assert.deepEqual(fs.readFileSync(cached), corrupted)
  assert.deepEqual(fs.readdirSync(path.dirname(cached)), [assetName])
})

test('npm assembly retains the release manifest and refuses an altered Browser Host', (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-npm-assembly-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  const result = spawnSync('python3', ['-B', '-c', [
    'import io, json, sys, tarfile',
    'from pathlib import Path',
    'from runpy import run_path',
    'packager = run_path("scripts/npm-release.py")',
    'files = {',
    '    "browser-runtime/host.cjs": b"// fixture",',
    '    "browser-runtime/package.json": b\'{"dependencies":{"playwright-core":"1.62.1"}}\',',
    '    "browser-runtime/package-lock.json": b\'{"packages":{"":{"dependencies":{"playwright-core":"1.62.1"}},"node_modules/playwright-core":{"version":"1.62.1"}}}\',',
    '}',
    'transport = io.BytesIO()',
    'with tarfile.open(fileobj=transport, mode="w:gz") as archive:',
    '    for name, data in files.items():',
    '        member = tarfile.TarInfo(name)',
    '        member.size = len(data)',
    '        archive.addfile(member, io.BytesIO(data))',
    'raw = transport.getvalue()',
    'sha256 = packager["sha256"]',
    'host = {"sizeBytes": len(raw), "sha256": sha256(raw),',
    '        "files": [{"path": name, "sha256": sha256(data), "sizeBytes": len(data)} for name, data in files.items()]}',
    'manifest = json.dumps({"schemaVersion": 3, "version": "2026.11.22", "browserHost": host}).encode()',
    'output = Path(sys.argv[1]) / "package"',
    'packager["assemble"](output, manifest, raw)',
    'metadata = json.loads((output / "package.json").read_bytes())',
    'assert metadata["name"] == "@bigduu/bamboo-agent" and metadata["version"] == "2026.11.22"',
    'assert "private" not in metadata and metadata["dependencies"] == {"playwright-core": "1.62.1"}',
    'assert (output / "bamboo-sidecars-v3.json").read_bytes() == manifest',
    'for name, data in files.items(): assert (output / name).read_bytes() == data',
    'try:',
    '    packager["assemble"](Path(sys.argv[1]) / "altered", manifest, raw[:-1] + bytes([raw[-1] ^ 1]))',
    'except AssertionError as error:',
    '    assert str(error) == "Browser Host archive changed"',
    'else:',
    '    raise AssertionError("The packager accepted changed Browser Host bytes")',
    'assert not (Path(sys.argv[1]) / "altered").exists()',
  ].join('\n'), dir], { cwd: root, encoding: 'utf8', timeout: 20_000 })
  assert.equal(result.status, 0, result.stderr)
})

test('the npm workflow keeps package verification outside the registry credential jobs', () => {
  const workflow = fs.readFileSync(path.join(root, '.github/workflows/publish-npm.yml'), 'utf8')
  const [prepare, registry] = workflow.split('\n  stage:\n')
  const [stage, publish] = registry.split('\n  publish:\n')
  assert.match(prepare, /on:\n  workflow_dispatch:/)
  assert.match(prepare, /refs\/heads\/dev' && github\.ref_protected/)
  assert.match(prepare, /npm pack "\$package" --dry-run --json --ignore-scripts/)
  assert.doesNotMatch(prepare, /secrets\.|id-token:|cargo/)
  assert.match(stage, /if: inputs\.operation == 'stage'/)
  assert.match(stage, /sha256sum --check --strict/)
  assert.match(stage, /NODE_AUTH_TOKEN:.*BAMBOO_NPM_STAGE_TOKEN/)
  assert.match(stage, /npm stage publish "\$NPM_TARBALL" --access public/)
  assert.doesNotMatch(stage, /id-token:/)
  assert.match(publish, /if: inputs\.operation == 'publish'/)
  assert.match(publish, /id-token: write/)
  assert.match(publish, /npm publish "\$NPM_TARBALL" --access public --tag latest --provenance/)
  assert.doesNotMatch(publish, /NODE_AUTH_TOKEN|secrets\./)
})
