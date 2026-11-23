"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { binaryArchitecture, execute, fileIdentity, HOST_FILES } = require("./sidecar-release.cjs");
const LOCK = require("../browser-runtime/desktop.lock.json");

const ROOT = path.resolve(__dirname, "..");
const MAX_ARCHIVE = 768 * 1024 ** 2;
const RUNNERS = {
  "aarch64-apple-darwin": ["darwin", "arm64"],
  "aarch64-pc-windows-msvc": ["win32", "arm64"],
  "aarch64-unknown-linux-gnu": ["linux", "arm64"],
  "x86_64-pc-windows-msvc": ["win32", "x64"],
  "x86_64-unknown-linux-gnu": ["linux", "x64"],
};
const readJson = file => JSON.parse(fs.readFileSync(file, "utf8"));

function releaseInputs(target, platform = process.platform, architecture = process.arch) {
  assert.deepEqual(RUNNERS[target], [platform, architecture], `Build ${target} on its native runner`);
  assert.equal(LOCK.schemaVersion, 1);
  const pins = LOCK.targets[target];
  const windows = platform === "win32";
  const nodePlatform = `${windows ? "win" : platform}-${architecture}`;
  const nodeFolder = `node-v${LOCK.nodeVersion}-${nodePlatform}`;
  const nodeArchive = `${nodeFolder}.${windows ? "zip" : "tar.gz"}`;
  const browserArchive = pins.browserSource === "playwright"
    ? "chromium-headless-shell-linux-arm64.zip" : `chrome-headless-shell-${pins.platform}.zip`;
  const browserUrl = pins.browserSource === "playwright"
    ? `https://cdn.playwright.dev/builds/chromium/${LOCK.chromiumRevision}/${browserArchive}`
    : `https://storage.googleapis.com/chrome-for-testing-public/${LOCK.chromiumVersion}/${pins.platform}/${browserArchive}`;
  return { pins, windows, nodeFolder, nodeArchive, browserArchive, browserUrl,
    nodeUrl: `https://nodejs.org/dist/v${LOCK.nodeVersion}/${nodeArchive}` };
}

function inventory(root) {
  const files = [];
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      const relative = path.relative(root, file).split(path.sep).join("/");
      if (entry.isDirectory()) visit(file);
      else {
        assert.ok(entry.isFile(), `Unexpected browser runtime member ${relative}`);
        files.push([relative, fileIdentity(file, MAX_ARCHIVE, 0).sha256]);
      }
    }
  }
  visit(root);
  files.sort(([a], [b]) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  const hash = crypto.createHash("sha256");
  for (const [name, digest] of files) hash.update(`${name}\0${digest}\n`);
  return hash.digest("hex");
}

function download(url, output, expected) {
  assert.match(expected, /^[a-f0-9]{64}$/);
  execute("curl", ["-q", "--fail", "--silent", "--show-error", "--location", "--max-redirs", "4", "--retry", "2",
    "--max-time", "180", "--proto", "=https", "--proto-redir", "=https", "--max-filesize", String(MAX_ARCHIVE),
    "--output", output, url]);
  assert.equal(fileIdentity(output, MAX_ARCHIVE).sha256, expected, `Checksum differs for ${url}`);
}

function tar(windows) {
  if (!windows) return "tar";
  assert.ok(path.isAbsolute(process.env.SystemRoot || ""), "Expected a native Windows SystemRoot");
  return path.join(process.env.SystemRoot, "System32", "tar.exe");
}

function extractZip(archive, destination, windows) {
  fs.mkdirSync(destination, { recursive: true });
  if (windows) execute(tar(windows), ["-xf", archive, "-C", destination]);
  else execute("unzip", ["-q", archive, "-d", destination]);
}

function committedBrowserHost(sourceRoot, revision) {
  const limit = 8 * 1024 ** 2;
  return new Map(HOST_FILES.map(name => {
    const entry = execute("git", ["ls-tree", revision, "--", name], { cwd: sourceRoot }).trim();
    assert.ok(entry.startsWith("100644 blob ") && entry.endsWith(`\t${name}`), `Expected a regular Browser Host asset ${name}`);
    const bytes = execute("git", ["show", `${revision}:${name}`], { cwd: sourceRoot, encoding: "buffer", maxBuffer: limit });
    assert.ok(Buffer.isBuffer(bytes) && bytes.length > 0 && bytes.length <= limit);
    return [path.basename(name), bytes];
  }));
}

function pack(target, output, sourceRoot = ROOT) {
  const { pins, windows, nodeFolder, nodeArchive, browserArchive, browserUrl, nodeUrl } = releaseInputs(target);
  const sourceRevision = execute("git", ["rev-parse", "HEAD"], { cwd: sourceRoot }).trim();
  assert.match(sourceRevision, /^[a-f0-9]{40}$/);
  const sourceLock = execute("git", ["show", `${sourceRevision}:browser-runtime/desktop.lock.json`], { cwd: sourceRoot });
  assert.deepEqual(JSON.parse(sourceLock), LOCK);
  const host = committedBrowserHost(sourceRoot, sourceRevision);
  const lockedPlaywright = JSON.parse(host.get("package-lock.json")).packages?.["node_modules/playwright-core"];
  assert.equal(lockedPlaywright?.version, LOCK.playwrightCoreVersion);
  assert.equal(lockedPlaywright?.integrity, LOCK.playwrightCoreIntegrity);

  const directory = fs.realpathSync(output);
  const assetName = `bamboo-browser-runtime-v1-${target}.tar.gz`;
  const destination = path.join(directory, assetName);
  const scratch = fs.mkdtempSync(path.join(directory, ".browser-runtime-"));
  try {
    const runtime = path.join(scratch, "browser-runtime");
    const nodeDir = path.join(runtime, "node");
    const chromium = path.join(runtime, "chromium");
    fs.mkdirSync(nodeDir, { recursive: true });
    const nodeInput = path.join(scratch, nodeArchive);
    const browserInput = path.join(scratch, browserArchive);
    download(nodeUrl, nodeInput, pins.nodeSha256);
    download(browserUrl, browserInput, pins.browserSha256);

    if (windows) extractZip(nodeInput, scratch, true);
    else execute(tar(false), ["-xzf", nodeInput, "-C", scratch]);
    const nodeName = windows ? "node.exe" : "node";
    const node = path.join(nodeDir, nodeName);
    fs.copyFileSync(path.join(scratch, nodeFolder, ...(windows ? [] : ["bin"]), nodeName), node);
    fs.copyFileSync(path.join(scratch, nodeFolder, "LICENSE"), path.join(nodeDir, "LICENSE"));
    fs.chmodSync(node, 0o755);
    extractZip(browserInput, chromium, windows);
    const browserDir = path.join(chromium, `chrome-headless-shell-${pins.platform}`);
    const browser = path.join(browserDir, `chrome-headless-shell${windows ? ".exe" : ""}`);
    if (pins.browserSource === "playwright") {
      fs.renameSync(path.join(chromium, "chrome-linux"), browserDir);
      fs.renameSync(path.join(browserDir, "headless_shell"), browser);
    }
    for (const [name, bytes] of host) fs.writeFileSync(path.join(runtime, name), bytes, { flag: "wx" });
    execute(windows ? "npm.cmd" : "npm", ["ci", "--omit=dev", "--ignore-scripts", "--no-bin-links", "--no-audit", "--no-fund",
      "--registry=https://registry.npmjs.org"], { cwd: runtime, shell: windows });
    const playwright = path.join(runtime, "node_modules/playwright-core");
    const shell = readJson(path.join(playwright, "browsers.json")).browsers.find(item => item.name === "chromium-headless-shell");
    assert.equal(readJson(path.join(playwright, "package.json")).version, LOCK.playwrightCoreVersion);
    assert.equal(shell?.revision, LOCK.chromiumRevision);
    assert.equal(shell?.browserVersion, LOCK.chromiumVersion);
    fs.rmSync(path.join(playwright, "bin"), { recursive: true });
    fs.unlinkSync(path.join(playwright, "lib/xdg-open"));
    fs.chmodSync(path.join(playwright, "cli.js"), 0o644);
    fs.chmodSync(path.join(playwright, "lib/webp_codec.wasm"), 0o644);

    binaryArchitecture(node, target);
    binaryArchitecture(browser, pins.browserTarget ?? target);
    assert.equal(execute(node, ["--version"]).trim(), `v${LOCK.nodeVersion}`);
    if (process.platform === "darwin") {
      for (const executable of [node, browser]) execute("codesign", ["--verify", "--strict", executable]);
    }
    const receipt = path.join(runtime, "receipt.json");
    fs.writeFileSync(receipt, `${JSON.stringify({
      schemaVersion: 1, target, sourceRevision, nodeVersion: LOCK.nodeVersion,
      playwrightCoreVersion: LOCK.playwrightCoreVersion, chromiumVersion: LOCK.chromiumVersion,
      chromiumRevision: LOCK.chromiumRevision, nodeArchiveSha256: pins.nodeSha256, browserArchiveSha256: pins.browserSha256,
      signingIdentity: null, hostSha256: fileIdentity(path.join(runtime, "host.cjs")).sha256,
      packageLockSha256: fileIdentity(path.join(runtime, "package-lock.json")).sha256, filesSha256: inventory(runtime),
    }, null, 2)}\n`, { flag: "wx" });
    const archive = path.join(scratch, assetName);
    execute(tar(windows), ["-czf", archive, "-C", scratch, "browser-runtime"], {
      env: { ...process.env, COPYFILE_DISABLE: "1" }, timeout: 300_000,
    });
    const artifact = { assetName, archive: fileIdentity(archive, MAX_ARCHIVE), receiptSha256: fileIdentity(receipt).sha256 };
    fs.linkSync(archive, destination);
    assert.deepEqual(fileIdentity(destination, MAX_ARCHIVE), artifact.archive);
    return { target, sourceRevision, artifact };
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}

module.exports = { committedBrowserHost, pack, releaseInputs };
if (require.main === module) {
  const [target, directory] = process.argv.slice(2);
  try {
    assert.ok(target && directory, "Usage: node scripts/pack-browser-runtime.cjs <native-target> <existing-output-directory>");
    console.log(JSON.stringify(pack(target, directory), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
