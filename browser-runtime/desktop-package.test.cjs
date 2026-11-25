const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { committedBrowserHost, releaseInputs } = require("../scripts/pack-browser-runtime.cjs");

test("desktop runtime inputs match exactly one native runner", () => {
  const targets = [
    ["aarch64-apple-darwin", "darwin", "arm64", "darwin-arm64.tar.gz", "mac-arm64.zip"],
    ["aarch64-unknown-linux-gnu", "linux", "arm64", "linux-arm64.tar.gz", "linux-arm64.zip"],
    ["aarch64-pc-windows-msvc", "win32", "arm64", "win-arm64.zip", "win64.zip"],
    ["x86_64-unknown-linux-gnu", "linux", "x64", "linux-x64.tar.gz", "linux64.zip"],
    ["x86_64-pc-windows-msvc", "win32", "x64", "win-x64.zip", "win64.zip"],
  ];
  for (const [target, platform, arch, nodeSuffix, browserSuffix] of targets) {
    const inputs = releaseInputs(target, platform, arch);
    assert.ok(inputs.nodeUrl.endsWith(`-${nodeSuffix}`));
    assert.ok(inputs.browserUrl.endsWith(`-${browserSuffix}`));
    assert.match(inputs.pins.nodeSha256, /^[a-f0-9]{64}$/);
    assert.match(inputs.pins.browserSha256, /^[a-f0-9]{64}$/);
    for (const [other, otherPlatform, otherArch] of targets) {
      if (other === target) continue;
      assert.throws(() => releaseInputs(target, otherPlatform, otherArch), /native runner/);
    }
  }
  assert.equal(releaseInputs("aarch64-pc-windows-msvc", "win32", "arm64").pins.browserTarget, "x86_64-pc-windows-msvc");
  assert.equal(releaseInputs("aarch64-unknown-linux-gnu", "linux", "arm64").pins.browserSource, "playwright");
  assert.throws(() => releaseInputs("x86_64-apple-darwin", "darwin", "x64"), /native runner/);
});

test("the packaged Browser Host uses the released Git blobs despite CRLF checkout bytes", t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-runtime-source-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const git = (...args) => execFileSync("git", ["-c", "core.autocrlf=false", ...args], { cwd: root });
  git("init", "--quiet");
  fs.mkdirSync(path.join(root, "browser-runtime"));
  for (const name of ["host.cjs", "package.json", "package-lock.json"]) {
    fs.writeFileSync(path.join(root, "browser-runtime", name), "published bytes\n");
  }
  git("add", "browser-runtime");
  git("-c", "user.name=Bamboo Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "--no-gpg-sign", "-m", "test source");
  const revision = git("rev-parse", "HEAD").toString().trim();
  const working = path.join(root, "browser-runtime", "host.cjs");
  fs.writeFileSync(working, "published bytes\r\n");
  assert.equal(committedBrowserHost(root, revision).get("host.cjs").toString(), "published bytes\n");
});
