const assert = require("node:assert/strict");
const { test } = require("node:test");
const { releaseInputs } = require("../scripts/pack-browser-runtime.cjs");

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
