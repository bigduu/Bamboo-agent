"use strict";
// Run only on a disposable Linux CI runner. The bounded mount below belongs to
// this fixture; it never fills the runner's root filesystem or uploads a crate.
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const sandbox = require("./release-cargo-sandbox.cjs");

function exec(bin, args, options = {}) {
  const result = spawnSync(bin, args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024, ...options });
  assert.equal(result.status, 0, `${bin}: ${result.error || result.stderr || result.stdout}`);
  return result.stdout + result.stderr;
}
const sha256 = bytes => crypto.createHash("sha256").update(bytes).digest("hex");

async function controller() {
  assert.equal(process.platform, "linux", "The disk fixture requires a Linux CI runner");
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-cargo-disk-test-")));
  const source = path.join(root, "source");
  const stateRoot = path.join(root, "isolation");
  const target = path.join(stateRoot, "data/target");
  let mounted = false;
  try {
    fs.mkdirSync(path.join(source, "src"), { recursive: true });
    fs.writeFileSync(path.join(source, "Cargo.toml"), '[package]\nname="fixture-disk"\nversion="0.0.0"\nedition="2021"\ndescription="Disposable publication disk fixture"\nlicense="MIT"\n');
    fs.writeFileSync(path.join(source, "src/lib.rs"), "pub fn ready() {}\n");
    fs.writeFileSync(path.join(source, "build.rs"), `use std::{env, fs, io::Write, path::Path};
fn main() {
    for key in ["GH_TOKEN", "GITHUB_TOKEN", "BAMBOO_RELEASE_SIGNING_KEY", "CARGO_REGISTRY_TOKEN", "GITHUB_OUTPUT"] {
        assert!(env::var_os(key).is_none(), "worker received controller authority");
    }
    let marker = Path::new("/cargo-home/disk-fixture-once");
    if marker.exists() {
        println!("cargo:warning=DISK_FIXTURE_RECOVERED");
        return;
    }
    fs::write(marker, b"first verification reached real disk pressure").unwrap();
    fs::create_dir_all("/target/debug").unwrap();
    let mut filler = fs::File::create("/target/debug/disk-fixture-fill").unwrap();
    let chunk = vec![0x5a; 1024 * 1024];
    loop {
        if let Err(error) = filler.write_all(&chunk) {
            assert_eq!(error.raw_os_error(), Some(28), "fixture must reach actual ENOSPC");
            // Leave only enough room for Cargo to flush this real failure's
            // diagnostics; free space remains far below the watermark.
            let length = filler.metadata().unwrap().len();
            filler.set_len(length.saturating_sub(chunk.len() as u64)).unwrap();
            eprintln!("DISK_FIXTURE_ENOSPC code={:?}: {}", error.raw_os_error(), error);
            panic!("fixture disk full: {}", error);
        }
    }
}
`);
    exec("git", ["init", "-q", source]);
    exec("git", ["-C", source, "add", "."]);
    exec("git", ["-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "disk fixture"]);
    const revision = exec("git", ["-C", source, "rev-parse", "HEAD"]).trim();
    const state = sandbox.install(source, stateRoot, { lowFreeBytes: 64 * 1024 ** 2, maxScratchBytes: 96 * 1024 ** 2 });
    assert.equal(fs.realpathSync(target), target);
    exec("sudo", ["-n", "mount", "-t", "tmpfs", "-o", "size=128m,mode=0777,nosuid,nodev", "bamboo-disk-fixture", target]);
    mounted = true;
    const volume = fs.statfsSync(target);
    assert.ok(volume.blocks * volume.bsize <= 128 * 1024 ** 2, "fixture must use its bounded target mount");
    const env = { ...process.env, BAMBOO_CARGO_SANDBOX: path.join(stateRoot, "state.json") };
    const cargo = args => exec(path.join(stateRoot, "bin/cargo"), args, { cwd: source, env });
    cargo(["metadata", "--format-version", "1", "--offline"]);
    cargo(["package", "--locked", "--allow-dirty", "--offline", "--no-verify", "-p", "fixture-disk"]);
    const archive = path.join(source, "target/package/fixture-disk-0.0.0.crate");
    const reserved = fs.readFileSync(archive);
    const archiveHash = sha256(reserved);
    const vcs = JSON.parse(exec("tar", ["-xOf", archive, "fixture-disk-0.0.0/.cargo_vcs_info.json"]));
    assert.equal(vcs.git.sha1, revision);
    const protectedFiles = new Map([
      [path.join(root, "signed-reservation-fixture.json"), "immutable-source-version-receipt"],
      [path.join(root, "host-only-sentinel"), "untouched-host-file"],
      [path.join(stateRoot, "data/cache/cache-sentinel"), "preserved-registry-cache"],
      [path.join(target, "package/package-sentinel"), "preserved-worker-package-output"],
      [path.join(source, "Cargo.toml"), fs.readFileSync(path.join(source, "Cargo.toml"))],
      [path.join(source, "Cargo.lock"), fs.readFileSync(path.join(source, "Cargo.lock"))],
    ]);
    for (const [file, bytes] of protectedFiles) {
      if (!fs.existsSync(file)) fs.writeFileSync(file, bytes);
    }
    const uploader = require("./release-cargo-upload.cjs");
    const originalUpload = uploader.uploadArchive;
    let uploads = 0;
    uploader.uploadArchive = async (bytes, metadata, token) => {
      uploads++;
      assert.equal(token, "dummy-registry");
      assert.equal(metadata.name, "fixture-disk");
      assert.equal(metadata.vers, "0.0.0");
      assert.equal(sha256(bytes), archiveHash);
      return { status: 0, output: "Fixture upload verified exact reserved bytes; no network request" };
    };
    try {
      assert.equal(await sandbox.run(state, ["publish", "--locked", "--allow-dirty", "-p", "fixture-disk"]), 0);
    } finally { uploader.uploadArchive = originalUpload; }
    assert.equal(uploads, 1);
    assert.equal(sha256(fs.readFileSync(archive)), archiveHash);
    for (const [file, bytes] of protectedFiles) assert.deepEqual(fs.readFileSync(file), Buffer.from(bytes), `preserved ${path.basename(file)}`);
    assert.equal(exec("git", ["-C", source, "rev-parse", "HEAD"]).trim(), revision);
    assert.ok(fs.existsSync(path.join(stateRoot, "data/cache/disk-fixture-once")), "recovery must preserve the worker cache marker");
    assert.ok(!fs.existsSync(path.join(target, "debug/disk-fixture-fill")), "recovery must remove the full debug scratch tree");
    assert.ok(!fs.readdirSync(path.join(stateRoot, "data")).some(name => name.startsWith("work-")), "disposable sources must be removed");
    // Removing worker-owned debug directories without sudo checks the quiet
    // permission handoff, in addition to checking the Cargo exit code above.
    fs.rmSync(path.join(target, "debug"), { recursive: true, force: true });
    console.log("Real 128 MiB ENOSPC: recovered once; one upload; archive, source, receipt and cache preserved");
  } finally {
    // Never recursively remove a live mount if unmounting fails.
    if (mounted) exec("sudo", ["-n", "umount", target]);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

if (process.argv[2] === "--controller") controller().catch(error => { console.error(error); process.exitCode = 1; });
else test("real Cargo publication recovers once from a bounded target filesystem", {
  skip: process.env.BAMBOO_CARGO_DISK_INTEGRATION !== "1" || process.platform !== "linux",
}, () => {
  const output = exec(process.execPath, [__filename, "--controller"], { env: {
    PATH: process.env.PATH, HOME: process.env.HOME,
    DOCKER_HOST: process.env.DOCKER_HOST, DOCKER_CONTEXT: process.env.DOCKER_CONTEXT,
    GH_TOKEN: "dummy-github", GITHUB_TOKEN: "dummy-github", CARGO_REGISTRY_TOKEN: "dummy-registry",
    BAMBOO_RELEASE_SIGNING_KEY: "dummy-signing", GITHUB_OUTPUT: "dummy-runner-output",
  } });
  assert.match(output, /DISK_FIXTURE_ENOSPC code=Some\(28\)/);
  assert.match(output, /DISK_FIXTURE_RECOVERED/);
  assert.equal(output.match(/DISK_FIXTURE_ENOSPC code=Some\(28\)/g).length, 1);
  assert.equal(output.match(/DISK_FIXTURE_RECOVERED/g).length, 1);
  assert.equal(output.match(/^\[cargo-disk\] phase=enospc-retry attempt=2 package=fixture-disk /gm)?.length, 1);
  assert.doesNotMatch(output, /chmod:|Operation not permitted/);
  assert.match(output, /Real 128 MiB ENOSPC: recovered once/);
  console.log(output.trim());
});
