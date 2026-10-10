"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const { describeArchive, uploadArchive } = require("./release-cargo-upload.cjs");
const NAME = "fixture-pkg";
const VERSION = "1.2.3";
const ENDPOINT = "https://crates.io/api/v1/crates/new";
const DUMMY_TOKEN = "dummy-upload-token-not-a-real-credential";
const env = () => Object.fromEntries(["PATH", "HOME"].filter(key => process.env[key]).map(key => [key, process.env[key]]));
function python(code, input, extra = []) {
  const result = spawnSync("python3", ["-I", "-c", code, ...extra], { input, env: env(), maxBuffer: 80 * 1024 * 1024 });
  assert.equal(result.status, 0, result.stderr?.toString());
  return result.stdout;
}
// These fixtures are actual gzip/tar byte streams with an independently
// authored normalized manifest, not mocks of the metadata parser.
function archive(manifest = manifestBase(), entries = [], options = {}) {
  const prefix = options.prefix ?? `${NAME}-${VERSION}/`;
  const members = [{ path: `${prefix}Cargo.toml`, body: manifest }, { path: `${prefix}README.md`, body: "# Fixture\nUnicode 玉\n" },
    { path: `${prefix}LICENSE.txt`, body: "Fixture license\n" }, { path: `${prefix}src/lib.rs`, body: "pub fn fixture() {}\n" }, ...entries];
  return python(String.raw`
import gzip, io, json, sys, tarfile
data = json.load(sys.stdin)
out = io.BytesIO()
with tarfile.open(fileobj=out, mode='w', format=tarfile.USTAR_FORMAT) as archive:
    for item in data:
        member = tarfile.TarInfo(item['path'])
        member.mtime = 0
        body = item.get('body', '').encode('utf-8')
        if item.get('type') == 'symlink':
            member.type = tarfile.SYMTYPE
            member.linkname = item['linkname']
        elif item.get('type') == 'hardlink':
            member.type = tarfile.LNKTYPE
            member.linkname = item['linkname']
        elif item.get('type') == 'device':
            member.type = tarfile.CHRTYPE
        else:
            member.size = len(body)
        archive.addfile(member, io.BytesIO(body))
sys.stdout.buffer.write(gzip.compress(out.getvalue(), mtime=0))
`, JSON.stringify(members));
}
function manifestBase(extra = "") {
  return `[package]\nname="${NAME}"\nversion="${VERSION}"\nedition="2021"\nauthors=["Fixture Author"]\ndescription="Upload fixture"\ndocumentation="https://example.invalid/docs"\nhomepage="https://example.invalid"\nreadme="README.md"\nkeywords=["fixture"]\ncategories=["development-tools"]\nlicense="MIT"\nlicense-file="LICENSE.txt"\nrepository="https://example.invalid/repo"\nlinks="fixture-native"\nrust-version="1.95"\n${extra}`;
}
const digest = bytes => crypto.createHash("sha256").update(bytes).digest("hex");

test("archive metadata preserves package fields and reads README/license only from archive bytes", () => {
  const bytes = archive();
  const data = describeArchive(bytes, NAME, VERSION);
  assert.deepEqual(data, { name: NAME, vers: VERSION, deps: [], features: {}, authors: ["Fixture Author"], keywords: ["fixture"],
    categories: ["development-tools"], readme: "# Fixture\nUnicode 玉\n", readme_file: "README.md", license_file: "LICENSE.txt", badges: {},
    description: "Upload fixture", documentation: "https://example.invalid/docs", homepage: "https://example.invalid", license: "MIT",
    repository: "https://example.invalid/repo", links: "fixture-native", rust_version: "1.95" });
  assert.equal(data.edition, undefined); // Cargo's NewCrate schema carries edition in the archive, not this JSON.
});

test("normal, dev, build and target dependencies retain rename, features and exact/caret semantics", () => {
  const data = describeArchive(archive(manifestBase(`
[dependencies.renamed]
package="actual-name"
version="=1.2.3"
optional=true
default-features=false
features=["serde"]
[dev-dependencies]
dev-only="2.1"
[build-dependencies.build-only]
version="~3.0"
[target.'cfg(windows)'.build-dependencies]
windows-only={version="4.*",features=["api"]}
[target.'cfg(unix)'.dev-dependencies]
unix-only={version=">=1, <2"}
[features]
default=["renamed?/serde"]
explicit=["dep:renamed"]
`)), NAME, VERSION);
  assert.deepEqual(data.deps, [
    { name: "actual-name", version_req: "=1.2.3", features: ["serde"], optional: true, default_features: false, target: null, kind: "normal", explicit_name_in_toml: "renamed" },
    { name: "dev-only", version_req: "^2.1", features: [], optional: false, default_features: true, target: null, kind: "dev" },
    { name: "build-only", version_req: "~3.0", features: [], optional: false, default_features: true, target: null, kind: "build" },
    { name: "windows-only", version_req: "4.*", features: ["api"], optional: false, default_features: true, target: "cfg(windows)", kind: "build" },
    { name: "unix-only", version_req: ">=1, <2", features: [], optional: false, default_features: true, target: "cfg(unix)", kind: "dev" },
  ]);
  assert.deepEqual(data.features, { default: ["renamed?/serde"], explicit: ["dep:renamed"] });
});

test("implicit optional features remain implicit, while existing explicit feature tables are unchanged", () => {
  for (const features of ["", '[features]\nrenamed=["renamed?/extra"]\n', '[features]\nother=["dep:renamed"]\n']) {
    const data = describeArchive(archive(manifestBase('[dependencies.renamed]\npackage="actual-name"\nversion="1"\noptional=true\n' + features)), NAME, VERSION);
    assert.equal(data.deps[0].optional, true);
    assert.deepEqual(data.features, !features ? {} : features.includes('other=') ? { other: ["dep:renamed"] } : { renamed: ["renamed?/extra"] });
  }
});

test("unresolved or unsupported registry/source dependency fields fail closed", () => {
  for (const field of ['path="../outside"', 'git="https://example.invalid/git"', 'registry="private"', 'registry-index="https://example.invalid/index"', 'artifact="bin"', 'workspace=true']) {
    assert.throws(() => describeArchive(archive(manifestBase(`[dependencies.bad]\nversion="1"\n${field}\n`)), NAME, VERSION), /Invalid or unsupported/);
  }
  assert.throws(() => describeArchive(archive(manifestBase('publish=false\n')), NAME, VERSION), /Invalid or unsupported/);
});

test("archive identity, duplicate/escaping paths, links and devices are rejected without extracting", () => {
  const unsafe = [
    { path: `${NAME}-${VERSION}/Cargo.toml`, body: manifestBase() },
    { path: `${NAME}-${VERSION}/../escape`, body: "x" },
    { path: `${NAME}-${VERSION}/./alias`, body: "x" },
    { path: `${NAME}-${VERSION}/nested//alias`, body: "x" },
    { path: `${NAME}-${VERSION}/nested\\alias`, body: "x" },
    { path: `${NAME}-${VERSION}/C:/escape`, body: "x" },
    { path: "/absolute", body: "x" },
    { path: `${NAME}-${VERSION}/link`, type: "symlink", linkname: "/outside" },
    { path: `${NAME}-${VERSION}/hard`, type: "hardlink", linkname: `${NAME}-${VERSION}/README.md` },
    { path: `${NAME}-${VERSION}/device`, type: "device" },
  ];
  for (const entry of unsafe) assert.throws(() => describeArchive(archive(manifestBase(), [entry]), NAME, VERSION), /Invalid or unsupported/);
  assert.throws(() => describeArchive(archive(), "another-name", VERSION), /Invalid or unsupported/);
  assert.throws(() => describeArchive(archive(), NAME, "1.2.4"), /Invalid or unsupported/);
  assert.throws(() => describeArchive(Buffer.from("not gzip"), NAME, VERSION), /Invalid or unsupported/);
  assert.throws(() => describeArchive(Buffer.concat([archive(), archive()]), NAME, VERSION), /Invalid or unsupported/);
  assert.throws(() => describeArchive(Buffer.alloc(0), NAME, VERSION), /size/);
  assert.throws(() => describeArchive(Buffer.alloc(64 * 1024 * 1024 + 1), NAME, VERSION), /size/);
  assert.throws(() => describeArchive(archive(), "../escape", VERSION), /identity/);
});

test("README/license paths cannot read host files and metadata/archive expansion is bounded", () => {
  for (const value of ['"../../outside"', '"/etc/passwd"', '"missing.md"', '"bad\\u0000path"', 'true']) {
    assert.throws(() => describeArchive(archive(manifestBase().replace('readme="README.md"', `readme=${value}`)), NAME, VERSION), /Invalid or unsupported/);
  }
  assert.throws(() => describeArchive(archive(manifestBase().replace('license-file="LICENSE.txt"', 'license-file="missing"')), NAME, VERSION), /Invalid or unsupported/);
  assert.throws(() => describeArchive(archive(manifestBase().replace('readme="README.md"', 'readme="huge-readme"'),
    [{ path: `${NAME}-${VERSION}/huge-readme`, body: "a".repeat(1024 * 1024 + 1) }]), NAME, VERSION), /Invalid or unsupported/);
  const noReadme = describeArchive(archive(manifestBase().replace('readme="README.md"', 'readme=false')), NAME, VERSION);
  assert.equal(noReadme.readme, null);
  assert.equal(noReadme.readme_file, null);
});

test("parser child receives only PATH/HOME, not dummy credentials or source import hooks", t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo-upload-env-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const actualPython = python("import sys; print(sys.executable)").toString().trim();
  const recorded = path.join(root, "child-env");
  fs.writeFileSync(path.join(root, "python3"), `#!/bin/sh\n/usr/bin/env > '${recorded}'\nexec '${actualPython}' "$@"\n`, { mode: 0o700 });
  fs.writeFileSync(path.join(root, "tomllib.py"), 'raise RuntimeError("Source import must never execute")\n');
  const bytes = archive();
  const script = `const h=require(${JSON.stringify(path.join(__dirname, "release-cargo-upload.cjs"))});h.describeArchive(Buffer.from(process.argv[1],"base64"),"${NAME}","${VERSION}");`;
  const result = spawnSync(process.execPath, ["-e", script, bytes.toString("base64")], { cwd: root, encoding: "utf8",
    env: { ...env(), PATH: root + path.delimiter + process.env.PATH, CARGO_REGISTRY_TOKEN: DUMMY_TOKEN, BAMBOO_RELEASE_TOKEN: "dummy-github-token",
      BAMBOO_RELEASE_RECEIPT_KEY: "dummy-hmac-key", PYTHONPATH: root } });
  assert.equal(result.status, 0, result.stderr);
  const values = fs.readFileSync(recorded, "utf8");
  assert.doesNotMatch(values, /dummy-|CARGO_REGISTRY_TOKEN|BAMBOO_RELEASE|PYTHONPATH/);
  assert.ok(values.includes("PATH="));
});

test("local HTTP capture verifies production URL, PUT headers, exact framing and unchanged archive checksum", async t => {
  const bytes = archive(); const metadata = describeArchive(bytes, NAME, VERSION); const before = digest(bytes);
  let received;
  const server = http.createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(chunk);
    received = { method: request.method, headers: request.headers, body: Buffer.concat(chunks) };
    response.writeHead(200, { "Content-Type": "application/json" }); response.end('{"warnings":{}}');
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const result = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async (url, options) => {
    assert.equal(url, ENDPOINT); assert.equal(options.redirect, "error");
    const response = await fetch(`http://127.0.0.1:${server.address().port}/capture`, options);
    return new Response(await response.arrayBuffer(), { status: response.status });
  } });
  assert.equal(result.status, 0);
  assert.equal(received.method, "PUT");
  assert.equal(received.headers.authorization, DUMMY_TOKEN);
  assert.equal(received.headers["content-type"], "application/octet-stream");
  assert.equal(received.headers.accept, "application/json");
  const jsonLength = received.body.readUInt32LE(0);
  assert.deepEqual(JSON.parse(received.body.subarray(4, 4 + jsonLength)), metadata);
  const archiveOffset = 8 + jsonLength;
  assert.equal(received.body.readUInt32LE(4 + jsonLength), bytes.length);
  assert.equal(received.body.length, archiveOffset + bytes.length);
  assert.deepEqual(received.body.subarray(archiveOffset), bytes);
  assert.equal(digest(bytes), before);
});

test("request holds immutable archive bytes across asynchronous transport", async () => {
  const bytes = archive(); const original = Buffer.from(bytes); const metadata = describeArchive(bytes, NAME, VERSION);
  const result = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async (_url, options) => {
    bytes.fill(0);
    await Promise.resolve();
    const n = options.body.readUInt32LE(0);
    assert.deepEqual(options.body.subarray(n + 8), original);
    return new Response('{}');
  } });
  assert.equal(result.status, 0);
});

test("HTTP/API/redirect/transport failures are nonzero, bounded and never print response secrets", async () => {
  const bytes = archive(); const metadata = describeArchive(bytes, NAME, VERSION);
  const cases = [
    [() => new Response('{"errors":[{"detail":"bad ' + DUMMY_TOKEN + '"}]}', { status: 200 }), /HTTP 200/],
    [() => new Response('{}', { status: 302 }), /HTTP 302/],
    [() => Object.defineProperty(new Response('{}'), "redirected", { value: true }), /redirect/],
    [() => Object.defineProperty(new Response('{}'), "url", { value: "https://example.invalid/redirect" }), /redirect/],
    [() => new Response(DUMMY_TOKEN, { status: 403 }), /validation/],
    [() => new Response('{"errors":"' + DUMMY_TOKEN + '"}'), /HTTP 200/],
    [() => new Response('x'.repeat(65537)), /validation/],
    [() => { throw new Error(DUMMY_TOKEN); }, /transport/],
    [() => new Response('[]'), /validation/],
  ];
  for (const [respond, expected] of cases) {
    const result = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => respond() });
    assert.notEqual(result.status, 0); assert.match(result.output, expected); assert.ok(!result.output.includes(DUMMY_TOKEN));
  }
  const retry = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => new Response(DUMMY_TOKEN, { status: 429 }) });
  assert.equal(retry.status, 1); assert.match(retry.output, /429 Too Many Requests/); assert.ok(!retry.output.includes(DUMMY_TOKEN));
  const duplicate = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => new Response('{"errors":[{"detail":"crate version is already uploaded"}]}', { status: 400 }) });
  assert.equal(duplicate.status, 1); assert.match(duplicate.output, /already exists on crates.io index/);
});

test("invalid token and oversized metadata fail before any request", async () => {
  let calls = 0;
  const fetchImpl = async () => { calls++; throw new Error("must not request"); };
  const bytes = archive(); const metadata = describeArchive(bytes, NAME, VERSION);
  for (const token of ["", "dummy\nheader", null]) assert.equal((await uploadArchive(bytes, metadata, token, { fetchImpl })).status, 1);
  assert.equal((await uploadArchive(bytes, { ...metadata, description: "x".repeat(8 * 1024 * 1024) }, DUMMY_TOKEN, { fetchImpl })).status, 1);
  assert.equal(calls, 0);
});

test("429 preserves only canonical body or Retry-After cooldown dates for existing outer retries", async t => {
  const bytes = archive(); const metadata = describeArchive(bytes, NAME, VERSION);
  const now = Date.UTC(2026, 9, 10, 12, 0, 0);
  t.mock.method(Date, "now", () => now);
  const date = "Sat, 10 Oct 2026 12:05:00 GMT";
  const responses = [
    new Response(JSON.stringify({ errors: [{ detail: `Please try again after ${date}; private echo ${DUMMY_TOKEN}` }] }), { status: 429 }),
    new Response(DUMMY_TOKEN, { status: 429, headers: { "Retry-After": date } }),
    new Response(DUMMY_TOKEN, { status: 429, headers: { "Retry-After": "300" } }),
    new Response(`try again after ${date}`, { status: 429, headers: { "Retry-After": DUMMY_TOKEN } }),
  ];
  for (const response of responses) {
    const result = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => response });
    assert.equal(result.status, 1);
    assert.equal(result.output, `HTTP 429 Too Many Requests from crates.io; try again after ${date}`);
    assert.equal(result.output.match(/try again after ([A-Za-z0-9:, ]*GMT)/i)?.[1], date);
    // The controller adds 10 seconds and applies its existing bounds.
    assert.equal(Math.max(15, Math.min(1200, Math.ceil((Date.parse(date) - Date.now()) / 1000) + 10)), 310);
    assert.ok(!result.output.includes(DUMMY_TOKEN));
  }
  for (const value of ["not a date", "Mon, 30 Feb 2026 12:05:00 GMT", "Fri, 10 Oct 2026 12:05:00 GMT", `300 ${DUMMY_TOKEN}`, "999999999999999999999999"]) {
    const result = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => new Response(`try again after ${value}`, { status: 429, headers: { "Retry-After": value } }) });
    assert.equal(result.output, "HTTP 429 Too Many Requests from crates.io");
  }
  const oversized = await uploadArchive(bytes, metadata, DUMMY_TOKEN, { fetchImpl: async () => new Response("x".repeat(65537) + DUMMY_TOKEN, { status: 429 }) });
  assert.equal(oversized.output, "HTTP 429 Too Many Requests from crates.io");
});
