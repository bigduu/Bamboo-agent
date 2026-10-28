"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const FILES = [
  "scripts/supervisor-native-acceptance.test.cjs",
  "scripts/lotus-next-artifact.test.cjs",
  "scripts/release-frontend-policy.test.cjs",
];
const CASES = [
  "bounded nofollow file reader and artifact hashing use actual bytes",
  "artifact preflight verifies canonical temp paths and actual bytes while remaining native UNRUN",
  "OS COMPONENT ONLY: bind the test process/listener and reject changed process identity before a write",
  "OS COMPONENT ONLY: an IPv6 owner cannot attest a different IPv4 listener on the same port",
  "OS COMPONENT ONLY: GET evidence rejects a replacement after the original owned process exits",
  "OS COMPONENT ONLY: port transfer after precheck must not receive an HTTP write",
  "a rejected pinned input leaves the last committed package byte-for-byte intact",
  "historical crate publication LF guard rejects a moved, mismatched or dirty source before package work",
  "historical crate publication CRLF guard rejects a moved, mismatched or dirty source before package work",
];
const pattern = "^(?:" + CASES.map(name => name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|") + ")$";

function verifyTap(output) {
  output = output.replace(/\r\n/g, "\n");
  const passed = [...output.matchAll(/^ok [0-9]+ - (.+)$/gm)].map(match => match[1]);
  assert.deepEqual(passed.sort(), [...CASES].sort(), "Native Node cases were missing, duplicated, skipped or changed");
  for (const [field, expected] of Object.entries({ tests: 9, pass: 9, fail: 0, cancelled: 0, skipped: 0, todo: 0 })) {
    const values = [...output.matchAll(new RegExp("^# " + field + " ([0-9]+)$", "gm"))];
    assert.equal(values.length, 1, "Missing or ambiguous TAP total: " + field);
    assert.equal(Number(values[0][1]), expected, "Unexpected native Node total: " + field);
  }
}

function run(argv, { spawn = spawnSync, stdout = process.stdout, stderr = process.stderr } = {}) {
  try {
    assert.deepEqual(argv, FILES, "Native Node files must match the reviewed selection");
    const result = spawn(process.execPath, ["--test", "--test-reporter=tap", "--test-name-pattern=" + pattern, ...argv], {
      cwd: path.resolve(__dirname, ".."), encoding: "utf8", maxBuffer: 16 * 1024 * 1024,
    });
    stdout.write(result.stdout || "");
    stderr.write(result.stderr || "");
    if (result.error) throw result.error;
    if (result.signal || result.status !== 0) return result.status || 1;
    verifyTap(result.stdout);
    stdout.write("Native Node selection: all 9 required OS cases passed.\n");
    return 0;
  } catch (error) {
    stderr.write("Native Node selection failed: " + error.message + "\n");
    return 1;
  }
}

module.exports = { FILES, CASES, verifyTap, run };
if (require.main === module) process.exitCode = run(process.argv.slice(2));
