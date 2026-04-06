#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const OUTPUT_DIR = path.join(ROOT, "frontend_package");
const OUTPUT_ZIP = path.join(OUTPUT_DIR, "lotus-frontend.zip");
const OUTPUT_MANIFEST = path.join(OUTPUT_DIR, "frontend-manifest.json");
const SOURCE_MODE = (process.env.LOTUS_SOURCE || "auto").toLowerCase();
const LOCAL_PATH = path.resolve(ROOT, process.env.LOTUS_LOCAL_PATH || "../lotus");
const PACKAGE_NAME = process.env.LOTUS_PACKAGE_NAME || "@bigduu/lotus";
const PREBUILT_DIST_DIR = process.env.LOTUS_DIST_DIR
  ? path.resolve(ROOT, process.env.LOTUS_DIST_DIR)
  : path.resolve(ROOT, "../lotus/dist");

function fail(message) {
  console.error(`❌ ${message}`);
  process.exit(1);
}

function dirExists(target) {
  try {
    return fs.statSync(target).isDirectory();
  } catch {
    return false;
  }
}

function localLotusExists() {
  return dirExists(LOCAL_PATH) && fs.existsSync(path.join(LOCAL_PATH, "package.json"));
}

function resolvePackageRoot() {
  try {
    const pkgJsonPath = require.resolve(`${PACKAGE_NAME}/package.json`, {
      paths: [ROOT],
    });
    return path.dirname(pkgJsonPath);
  } catch {
    return null;
  }
}

function runNpmScript(prefix, script) {
  const isWindows = process.platform === "win32";
  let result = spawnSync("npm", ["run", script], {
    stdio: "inherit",
    env: process.env,
    cwd: prefix,
    shell: isWindows,
  });

  if (typeof result.status === "number") {
    if (result.status !== 0) process.exit(result.status);
    return;
  }

  if (isWindows) {
    const fallbackCommand = `npm run ${script}`;
    result = spawnSync(fallbackCommand, {
      stdio: "inherit",
      env: process.env,
      cwd: prefix,
      shell: true,
    });
    if (typeof result.status === "number") {
      if (result.status !== 0) process.exit(result.status);
      return;
    }
  }

  fail(`Failed to run npm script "${script}" in ${prefix}`);
}

function resolveSource() {
  if (!["auto", "local", "package"].includes(SOURCE_MODE)) {
    fail(`Invalid LOTUS_SOURCE="${SOURCE_MODE}" (expected auto|local|package)`);
  }

  const localAvailable = localLotusExists();
  const packageRoot = resolvePackageRoot();

  if (SOURCE_MODE === "local") {
    if (!localAvailable) {
      fail(
        `LOTUS_SOURCE=local but local Lotus not found at ${LOCAL_PATH}. Set LOTUS_LOCAL_PATH or use LOTUS_SOURCE=package.`,
      );
    }
    return { mode: "local", packageRoot: null };
  }

  if (SOURCE_MODE === "package") {
    if (!packageRoot) {
      fail(
        `LOTUS_SOURCE=package but package "${PACKAGE_NAME}" is not installed. Install it or use LOTUS_SOURCE=local.`,
      );
    }
    return { mode: "package", packageRoot };
  }

  if (localAvailable) {
    return { mode: "local", packageRoot: null };
  }
  if (packageRoot) {
    return { mode: "package", packageRoot };
  }

  fail(
    `No Lotus source found. Expected local checkout at ${LOCAL_PATH} or installed package "${PACKAGE_NAME}".`,
  );
}

function listFilesRecursively(rootDir) {
  const files = [];

  const walk = (currentDir) => {
    const entries = fs.readdirSync(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const absolutePath = path.join(currentDir, entry.name);
      const relativePath = path.relative(rootDir, absolutePath).replace(/\\/g, "/");
      if (entry.isDirectory()) {
        walk(absolutePath);
      } else if (entry.isFile()) {
        files.push({ absolutePath, relativePath });
      }
    }
  };

  walk(rootDir);
  return files.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
}

function computeDirectoryHash(rootDir) {
  const hash = crypto.createHash("sha256");
  for (const file of listFilesRecursively(rootDir)) {
    hash.update(file.relativePath);
    hash.update("\0");
    hash.update(fs.readFileSync(file.absolutePath));
    hash.update("\0");
  }
  return `sha256:${hash.digest("hex")}`;
}

function resolveLotusVersion(source) {
  const pkgPath =
    source.mode === "local"
      ? path.join(LOCAL_PATH, "package.json")
      : path.join(source.packageRoot, "package.json");
  return JSON.parse(fs.readFileSync(pkgPath, "utf8")).version;
}

function resolveDistDir(source) {
  if (source.mode === "local") {
    console.log(`ℹ️ Using local Lotus at ${LOCAL_PATH}`);
    runNpmScript(LOCAL_PATH, "build");
    return path.join(LOCAL_PATH, "dist");
  }

  console.log(`ℹ️ Using packaged Lotus "${PACKAGE_NAME}" at ${source.packageRoot}`);
  return path.join(source.packageRoot, "dist");
}

function resolvePrebuiltDistDir() {
  if (!dirExists(PREBUILT_DIST_DIR)) {
    fail(`Prebuilt Lotus dist directory not found: ${PREBUILT_DIST_DIR}`);
  }
  return PREBUILT_DIST_DIR;
}

function resolvePrebuiltLotusVersion() {
  const pkgPath = path.join(LOCAL_PATH, "package.json");
  if (!fs.existsSync(pkgPath)) {
    fail(`Cannot resolve Lotus version for prebuilt dist; missing package.json at ${pkgPath}`);
  }
  return JSON.parse(fs.readFileSync(pkgPath, "utf8")).version;
}

function resetOutputDir() {
  fs.rmSync(OUTPUT_DIR, { recursive: true, force: true });
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function writeManifest(stageDir, version) {
  const manifest = {
    schema_version: 1,
    frontend_name: "lotus",
    frontend_version: version,
    bundle_hash: computeDirectoryHash(stageDir),
    built_at: new Date().toISOString(),
    entry: "index.html",
  };
  const content = `${JSON.stringify(manifest, null, 2)}\n`;
  fs.writeFileSync(path.join(stageDir, "frontend-manifest.json"), content);
  fs.writeFileSync(OUTPUT_MANIFEST, content);
  return manifest;
}

function createZipFromStage(stageDir) {
  const isWindows = process.platform === "win32";
  const zipName = path.basename(OUTPUT_ZIP);
  const args = isWindows ? ["a", "-tzip", zipName, "."] : ["-r", zipName, "."];

  const result = isWindows
    ? spawnSync("7z", args, { cwd: stageDir, stdio: "inherit", env: process.env, shell: true })
    : spawnSync("zip", args, { cwd: stageDir, stdio: "inherit", env: process.env });

  if (result.status !== 0) {
    fail(`Failed to create frontend zip package from ${stageDir}`);
  }

  fs.renameSync(path.join(stageDir, zipName), OUTPUT_ZIP);
}

function stagePackageFromDist(distDir, version) {
  if (!dirExists(distDir)) {
    fail(`Lotus dist directory not found: ${distDir}`);
  }

  resetOutputDir();

  const stageDir = path.join(OUTPUT_DIR, ".stage");
  fs.mkdirSync(stageDir, { recursive: true });
  fs.cpSync(distDir, stageDir, { recursive: true });

  const manifest = writeManifest(stageDir, version);
  createZipFromStage(stageDir);
  fs.rmSync(stageDir, { recursive: true, force: true });

  console.log(`✅ Created embedded frontend package: ${OUTPUT_ZIP}`);
  console.log(`✅ Wrote embedded frontend manifest: ${OUTPUT_MANIFEST}`);
  console.log(`ℹ️ Embedded frontend version: ${manifest.frontend_version}`);
  console.log(`ℹ️ Embedded frontend hash: ${manifest.bundle_hash}`);
}

function stagePackage() {
  const source = resolveSource();
  stagePackageFromDist(resolveDistDir(source), resolveLotusVersion(source));
}

function stagePrebuiltPackage() {
  stagePackageFromDist(resolvePrebuiltDistDir(), resolvePrebuiltLotusVersion());
}

function printInfo() {
  const localAvailable = localLotusExists();
  const packageRoot = resolvePackageRoot();
  console.log(`LOTUS_SOURCE=${SOURCE_MODE}`);
  console.log(`LOTUS_LOCAL_PATH=${LOCAL_PATH} (${localAvailable ? "found" : "missing"})`);
  console.log(
    `LOTUS_PACKAGE_NAME=${PACKAGE_NAME} (${packageRoot ? `found at ${packageRoot}` : "missing"})`,
  );
}

const command = process.argv[2] || "stage";
if (command === "stage") {
  stagePackage();
  process.exit(0);
}
if (command === "stage:prebuilt") {
  stagePrebuiltPackage();
  process.exit(0);
}
if (command === "info") {
  printInfo();
  process.exit(0);
}

fail(`Unknown command "${command}". Use one of: stage, stage:prebuilt, info.`);
