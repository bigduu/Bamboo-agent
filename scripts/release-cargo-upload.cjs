"use strict";

// Trusted upload code: never invoke Cargo, source code or a credential provider.
// Wire format: https://doc.rust-lang.org/cargo/reference/registry-web-api.html#publish
const { spawnSync } = require("node:child_process");
const MAX_ARCHIVE_BYTES = 64 * 1024 * 1024;
const MAX_METADATA_BYTES = 8 * 1024 * 1024;
const ENDPOINT = "https://crates.io/api/v1/crates/new";

// -I excludes source-directory/PYTHONPATH/user-site imports. Only the fixed
// standard-library parser receives archive bytes, never the parent's tokens.
const PARSER = String.raw`
import gzip, io, json, re, sys, tarfile, tomllib

def require(value):
    if not value:
        raise ValueError()

def text(value, limit=65536):
    require(isinstance(value, str) and len(value.encode('utf-8')) <= limit and chr(0) not in value)
    return value

def strings(value):
    require(isinstance(value, list) and len(value) <= 10000)
    return [text(item) for item in value]

def boolean(value):
    require(isinstance(value, bool))
    return value

def relative(value):
    text(value)
    require(value and not value.startswith('/') and chr(92) not in value and not re.match(r'^[A-Za-z]:', value))
    require(all(part not in ('', '.', '..') for part in value.split('/')))
    return value

def main():
    name, version = sys.argv[1:]
    prefix = name + '-' + version + '/'
    compressed = sys.stdin.buffer.read(64 * 1024 * 1024 + 1)
    require(0 < len(compressed) <= 64 * 1024 * 1024)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(256 * 1024 * 1024 + 1)
    require(len(raw) <= 256 * 1024 * 1024)
    files, seen = {}, set()
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        for member in archive:
            require(len(seen) < 20000)
            require(member.name.startswith(prefix) and not member.issparse())
            rel = member.name[len(prefix):].removesuffix('/') if member.isdir() else member.name[len(prefix):]
            relative(rel)
            require(rel not in seen)
            seen.add(rel)
            require(member.isfile() or member.isdir())
            if member.isfile():
                require(0 <= member.size <= 256 * 1024 * 1024)
                files[rel] = archive.extractfile(member).read()
        require(not any(raw[archive.offset:]))
    require('Cargo.toml' in files and len(files['Cargo.toml']) <= 1024 * 1024)
    doc = tomllib.loads(files['Cargo.toml'].decode('utf-8'))
    package = doc['package']
    require(package['name'] == name and package['version'] == version and 'workspace' not in doc)
    require(package.get('publish', True) is True or package.get('publish') == ['crates-io'])
    deps = []
    tables = [(kind, None, doc.get(table, {})) for table, kind in
              [('dependencies', 'normal'), ('dev-dependencies', 'dev'), ('build-dependencies', 'build')]]
    for target, tablespec in doc.get('target', {}).items():
        text(target)
        require(isinstance(tablespec, dict))
        tables.extend((kind, target, tablespec.get(table, {})) for table, kind in
                      [('dependencies', 'normal'), ('dev-dependencies', 'dev'), ('build-dependencies', 'build')])
    for kind, target, table in tables:
        require(isinstance(table, dict))
        for alias, spec in table.items():
            require(re.fullmatch(r'[A-Za-z0-9_-]+', alias))
            if isinstance(spec, str):
                spec = {'version': spec}
            require(isinstance(spec, dict) and set(spec) <= {'version', 'package', 'features', 'optional', 'default-features'})
            req = text(spec['version'])
            require(req.strip() == req and req)
            # Cargo's VersionReq display makes an unadorned version a caret
            # requirement. Explicit operators/wildcards retain their meaning.
            if re.fullmatch(r'[0-9]+(?:\.[0-9]+){0,2}(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?', req):
                req = '^' + req
            actual = text(spec.get('package', alias))
            require(re.fullmatch(r'[A-Za-z0-9_-]+', actual))
            optional = boolean(spec.get('optional', False))
            dep = {'name': actual, 'version_req': req, 'features': strings(spec.get('features', [])),
                   'optional': optional, 'default_features': boolean(spec.get('default-features', True)),
                   'target': target, 'kind': kind}
            if actual != alias:
                dep['explicit_name_in_toml'] = alias
            deps.append(dep)
    features = doc.get('features', {})
    require(isinstance(features, dict))
    features = {text(key): strings(values) for key, values in features.items()}
    # Match Cargo prepare_transmit: send the normalized table unchanged. Cargo
    # consumers derive implicit optional-dependency features themselves.
    readme = package.get('readme')
    require(readme is None or readme is False or isinstance(readme, str))
    readme_file = relative(readme) if isinstance(readme, str) else None
    require(readme_file is None or readme_file in files)
    readme_text = text(files[readme_file].decode('utf-8'), 1024 * 1024) if readme_file else None
    license_file = package.get('license-file')
    if license_file is not None:
        relative(license_file)
        require(license_file in files)
    badges = doc.get('badges', {})
    require(isinstance(badges, dict))
    badges = {text(k): {text(a): text(v) for a, v in spec.items()} for k, spec in badges.items()}
    metadata = {'name': name, 'vers': version, 'deps': deps, 'features': features,
                'authors': strings(package.get('authors', [])), 'keywords': strings(package.get('keywords', [])),
                'categories': strings(package.get('categories', [])), 'readme': readme_text,
                'readme_file': readme_file, 'license_file': license_file, 'badges': badges}
    for key, toml_key in [('description', 'description'), ('documentation', 'documentation'), ('homepage', 'homepage'),
                          ('license', 'license'), ('repository', 'repository'), ('links', 'links'), ('rust_version', 'rust-version')]:
        value = package.get(toml_key)
        metadata[key] = text(value) if value is not None else None
    result = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
    require(len(result.encode('utf-8')) <= 8 * 1024 * 1024)
    print(result)

try:
    main()
except Exception:
    sys.exit('Invalid or unsupported crate archive')
`;

function archiveBytes(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length === 0 || bytes.length > MAX_ARCHIVE_BYTES) {
    throw new Error("Invalid crate archive size");
  }
  return bytes;
}
function describeArchive(bytes, name, version) {
  archiveBytes(bytes);
  if (typeof name !== "string" || !/^[A-Za-z0-9_-]{1,64}$/.test(name)
    || typeof version !== "string" || !/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error("Invalid expected crate identity");
  }
  const env = Object.fromEntries(["PATH", "HOME"].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const result = spawnSync("python3", ["-I", "-c", PARSER, name, version], {
    input: bytes, env, encoding: "utf8", maxBuffer: MAX_METADATA_BYTES + 1024, timeout: 30_000,
  });
  if (result.error || result.status !== 0) throw new Error("Invalid or unsupported crate archive (Python 3.11+ required)");
  return JSON.parse(result.stdout);
}

async function responseText(response) {
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > 64 * 1024) throw new Error("Response limit");
      chunks.push(Buffer.from(value));
    }
  } catch (error) { await reader.cancel().catch(() => {}); throw error; }
  return Buffer.concat(chunks).toString("utf8");
}
async function responseBody(response) {
  const text = await responseText(response);
  const data = text ? JSON.parse(text) : {};
  if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error("Invalid response");
  return data;
}

function validHttpDate(value) {
  if (typeof value !== "string" || !/^[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT$/i.test(value)) return null;
  const time = Date.parse(value);
  const canonical = Number.isFinite(time) ? new Date(time).toUTCString() : null;
  // Round-trip rejects invalid calendar dates and mismatched weekday fields.
  return canonical?.toLowerCase() === value.toLowerCase() ? canonical : null;
}
async function cooldownHint(response) {
  const header = response.headers?.get("retry-after");
  let date = validHttpDate(header);
  if (!date && typeof header === "string" && /^\d{1,10}$/.test(header) && Number(header) <= 2147483647) {
    date = new Date(Date.now() + Number(header) * 1000).toUTCString();
  }
  if (date) {
    await response.body?.cancel().catch(() => {});
  } else {
    try {
      const body = await responseText(response);
      const advertised = body.match(/try again after ([A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT)\b/i)?.[1];
      date = validHttpDate(advertised);
    } catch { /* Malformed/oversized bodies retain the existing 120s fallback. */ }
  }
  // Only a validated, regenerated timestamp crosses the log boundary; never
  // body/header text. Both existing callers apply their own 15..1200s clamp.
  return date ? `; try again after ${date}` : "";
}

async function uploadArchive(bytes, metadata, token, { fetchImpl = global.fetch } = {}) {
  try {
    // Copy before the first await; later filesystem or Buffer changes cannot
    // replace the verified/reserved archive bytes in this request.
    const archive = Buffer.from(archiveBytes(bytes));
    if (!metadata || typeof metadata !== "object" || typeof metadata.name !== "string" || typeof metadata.vers !== "string"
      || typeof token !== "string" || !/^[\x21-\x7e]+$/.test(token)) return { status: 1, output: "Invalid upload inputs" };
    const json = Buffer.from(JSON.stringify(metadata), "utf8");
    if (json.length > MAX_METADATA_BYTES) return { status: 1, output: "Invalid upload metadata size" };
    const first = Buffer.alloc(4); first.writeUInt32LE(json.length);
    const second = Buffer.alloc(4); second.writeUInt32LE(archive.length);
    const body = Buffer.concat([first, json, second, archive]);
    const response = await fetchImpl(ENDPOINT, {
      method: "PUT", redirect: "error", signal: AbortSignal.timeout(120_000),
      headers: { Authorization: token, "Content-Type": "application/octet-stream", Accept: "application/json",
        "User-Agent": "bamboo-release-cargo-upload/1 (+https://github.com/bigduu/Bamboo-agent)", "Content-Length": String(body.length) }, body,
    });
    if (!Number.isInteger(response.status) || response.status < 100 || response.status > 599) return { status: 1, output: "Invalid registry response status" };
    if (response.redirected || (response.url && response.url !== ENDPOINT)) return { status: 1, output: "Registry upload redirect rejected" };
    if (response.status === 429) {
      return { status: 1, output: "HTTP 429 Too Many Requests from crates.io" + await cooldownHint(response) };
    }
    const data = await responseBody(response);
    const errors = data.errors;
    if (response.status < 200 || response.status >= 300 || (errors !== undefined && (!Array.isArray(errors) || errors.length))) {
      // Do not print response bodies, headers or transport exceptions: they may
      // echo authorization values. Only recognized duplicate state is exposed.
      const duplicate = Array.isArray(errors) && errors.some(error => typeof error?.detail === "string"
        && /already (?:exists|uploaded|been uploaded)|previously uploaded/i.test(error.detail));
      return { status: 1, output: duplicate ? "crate version already exists on crates.io index"
        : `HTTP ${Number.isInteger(response.status) ? response.status : "error"} crates.io upload failed` };
    }
    return { status: 0, output: "Uploaded verified crate archive to crates.io" };
  } catch { return { status: 1, output: "crates.io upload transport or response validation failed" }; }
}

module.exports = { describeArchive, uploadArchive };
