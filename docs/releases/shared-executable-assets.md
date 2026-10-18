# Shared Bamboo executables

The automatic `Publish Crate` workflow publishes the same complete Bamboo
executable for standalone users and desktop consumers. Its frontend build mode
is `embedded`, with the normal bundled Lotus frontend. There is no separate
API-only desktop build. Bodhi can retain its separately locked Tauri frontend
and select it with `bamboo serve --static-dir`; that explicit directory takes
priority over the embedded frontend. Desktop acceptance must check the real
executable's version, health endpoints, static asset bytes and SPA fallback.

Publication retains the successful same-repository main-push CI prerequisite,
the protected-source authorization, the existing version allocator and crate
validation, and the single `bamboo-crate-publication` queue. The executable jobs
run after that allocator has completed the official crate Release. They stamp
all local crates with its existing version, reject external dependency lock
changes, restore the exact frontend assets preserved in that Release, and build
the standard release executable once on each native runner:

| Target | Runner | Raw executable asset |
| --- | --- | --- |
| `aarch64-apple-darwin` | `macos-latest` | `bamboo-aarch64-apple-darwin` |
| `x86_64-apple-darwin` | `macos-15-intel` | `bamboo-x86_64-apple-darwin` |
| `x86_64-unknown-linux-gnu` | `ubuntu-22.04` | `bamboo-x86_64-unknown-linux-gnu` |
| `x86_64-pc-windows-msvc` | `windows-latest` | `bamboo-x86_64-pc-windows-msvc.exe` |

The command is `cargo build --locked --release --bin bamboo --target <target>
--message-format=json-render-diagnostics`. The release profile and normal
features are unchanged. Each build checks the actual Cargo executable and
build-script outputs, the embedded ZIP and manifest hashes, the native binary
architecture, the stamped source diff, and real `bamboo --version` output.
Commit-derived generated metadata remains bound to that actual source. An
embedded CI build of another source or version is not substituted as an asset.

Before any version reservation or crate upload, source preflight requires the
complete producer files and shared executable ABI marker. A delayed or rerun
successful main CI event from an older source therefore fails before creating
a release identity, rather than publishing crates and later discovering that
its source cannot build the required executable assets.

All public assets belong to `bigduu/Bamboo-agent` and tag `v<version>`. Four
raw executables, `bamboo-browser-host-v1.tar.gz`,
`bamboo-executables-provenance-v1.json`, and `bamboo-sidecars-v1.json` are added
to that same Release. The final job authenticates the completed crate receipt
using the existing protected Environment, verifies the tag/source and all four
same-run build receipts, and verifies each uploaded asset's bytes. It refuses
replacement uploads. An interrupted final upload can reuse identical assets;
a conflicting asset blocks publication. The manifest uploads last. A public
crate Release without this manifest is not a complete desktop binary release.

The final publisher authenticates its executable provenance with the existing
protected signing key before uploading that checkpoint. If a full workflow
rerun rebuilds different bytes after the checkpoint was published, it verifies
the original same-run/source/version checkpoint, its four native receipts and
all original executable/host bytes, regenerates the identical source-derived
manifest, and uploads only the missing manifest. It never re-signs or adopts an
unsigned, foreign or incomplete checkpoint. Native transports have per-attempt
names selected through the independent job outputs; earlier transports remain
intact rather than being overwritten by a later attempt.

Release actions use reviewed immutable commit references. Each native build
also sends the executable hash/size and receipt hash through its uniquely
named GitHub job output. The final job verifies downloaded bytes against those
independent outputs, so replacing both a transported binary and its receipt
cannot supply a new accepted identity. Missing platform output bindings fail
closed. This uses the existing job-output channel and publisher permissions.

The manifest schema is version 1. `version` is the allocated version without
`v`, and `sourceRevision` is the complete accepted Git SHA. `artifacts` contains
exactly the four targets above. Each entry contains `target`,
`buildMode: "embedded"`, `embeddedFrontend: true`, `format: "raw"`, `assetName`,
lowercase `sha256`, and integer `sizeBytes`. The `browserHost` entry contains
its `assetName`, `sha256`, `sizeBytes`, and an exact `files` list with the path,
hash and size of these three source files:

- `browser-runtime/host.cjs`
- `browser-runtime/package-lock.json`
- `browser-runtime/package.json`

The deterministic gzip archive uses USTAR and contains only these three regular
file entries, with no directory, PAX, symlink, hardlink or extra members. It is
a Release asset so desktop packaging needs no Bamboo source checkout. The
existing browser runtime dependency lock remains authoritative.

A consumer pins the manifest SHA-256 and its exact version/source, validates
all target/mode/hash/size fields, downloads only assets of that same repository
and tag, verifies the raw executable and browser host, then stages its Tauri
external binary. An unavailable target, missing identity, corrupt cache, changed
asset, or incompatible mode fails closed. There is no source-build fallback in
the release download path. Local developer builds remain an explicit choice.

Run `node --test scripts/sidecar-release.test.cjs` for producer contract,
architecture, provenance, archive, credential isolation, and upload recovery
tests. Synthetic header fixtures in those tests are never executed and cannot
stand in for actual platform builds or native desktop acceptance. Update a
consumer's official identity lock only after the real four-platform assets and
manifest have been published and independently verified.
