# Bamboo Agent

Install the Bamboo CLI with Node.js 22.14 or newer.

~~~sh
npm install --global @bigduu/bamboo-agent
bamboo --version
~~~

The first invocation downloads the corresponding executable from the exact
version of the official GitHub Release. The launcher verifies its size and
SHA-256 against the included manifest and stores it in the per-user cache.
Subsequent invocations verify and use the cached executable.

Bamboo supports macOS ARM64, Linux x64 and ARM64, and Windows x64 and ARM64.
The Linux binaries use the GNU runtime. The package includes the Release's
Browser Host and its pinned JavaScript dependency. Browser automation uses
an available Chromium installation.

See the [Bamboo releases](https://github.com/bigduu/Bamboo-agent/releases)
and [Bamboo documentation](https://github.com/bigduu/Bamboo-agent).
