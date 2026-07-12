---
name: hello-world
description: Minimal reference skill bundled with the hello-plugin example. Use when demonstrating or testing the plugin system's in-place skill discovery.
---

# Hello World

This is a trivial skill bundled inside the `hello-plugin` example plugin
(`crates/infra/bamboo-plugin/examples/hello-plugin/`). It exists purely to
exercise the plugin system's schema end-to-end:

- `plugin.json` declares it under `provides.skills` as `"hello-world"`.
- Once the plugin is installed to `~/.bamboo/plugins/hello-plugin/`, this
  skill is discovered **in place** — no copy, no symlink — because
  `~/.bamboo/plugins/hello-plugin/skills` becomes an additional skill
  discovery directory (see `bamboo-skills`' `SkillDirectorySource::Plugin`).

When invoked, just greet the user and mention that you are the example skill
from the `hello-plugin` plugin.
