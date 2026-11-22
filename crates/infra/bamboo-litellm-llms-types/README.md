# LiteLLM Rust wire types

The source comes from LiteLLM v1.110.2 at commit
`c3cafa3782642685ef10777e4c5931400c0a7176`. The upstream `src/` tree ID is
`19772ceeb440e946d2b9405972a86318118c1a97`. The Anthropic beta header test
loads its fixture from within this package. That fixture is copied from the
same upstream commit. [Upstream crate](https://github.com/BerriAI/litellm/tree/c3cafa3782642685ef10777e4c5931400c0a7176/litellm-rust/crates/llms-types)

Bamboo publishes the pinned source under its own package name and stamps its
workspace version during publication. The upstream license is included in
`LICENSE`.
