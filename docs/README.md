# Bamboo Documentation

Start with the [main README](../README.md) for the product overview, installation, and quick start.  
This `docs/` directory keeps the deeper material that does not belong on the landing page.

## Documentation Map

### User And Integrator Docs
- [Project overview and quick start](../README.md)
- [API reference](guides/API.md)
- [Migration guide](guides/MIGRATION_GUIDE.md)
- [CHANGELOG](../CHANGELOG.md)
- [CONTRIBUTING](../CONTRIBUTING.md)
- [SECURITY](../SECURITY.md)

### Development Notes And Runtime Research
See [development/README.md](development/README.md) for active design documents and deeper runtime analysis, including:

- scheduler redesign notes
- memory-system validation plans
- session model refactors
- prompt / tools / context / compression research
- competitive runtime analysis against other agent systems

### Historical Material
- [Archive](archive/)

## Suggested Reading Paths

### I want to use Bamboo
1. [README](../README.md)
2. [API reference](guides/API.md)
3. [Migration guide](guides/MIGRATION_GUIDE.md)

### I want to understand how Bamboo works
1. [Development docs index](development/README.md)
2. runtime research under [`development/research/`](development/research/)
3. archived implementation history under [`archive/`](archive/)

### I want to contribute to Bamboo
1. [CONTRIBUTING](../CONTRIBUTING.md)
2. [Development docs index](development/README.md)
3. [CHANGELOG](../CHANGELOG.md)

## Document Placement Rules

- **Landing-page / user-facing overview** → `README.md`
- **Integrator and reference docs** → `docs/guides/`
- **Active design docs and research** → `docs/development/`
- **Historical implementation records** → `docs/archive/`

## Quick Links

- Repository: https://github.com/bigduu/Bamboo-agent
- crates.io: https://crates.io/crates/bamboo-agent
- docs.rs: https://docs.rs/bamboo-agent
