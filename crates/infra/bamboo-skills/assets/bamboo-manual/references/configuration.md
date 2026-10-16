# Configuration: keys, partitions and overrides

## Read the right representation

`Config` is the compatibility/effective-value view. It is not the storage
authority for every setting. The typed section facade owns independently
revisioned files and immutable LiveSection snapshots. `config-sections.json`
records section layout. The current section names are core, providers, mcp,
tools-skills, memory, subagents, notifications, connect, cluster-fabric, env,
access-control, hooks, model-policy, model-limits and credentials. Most use the
same name plus `.json`; model limits uses `model_limits.json`.

Use the supported settings API or CLI for the installed version. Do not edit a
legacy `config.json` and assume it overrides a migrated section. Do not copy a
compatibility-view fragment directly into a typed section envelope. Migration,
revision/CAS checks, credential ownership and validation belong to the existing
configuration authority, not an agent-created writer.

The default Bamboo data root is `~/.bamboo`; `BAMBOO_DATA_DIR` or a command's
`--data-dir` may select another root. That does not select Jiandu's memory root.
Never publish an actual user's path when only a generic location is needed.

## Defaults, disk, environment and CLI

There is no universal last-writer-wins stack for all keys. For supported
overrides, typed defaults feed the loaded configuration, process environment
can override it in memory, and the relevant CLI command can override its own
runtime settings. The section facade and the consuming runtime are separate
observations. A persisted revision is not proof that a consumer has applied it.

Examples of implemented controls include `BAMBOO_PROVIDER`, provider-specific
`BAMBOO_<PROVIDER>_API_KEY`, `BAMBOO_MEMORY_RELEVANT_RECALL_RERANK`, and
`bamboo serve --port ...`. Never infer a general `BAMBOO_<DOTTED_KEY>` mechanism.
Environment credential overrides are in-memory inputs; don't write them into a
configuration example or tell a user they have been persisted.

## Bounded key catalog

`config-keys.json` documents selected serialized paths, section ownership,
defaults and non-default example values. Tests construct the actual typed DTOs,
compare defaults, deserialize the examples and compare their typed fields.
This avoids treating unknown keys retained in a flattened map as recognized
typed settings. It does not cover all validators, dynamic provider maps,
credential operations, optional fields or feature-specific settings.

`Config::default()` serialized to JSON is not a schema. Optional and omitted
values, aliases, enum variants, flattened maps and validation constraints need
their actual DTOs and validators. Absence from this catalog means “not covered
by this catalog”. It does not mean invalid, disabled or safe to delete.

The following illustrates compatibility-view spelling only. It is not a whole
section file, a write request, or evidence about the running instance:

```json
{
  "memory": {
    "relevant_recall": true,
    "relevant_recall_rerank": false
  },
  "skills": {
    "disabled": []
  }
}
```

## Secrets and effective state

Credential values belong to the isolated credential store. Ordinary settings
can carry references/configured metadata. Even a redacted configuration dump
can include private paths, hostnames, public environment values or identities;
it is not safe to forward the whole object by default.

Consult `providers-mcp.md` for process-observed desired/applied receipts and
their coverage limits. Unknown, last-known-good, in-sync and remote service
success are different claims.

Source: `crates/infra/bamboo-config/src/config.rs`, `section_facade.rs`,
`paths.rs`; `docs/config-reference.md` and
`docs/guides/configuration-application-state.md`.
