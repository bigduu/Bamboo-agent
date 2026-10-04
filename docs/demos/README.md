# Browser project demo

This is the same real recording as Lotus Next's `docs/demos/project-workspace.gif`.
Bamboo source: `025641317c5703226052a4b94a52d1844615c352`; Lotus Next source:
`1131c275cb441694a41f996228d9f91473d920f5`. Linux Chromium, 1100×720,
15.51 seconds, 2,033,461 bytes, infinite loop. The PNG is the static alternative.

The normal UI creates a real temporary project through Bamboo, then selects it
for a new task. No model is called and no agent result is staged. It demonstrates
source project management, not a released desktop build or autonomous completion.
Reproduction scripts and the actual API response are in the companion Lotus Next
checkout under `docs/demos/`. No private data or provider credentials were used.

The project recording was replaced after isolation QA. The replacement uses separate
fresh Bamboo and Jiandu roots; file-access tracing observed no default-root accesses.
See `lotus-next/docs/demos/isolation-evidence.json` for the sanitized evidence.
