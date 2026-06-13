#!/usr/bin/env python3
"""Print the crates.io publish order for the Bamboo workspace, one per line.

The order is derived from the REAL dependency graph (``cargo metadata``), not a
hand-maintained list. A stale hand-list is what silently dropped sub-crates
(e.g. bamboo-config, bamboo-llm) from the publish loop and broke the nightly
release: a dependency was published before the crates it needs existed on the
index. Computing the order here keeps it in lock-step with the workspace.

Scope: ``bamboo-agent``'s transitive workspace-dependency closure (the published
library and everything it pulls in), dependencies first. Standalone binaries
outside that closure (bamboo-cli, bamboo-tui, bamboo-client-core) are not part
of the published library and are intentionally excluded.

Run from the workspace root (where the top-level Cargo.toml lives).
"""

import json
import subprocess
import sys

ROOT_CRATE = "bamboo-agent"


def main() -> int:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"]
        )
    )
    packages = {pkg["name"]: pkg for pkg in metadata["packages"]}
    members = set(packages)

    def workspace_deps(name):
        # Dependencies of `name` that are themselves workspace members, across
        # all kinds (normal/build/dev) — any of them must be on the index before
        # `name` can be packaged/verified.
        return {
            dep["name"]
            for dep in packages[name]["dependencies"]
            if dep["name"] in members
        }

    if ROOT_CRATE not in members:
        print(f"error: {ROOT_CRATE} not found in workspace", file=sys.stderr)
        return 1

    # Transitive closure from the root crate.
    closure = set()
    stack = [ROOT_CRATE]
    while stack:
        current = stack.pop()
        if current in closure:
            continue
        closure.add(current)
        stack.extend(workspace_deps(current))

    # Topological sort (DFS post-order): a crate is emitted only after every
    # workspace dependency it has. Ties broken alphabetically for stable output.
    order = []
    state = {}  # 1 = visiting, 2 = done

    def visit(node):
        if state.get(node) == 2:
            return
        state[node] = 1
        for dep in sorted(workspace_deps(node)):
            if dep in closure and state.get(dep) != 2:
                visit(dep)
        state[node] = 2
        order.append(node)

    for node in sorted(closure):
        visit(node)

    # Safety net: assert the emitted order is a valid topological order.
    index = {name: i for i, name in enumerate(order)}
    for name in order:
        for dep in workspace_deps(name):
            if dep in index and index[dep] >= index[name]:
                print(
                    f"error: publish order invalid — {name} precedes its "
                    f"dependency {dep}",
                    file=sys.stderr,
                )
                return 1

    print("\n".join(order))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
