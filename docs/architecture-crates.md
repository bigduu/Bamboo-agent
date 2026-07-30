# Bamboo Crate Architecture

Layered Cargo workspace. Dependencies point **downward only** — no cycles.
The HTTP server is a thin top layer; the agent loop and use-cases live in
`bamboo-engine`; ports invert the dependency between generic tools and the
server's `AppState`.

## 1. Crate dependency layers

```mermaid
flowchart TD
    subgraph APP["Application / HTTP"]
        SERVER["bamboo-server<br/>handlers · routes · AppState<br/>adapters · schedule_app"]
    end

    subgraph TOOLS["Generic tools (subsystem-independent)"]
        STOOLS["bamboo-server-tools<br/>memory · session_inspector · skill_runtime<br/>compact · overlay · sub_agent · ToolSurfaceFactory"]
    end

    subgraph ENGINE["Engine"]
        ENG["bamboo-engine<br/>runtime (agent loop) · session_app (use-cases)<br/>ports: ChildSessionPort · SubagentResolutionPort"]
    end

    subgraph CAP["Capability crates"]
        MEM["bamboo-memory"]
        TLS["bamboo-tools"]
        HOOKS["bamboo-hooks<br/>registry · matching · handler runtimes"]
        SK["bamboo-skills"]
        MCP["bamboo-mcp"]
        PERM["bamboo-permission"]
        MET["bamboo-metrics"]
        CMP["bamboo-compression"]
    end

    subgraph CORE["Core / Infrastructure"]
        AC["bamboo-agent-core<br/>Tool · ToolExecutor · ToolExecutionContext<br/>Session · Storage · AgentEvent"]
        INFRA["bamboo-infrastructure<br/>Config · SessionStoreV2 · ProviderRegistry"]
        LLM["bamboo-llm"]
        CFG["bamboo-config"]
    end

    subgraph FND["Foundation"]
        DOM["bamboo-domain<br/>core types · schedule model · subagent registry"]
    end

    SERVER --> STOOLS
    SERVER --> ENG
    STOOLS --> ENG
    STOOLS --> AC
    ENG --> MEM
    ENG --> TLS
    ENG --> HOOKS
    ENG --> SK
    ENG --> MCP
    ENG --> AC
    ENG --> INFRA
    MEM --> AC
    TLS --> AC
    HOOKS --> AC
    HOOKS --> INFRA
    PERM --> INFRA
    AC --> DOM
    INFRA --> DOM
    INFRA --> LLM
    LLM --> CFG
    LLM --> DOM

    classDef new fill:#1f6f43,stroke:#39d98a,color:#fff;
    classDef port fill:#1e4d8c,stroke:#5aa9ff,color:#fff;
    class STOOLS new;
    class ENG port;
```

- **`bamboo-server-tools`** (green) — extracted in L1. Holds tools that are
  generic agent capabilities. Depends only on lower crates, never on
  `bamboo-server`/`AppState`.
- **`bamboo-engine`** (blue) — owns the agent loop **and** the port traits that
  let generic tools reach server runtime state without depending on the server.
- **`bamboo-hooks`** — owns lifecycle registration, matcher evaluation,
  deterministic dispatch, and the command/embedded-JavaScript runtimes. The
  engine owns lifecycle seams and applies returned control or context effects.

## 2. The port pattern (dependency inversion)

How a generic tool (`SubAgentTool`) reaches `AppState`-bound runtime state
without depending on `bamboo-server`, and why the scheduler tool stays inside
its subsystem instead.

```mermaid
flowchart LR
    subgraph STOOLS["bamboo-server-tools"]
        SUBAGENT["SubAgentTool<br/>holds Arc&lt;dyn Port&gt;"]
        GENERIC["memory · skills · compact<br/>overlay · session_inspector"]
    end

    subgraph ENGINE["bamboo-engine"]
        PORTS["ChildSessionPort<br/>SubagentResolutionPort<br/>(trait definitions)"]
    end

    subgraph SERVER["bamboo-server (composition root)"]
        AS["AppState<br/>sessions · runners · stores<br/>event senders · config"]
        ADAPTER["ChildSessionAdapter<br/>impl both ports"]
        FACTORY["ToolSurfaceFactory<br/>bundles tools into executors"]
        subgraph SA["schedule_app (self-contained vertical slice)"]
            CORE2["trigger · store · manager · session_factory"]
            SCHEDTOOL["scheduler_tool<br/>facade over this subsystem"]
        end
    end

    SUBAGENT -- depends on --> PORTS
    ADAPTER -- implements --> PORTS
    AS -- builds + holds --> ADAPTER
    ADAPTER -. injected as Arc&lt;dyn Port&gt; .-> SUBAGENT
    FACTORY --> SUBAGENT
    FACTORY --> GENERIC
    FACTORY --> SCHEDTOOL
    SCHEDTOOL --> CORE2

    classDef port fill:#1e4d8c,stroke:#5aa9ff,color:#fff;
    classDef glue fill:#6b3fa0,stroke:#b794f6,color:#fff;
    class PORTS port;
    class ADAPTER,FACTORY glue;
```

**Reading it:**

- The dependency edge `SubAgentTool → AppState` is gone. It now points
  `SubAgentTool → Port` (engine), and `Adapter → Port` (server). The arrow was
  *inverted*: the tool and the server both depend on the engine-owned trait.
- `ChildSessionAdapter` (purple) is the only place the `AppState` runtime state
  is bound. It implements `ChildSessionPort` (session lifecycle: load/save/run/
  cancel + parent-wait + active children) and `SubagentResolutionPort`
  (subagent_type → model / metadata / prompt).
- `ToolSurfaceFactory` is the composition root that assembles the per-surface
  tool executors the agent runtime uses (Base / Child / WithTask / Root).
- **`scheduler_tool`** stays *inside* `schedule_app`: it is a facade over that
  subsystem (its args embed schedule-domain types; it returns schedule DTOs),
  not a subsystem-independent capability. Keeping it co-located preserves
  cohesion and makes `schedule_app` a clean slice ready to become its own crate.

## 3. What L1 changed

| | Before | After |
|---|---|---|
| Generic tools (memory, skills, compact, overlay, session_inspector) | `bamboo-server::server_tools` | `bamboo-server-tools` crate |
| `SubAgentTool` | held concrete `Arc<ChildSessionAdapter>` | holds `Arc<dyn ChildSessionPort>` + `Arc<dyn SubagentResolutionPort>`; lives in `bamboo-server-tools` |
| Ports | none | `ChildSessionPort` (extended) + `SubagentResolutionPort` in `bamboo-engine` |
| Scheduler tool | `bamboo-server::tools::schedule_tasks` | `bamboo-server::schedule_app::scheduler_tool` (with its subsystem) |
| `bamboo-server/src` | 49,465 LOC | 45,488 LOC |

Future moves this sets up: **L2** sink handler orchestration into
`engine::session_app` use-cases; **L5** lift the whole `schedule_app` slice
(incl. its tool) into a `bamboo-schedule` crate.
