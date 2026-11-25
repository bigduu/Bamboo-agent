# Bamboo architecture

[简体中文](./architecture.zh-CN.md) · [Bamboo README](../README.md)

A single run can end while its session and Work are still open. Bamboo keeps session history, run state, and Work records separately. You can open a conversation again or reassign Work while keeping its goal and acceptance criteria.

## Runtime entry points

```mermaid
flowchart TD
    CLI["bamboo -p · one task"] --> Engine["Bamboo engine"]
    SDK["Rust SDK · in process"] --> Engine
    Client["Lotus Next or API client"] <-->|HTTP requests / SSE or WebSocket events| Serve["bamboo serve"]
    Serve --> Engine
    Engine --> Workspace["Project workspace"]
    Engine --> Sessions["Local session store"]
```

All three entry points use the same engine code. The SDK embeds the engine in your process. Web clients connect to a running server. [CLI, HTTP and SDK examples](./guides/GETTING_STARTED.md) · [Rust crate boundaries](./architecture-crates.md)

## Tool calls and approvals

```mermaid
flowchart TD
    Input["User request"] --> Round["Model round"]
    Round -->|Text| Events["Client events"]
    Round -->|Propose tool call| Policy{"Check tool policy"}
    Policy -->|Allow| Tool["Run tool"]
    Policy <-->|Ask / user decision| Wait["Wait for approval"]
    Wait -->|Approval request| Events
    Policy -->|Deny| Outcome["Tool outcome"]
    Tool --> Outcome
    Outcome -->|Next round| Round
    Outcome -->|Tool status| Events
```

A denied call also produces an outcome for the next model round, so the agent can report the block or choose an allowed action. [Permissions](./config-reference.md#permissions) · [User responses](./guides/API.md#submit-user-response) · [Event types](./guides/API.md#agentevent-types)

## Sessions and recovery

```mermaid
flowchart TD
    Run["Running session"] -->|Save history and runtime state| Store["Local session store"]
    Store -->|Read by Session ID| History["Open saved history"]
    History -->|Send a message| Next["Start a new turn"]
    Store -->|Supported checkpoint| Check{"Check session and run"}
    Check -->|Eligible| Resume["Continue interrupted work"]
    Check -->|Conditions unresolved| Pending["Surface recovery state"]
```

Reopening a session reads its history. Recovery of an interrupted run uses a supported checkpoint and fresh identity and ownership checks. [Session and run identity](./session-inbox-router.md#components-and-ownership) · [Storage](./session-storage-v3.md#current-boundary) · [Goal recovery](./goal-recovery.md#completion-and-restart)

## Project context and memory

```mermaid
flowchart TD
    History["Session history and selected instructions"] --> Context["Bamboo · assemble context"]
    Context --> Model["Configured model"]
    Model -->|Request memory on demand| Tool["Bamboo memory tool"]
    Scope["Saved session and project scope"] -->|Scope check| Tool
    Tool <-->|Search, read or write| Jiandu["Jiandu · local memory store and retrieval"]
    Tool -->|Results| Context
```

Memory queries use the saved Session, Section and Project context. Switching folders preserves that scope. Bamboo puts returned memory into the model context as a tool result. [Memory ownership and scope](../crates/infra/bamboo-skills/assets/bamboo-manual/references/memory.md#ownership-and-scope) · [Project identity](./project-identity.md#project-and-workspace) · [Jiandu](https://github.com/bigduu/Jiandu)

## Supervisor, Work and execution

```mermaid
flowchart TD
    User["User · goal and approval"] --> Supervisor["Supervisor · plan, delegate, verify"]
    Supervisor -->|Request dispatch| Host["Bamboo host · check authority"]
    Policy["Orchestration · dispatch and capacity policy"] --> Host
    Host <-->|Work commands and facts| Ledger["Work record · goal, assignment, submission, acceptance"]
    Host <-->|Start work / receive evidence| Session["Execution session · own tools and permissions"]
    User -.->|Approve when needed| Session
```

Work records outlive individual execution attempts, so a new assignment can use the same goal and acceptance criteria. The user can approve a tool while work runs and accept the delivered work separately. [Supervisor role](./supervisor-identity.md#model-role-and-thinking-mode) · [Work controls](./guides/TICKET_CONTROLS.md) · [Work records](../crates/infra/bamboo-tickets/src/lib.rs) · [Orchestration](../crates/engine/bamboo-orchestration/src/lib.rs)

## Sub-agent placement

```mermaid
flowchart TD
    Host["Bamboo host · sub-agent admission"] -->|Local execution| Local["Local child"]
    Host --> Id["Child session ID and history"]
    Host <-->|Scoped WSS| Broker["Broker · durable mailboxes"]
    Broker <-->|Scoped WSS| Remote["Configured fixed remote worker"]
    Remote --> Checkout["Verify the assigned Git checkout"]
```

The host owns Child identity and history. In the fixed remote setup, the broker routes messages and the worker checks its checkout before execution. [Remote topology and setup](./design/architecture-overview.md) · [Fixed remote acceptance](../tests/remote_broker_activation.rs) · [Server deployment](./guides/DEPLOY.md)
