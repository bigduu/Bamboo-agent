# 班布架构

[English](./architecture.md) · [班布 README](../README.zh-CN.md)

一次运行结束后，会话和 Work 还可以继续。班布分别保存会话历史、运行状态和 Work 记录。你可以回到原会话，也可以重新指派 Work，沿用已有的目标和验收条件。

## 运行入口

```mermaid
flowchart TD
    CLI["bamboo -p · 单次任务"] --> Engine["班布引擎"]
    SDK["Rust SDK · 进程内执行"] --> Engine
    Client["Lotus Next 或 API 客户端"] <-->|HTTP 请求 / SSE 或 WebSocket 事件| Serve["bamboo serve"]
    Serve --> Engine
    Engine --> Workspace["项目工作区"]
    Engine --> Sessions["本地会话存储"]
```

三个入口使用同一套引擎代码。Rust 应用可以把引擎嵌进自己的进程，网页和 HTTP 客户端则连接正在运行的服务。[CLI、HTTP 和 SDK 示例](./guides/GETTING_STARTED.md) · [Rust Crate 分层](./architecture-crates.md)

## 工具调用与审批

```mermaid
flowchart TD
    Input["用户请求"] --> Round["模型调用"]
    Round -->|文本| Events["客户端事件"]
    Round -->|提出工具调用| Policy{"检查工具权限"}
    Policy -->|允许| Tool["执行工具"]
    Policy <-->|请求确认 / 用户回复| Wait["等待审批"]
    Wait -->|审批请求| Events
    Policy -->|拒绝| Outcome["工具结果"]
    Tool --> Outcome
    Outcome -->|下一轮| Round
    Outcome -->|工具状态| Events
```

工具调用被拒绝时，班布也会把结果交给下一轮模型。模型可以说明哪里被拦住，或选择权限允许的操作。[权限配置](./config-reference.md#permissions) · [提交用户回复](./guides/API.md#submit-user-response) · [事件类型](./guides/API.md#agentevent-types)

## 会话保存与恢复

```mermaid
flowchart TD
    Run["正在执行的会话"] -->|保存历史和运行状态| Store["本地会话存储"]
    Store -->|按会话 ID 读取| History["打开历史记录"]
    History -->|发新消息| Next["开始新一轮"]
    Store -->|受支持的检查点| Check{"核对会话和运行"}
    Check -->|符合条件| Resume["继续中断的工作"]
    Check -->|条件未满足| Pending["显示恢复状态"]
```

重新打开会话通常是读取历史。恢复中断的任务需要受支持的检查点，并校验会话身份和运行归属。[会话与运行标识](./session-inbox-router.md#components-and-ownership) · [会话存储](./session-storage-v3.md#current-boundary) · [Goal 恢复](./goal-recovery.md#completion-and-restart)

## 项目上下文与简牍记忆

```mermaid
flowchart TD
    History["会话历史和选定的指令"] --> Context["班布组装上下文"]
    Context --> Model["已配置的模型"]
    Model -->|按需请求记忆| Tool["班布记忆工具"]
    Scope["已保存的会话和项目范围"] -->|核验范围| Tool
    Tool <-->|搜索、读取或写入| Jiandu["简牍 · 本地记忆与检索"]
    Tool -->|结果| Context
```

记忆查询使用会话已保存的项目和分组归属。切换工作目录时仍沿用这个范围，查到的内容作为工具结果交给模型。[记忆归属与范围](../crates/infra/bamboo-skills/assets/bamboo-manual/references/memory.md#ownership-and-scope) · [项目标识](./project-identity.md#project-and-workspace) · [简牍](https://github.com/bigduu/Jiandu)

## Supervisor、Work 与执行

```mermaid
flowchart TD
    User["用户 · 提出目标和审批"] --> Supervisor["Supervisor · 计划、分派、核验"]
    Supervisor -->|请求派发| Host["Bamboo 宿主 · 检查权限"]
    Policy["编排策略 · 派发与并行限制"] --> Host
    Host <-->|Work 操作与记录| Ledger["Work 记录 · 目标、指派、提交、验收"]
    Host <-->|启动工作 / 收到证据| Session["执行会话 · 自己的工具和权限"]
    User -.->|需要时批准| Session
```

同一个 Work 可以重新指派，执行会话换了，任务目标和验收条件仍在。用户可以在执行时审批工具，交付后再决定是否验收。[Supervisor 职责](./supervisor-identity.md#model-role-and-thinking-mode) · [Work 操作](./guides/TICKET_CONTROLS.md) · [Work 记录](../crates/infra/bamboo-tickets/src/lib.rs) · [编排策略](../crates/engine/bamboo-orchestration/src/lib.rs)

## 子代理的运行位置

```mermaid
flowchart TD
    Host["Bamboo 宿主 · 子代理准入"] -->|在本机执行| Local["本地子会话"]
    Host --> Id["子会话 ID 和历史"]
    Host <-->|限定范围的 WSS| Broker["Broker · 持久信箱"]
    Broker <-->|限定范围的 WSS| Remote["已配置的固定远端 Worker"]
    Remote --> Checkout["核对分配的 Git 工作区"]
```

宿主管理子会话的身份和历史。使用固定远端 Worker 时，Broker 投递消息，Worker 在执行前核对工作区。[远端部署架构](./design/architecture-overview.md) · [固定远端验收](../tests/remote_broker_activation.rs) · [部署服务](./guides/DEPLOY.md)
