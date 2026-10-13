# Architecture Overview — Broker-mediated remote sub-agents

> 本文是面向"怎么部署 / 项目结构 / 部署能力实现"的总览。
> 配套设计文档: [SubAgent Actor runtime](subagent-actor-runtime-design.md)、
> [broker 设计](remote-mailbox-broker-design.md) 和 [远端 Actor 方案](remote-actor-plan.md)。

## #791 当前交付边界（2026-09-29）

`ActorSession`（持久 `Session.id`）是逻辑身份、历史和父子归属的唯一权威；
`ActorActivation` 是有租约和 fencing 的一次 Run，`WorkerHost` 只是可替换容量。
Root 和 Child 的模型工具目录在子代理能力上只暴露逻辑 `SubAgent`，`deploy_agent`、`ask_agent` 和
`cluster` 保留为 Host/兼容调用，不能作为新的 Actor 身份来源。

规范本地 Child 和**固定远端** Child 已能通过 broker 执行。固定远端使用经 PeerPolicy
认证的 WSS `Run` 和每次 Run 的 `EnvironmentLease` v1：Host 捕获干净 Git 工作区的
commit/内容摘要，Worker 在执行前核验本机工作区。HostRegistry 和调度器已能记录
经 broker 认证的容量并原子预留 slot，但尚未接入通用自动放置和故障转移。

**仍待 #791 验收**：通用远端 Run 管理与跨 Host 迁移/故障转移；Docker、SSH 和调度池
WorkerHost 的统一 ActorActivation；干净 Git 快照以外的 EnvironmentLease、artifact 和
能力代理边界；失联或 ACK 不确定时 broker receipt 的活性修复。下文第 2–6 节保留
历史 `deploy_agent`/`ask_agent` 部署链路，以说明物理 worker 管理；不能将其当作已
完成的规范 SubAgent 路径。

整个系统是**中心辐射(hub-and-spoke)**拓扑:一个中心消息总线(broker),一个编排者
(orchestrator),N 个可部署到任意环境的 worker。三者都是**同一个 `bamboo` 二进制**的不
同子命令。broker 是**纯消息总线**——只路由消息,不 spawn actor、不和别的 broker 协调。

---

## 1. 运行形态:三种进程

```
┌─────────────────────────────────────────────────────────────────┐
│  bamboo broker serve            中心消息总线(broker)             │
│    · WebSocket 服务 + Bearer 鉴权                                  │
│    · 每个 session 一个持久 Mailbox(maildir),路由 + 推送          │
│    · 仅转发消息,不执行业务逻辑                                     │
└───────────▲───────────────────────────────▲──────────────────────┘
            │ ws(s):// + token                │ ws(s):// + token
   ┌────────┴─────────────────┐     ┌────────┴──────────────────────┐
   │  编排者(orchestrator)   │     │  worker = bamboo broker-agent  │
   │  = bamboo serve           │     │    serve(本地/Docker/远端)    │
   │  · root agent loop        │     │  · 连 broker、订阅自己邮箱      │
   │  · 跑真实 MCP servers      │     │  · serve_executor 等任务        │
   │  · serve_mcp_proxy 服务    │     │  · 能力 = 内置 + 同步skills+MCP │
   │  · 模型工具 SubAgent       │     │                                 │
   │  · Host 管理物理容量       │     │                                 │
   └────────────────────────────┘     └─────────────────────────────────┘
```

| 角色 | 子命令 | 职责 |
|---|---|---|
| **broker** | `bamboo broker serve` | 网络消息总线 + 持久邮箱;鉴权;路由/推送 |
| **orchestrator** | `bamboo serve`(根 agent) | 持有 ActorDirectory、SessionInbox 和规范 Child Run；模型通过 `SubAgent` 编排，Host 管理 broker/worker |
| **worker** | `bamboo broker-agent serve` | 连 broker、提供执行容量；只有经 Host 绑定的 Run 才能修改规范 ActorSession |

规范 Actor 按 `Session.id` 寻址。broker mailbox、worker id 和 endpoint 是物理投递信息，
不能替代 ActorId。broker 是中心辐射的传输层，归属和权限仍由 Host 的 ActorDirectory 判定。

---

## 2. 遗留物理 worker 部署流程（兼容路径）

下面流程描述 `deploy_agent`/`ask_agent` 对物理 worker 的操作。它可供 Host/兼容调用，
但不创建可替换 Host 上继续执行的规范 Child ActorSession。规范 Child 经 `SubAgent`
创建持久 Session、准入 SessionInbox，再由 Host 绑定 ActorActivation 并发送 broker Run。

```
旧兼容调用方
   │  调用工具
   ▼
deploy_agent(action="deploy", env="local"|"docker"|"ssh", model=…, [echo])
   │  DeployAgentTool → Deployer
   ▼
Local/Docker/Ssh Deployer 拉起:
   bamboo broker-agent serve --broker <ws> --token <env> --id w1 [--mcp-proxy <orch>]
   │  (token 走环境变量,不进 argv)
   ▼
worker 进程:
   BrokerClient.connect(broker)  →  subscribe("w1")  →  serve_executor 等任务
   │  (能力对齐:build_spec 读本机 config → Capabilities → 装 skills + MCP)
   ▼
编排者:
   ask_agent(target="w1", question=…, mode=query|steer)  →  经 broker 投递 →  worker 应答
   │
   ▼  (worker 干活若需 host-bound MCP,如 nova)
   McpProxyExecutor  →  McpRequest 经 broker  →  编排者 serve_mcp_proxy 执行真 MCP  →  回结果

回收: deploy_agent(action="stop", id="w1")   /   查看: deploy_agent(action="list")
```

---

## 3. 项目结构(workspace 分层 + 本次新增/改动)

workspace 四层:`core`(类型/接口)→ `infra`(独立服务)→ `engine`(核心逻辑)→ `app`(可执行)。

### `crates/infra/bamboo-subagent` — actor 底座(leaf crate)
| 文件 | 内容 |
|---|---|
| `proto.rs` | 线协议 `ParentFrame` / `ChildFrame`（actor 直连 WS）|
| `transport.rs` | `WsServer::bind(addr)` / `bind_loopback`;`ChildClient` |
| `fleet.rs` | `spawn_worker`（本地子进程引导）|
| `launcher.rs` | `WorkerLauncher` trait + `LocalSubprocessLauncher`(P0 接缝)|
| `discovery.rs` | `Discovery` trait + `FileFabric`(`impl for Fabric`)|
| `mailbox.rs` | `Mailbox`(maildir）+ `InboxKind{Task,Ask,Reply,McpRequest,McpReply}` + `AskBody/AskMode/ReplyBody` |
| `provision.rs` | `ProvisionSpec`(identity/secrets/`Placement`/**`Capabilities{mcp,skills_dir,mcp_proxy}`**/`McpProxyConfig`)|
| `executor.rs` | `ChildExecutor` / `EchoExecutor` / `ChildOutcome` |
| `store.rs` | 项目键控会话存储 + per-session mailbox 目录 |

### `crates/app/bamboo-broker` — ⭐ 本次新建的整套 broker
| 文件 | 内容 |
|---|---|
| `proto.rs` | `ClientFrame`(Hello/Deliver/Subscribe/Ack) ↔ `BrokerFrame`(Welcome/Error/Message/Delivered) |
| `core.rs` | `BrokerCore` —— 按 session 的 Mailbox 路由、推送订阅、at-least-once |
| `server.rs` | `BrokerServer` —— WS 外壳 + Bearer 握手(`bamboo broker serve`)|
| `client.rs` | `BrokerClient` —— 连接 + 帧分流(messages / delivered)|
| `serve.rs` | `serve_mailbox` / `serve_executor`(query/steer over `ChildExecutor`)|
| `ask.rs` | `ask_agent` / `ask_over` / `request_over`(通用相关请求/应答)|
| `deploy.rs` | `Deployer` trait + `Local/Docker/Ssh` 实现 + `AgentDeployment` + `DeployedAgent` |
| `mcp.rs` | `McpProxyExecutor`(worker 端)+ `serve_mcp_proxy`(编排者端)+ `McpRequest/McpReply` |
| `tests/ws_roundtrip.rs` | 端到端 WS 测试 |

### 根 crate `bamboo-agent` — `bamboo` 二进制
| 文件 | 内容 |
|---|---|
| `src/bin/bamboo.rs` | CLI —— 新增 `broker serve` / `broker-agent serve` 子命令 |
| `src/broker_agent.rs` | `broker-agent serve`:`build_spec` 填 `Capabilities`,拉起 executor(echo 或真）|
| `src/subagent_worker.rs` | `BambooRuntimeExecutor` —— 真 agent loop;按 `Capabilities` 装 MCP / skills / proxy |

### `crates/app/bamboo-server-tools` — 逻辑工具与兼容工具
| 文件 | 内容 |
|---|---|
| `sub_agent.rs` / `sub_agent_facade.rs` | 模型可见的逻辑 `SubAgent`（创建、纠偏、检查、控制、父请求回复）|
| `ask_agent.rs` | 兼容查询/物理 worker 通信；不进入 Root/Child 模型目录 |
| `deploy_agent.rs` | 兼容物理部署/停止/列表，`DeployedRegistry` 是物理句柄缓存，不是 ActorDirectory |

### `crates/app/bamboo-server` — 编排者
| 位置 | 内容 |
|---|---|
| `app_state/builder.rs` | 配了 `subagents.broker` 就 spawn `serve_mcp_proxy`(backend = 真 `McpToolExecutor`)|
| `app_state/tools.rs` | 注册规范 `SubAgent` 和 Host 兼容调用；模型目录在 engine 中滤除物理工具 |
| `app_state/wake_reconciler.rs` | 启动与周期性扫描持久 SessionInbox，修复错过的唤醒 |
| `app_state/actor_events.rs` | 验证当前 ActorActivation 后投影 Actor 事件 |

---

## 4. 遗留部署能力的实现原理

### ① `Deployer` trait —— "在某环境拉起 worker" 的抽象(`deploy.rs`)
三个实现生成**同一条** `bamboo broker-agent serve …` 命令,只是放在不同环境跑:
```
LocalProcessDeployer   Command::new(bamboo_bin).args(…)                         本机子进程
DockerDeployer         docker run --rm --network host -v ~/.bamboo:ro <image> … 容器
SshDeployer            ssh -tt host 'BAMBOO_BROKER_TOKEN=… bamboo …'(shell 转义) 远端
```
- 物理部署使用旧 broker 凭证传递方式；它不是按 ActorActivation 缩窄的权限边界。
- 返回 `DeployedAgent`:`kill_on_drop` 子进程句柄 + 可选清理命令(docker `rm -f`)。

### ② 物理句柄缓存(`deploy_agent.rs` 的 `DeployedRegistry`)
`DeployedAgent` 是 kill-on-drop 的——部署完若丢弃句柄,进程立刻被杀。所以工具把句柄存进
`Arc<Mutex<HashMap<id, DeployedAgent>>>`,**随 server 生命周期存活**;`action=stop` 取出并
优雅关闭,`action=list` 枚举。缓存随 server 消失，不承担规范 Actor 的持久身份、
transcript 或故障转移；这些属于 ActorDirectory/SessionStore。

### ③ 位置无关的接缝(Phase 0,让"远程"成为可能)
把三个"本地死结"抽象成 trait,默认实现逐行复刻现状、零行为变化:
- `WorkerLauncher`(本地 spawn vs 未来连远端常驻 worker)
- `Discovery`(本地文件 fabric vs 未来 registry/控制面)
- `WsServer::bind(addr)`(可绑 `0.0.0.0`,不再只 loopback)
- `Placement` 枚举(`Local` / `Remote{endpoint}` / `Schedulable{pool}`)进 `ProvisionSpec`

### ④ 能力对齐(worker 不"残废")—— P1
worker 的 `build_spec`(`broker_agent.rs`)读**它那台机器的 config**(本地=同一份 `~/.bamboo`;
Docker=只读挂载 `-v ~/.bamboo:/root/.bamboo:ro`;ssh=远端那份)→ 填 `Capabilities`:
- `skills_dir` = 用户 skills 目录(内置 skills 随二进制走、**不用同步**)
- MCP 二选一:`mcp`(P1:同步**可移植的 URL 类** SSE/streamable-http 直连;**排除 stdio**)
  或 `mcp_proxy`(P2:代理)

`BambooRuntimeExecutor::build`(`subagent_worker.rs`)据此把 MCP/skills 叠到内置工具上。
**没有 `Capabilities` 的普通 actor children 行为完全不变(gated,零回归)。**

### ⑤ MCP 代理(P2)—— 远端用宿主绑定 MCP
宿主绑定的 stdio MCP(nova 要本机屏幕/凭证)不可能搬到远端。复用请求/应答机制:
- worker 侧 `McpProxyExecutor`(impl `ToolExecutor`)用一个 `<worker>#mcp` 子连接,启动拉
  manifest(可代理工具 schema)、调用时发 `McpRequest` → 等 `McpReply`;
- 编排者侧 `serve_mcp_proxy` 收到就对真 `McpServerManager` 执行、回结果。
- **只有编排者跑那些 host-bound server**(一个 nova、无争用),worker 不需要本地二进制。

---

## 5. CLI 与工具面

```bash
# 中心 broker(可对外)
bamboo broker serve --bind 0.0.0.0:9600 --token $T          # 或 BAMBOO_BROKER_TOKEN

# 部署一个干活的 worker(本地/远端/容器),它拨回 broker
bamboo broker-agent serve --broker ws://host:9600 --token $T \
    --id w1 --model anthropic:claude-sonnet-4-6              # 或 --echo 冒烟
    [--mcp-proxy bamboo-orchestrator]                        # 把 MCP 代理回编排者

# 编排者:config 里设 subagents.broker {endpoint, token}
#   → Host 获得 broker 传输/兼容管理能力；Root/Child 模型仍只见 SubAgent
```

下列工具是 Host/兼容调用，不进入 Root/Child 的模型目录：
- `deploy_agent(action=deploy, env=local|docker|ssh, model, image?, host?, echo?)` → `{id}`
- `deploy_agent(action=stop, id)` / `deploy_agent(action=list)`
- `ask_agent(target=id, question, mode=query|steer, timeout_secs?)` → `{answer}`

模型创建、继续、检查和控制规范 Child 时调用 `SubAgent`，以 ActorId 寻址。
Host 执行时从持久 Session/Inbox 和当前 activation fence 推导权限，不使用模型传入的
worker id、broker mailbox、endpoint 或容器 id 作为身份。

---

## 6. 配置(`subagents` 段)

```jsonc
"subagents": {
  "max_concurrent": 200,
  "broker": { "endpoint": "ws://broker-host:9600", "token": "…" },  // broker 传输与 Host 兼容服务
  "mcp_role_allowlist": [                                          // 可选(issue #54):按角色收窄代理工具面
    { "role": "researcher", "tools": ["fetch_url"] },
    { "role": "sandboxed", "tools": [] }                           // 空数组 = 显式锁死(0 工具)
  ]
}
```
配了 `subagents.broker` 后 Host 可注册 `deploy_agent`/`ask_agent` 兼容调用并启动
`serve_mcp_proxy`；Root/Child 的模型目录仍滤除物理工具。远端规范 Run 还要求 scoped
PeerPolicy、当前 ActorActivation、精确 broker 身份及 EnvironmentLease；一个 endpoint/token
配置本身不授予执行权限。

`mcp_role_allowlist` 为空(默认)= 每个角色都不受限,行为与 #54 之前完全一致。列出的角色只能
看到/调用其 `tools` 里的工具(manifest 过滤 + Call 兜底拒绝双重生效);未列出的角色仍不受限。
**信任边界**:这里的 `role` 取自连接 worker 自报的 `AgentRef.role`(`ChildIdentity.role`),
**不是**经过 broker 鉴权验证的身份——同一个 bearer token 能连接的 worker 可以自称任意角色。
所以这个 allowlist 只能防"跑歪了的/幻觉的"worker(它按分配到的角色老实上报),挡不住真正
恶意、蓄意冒充其他角色的 worker;需要更强边界(per-role broker 凭证、签名 `AgentRef` 等)不在
本 issue 范围内。

---

## 7. 高并发 sub-agent 事件面（#1031）

sub-agent 的完整事件流只发布到它自己的 session channel；parent channel 只保留
`SubAgentStarted` / `SubAgentHeartbeat` / `SubAgentCompleted`。因此 200 个并行 child
不会把逐 token 事件递归复制到每一层祖先，前端打开 child session 时仍可直接订阅其完整事件。

actor wire 使用 `ActorEventBatch`，路由键为 logical Session。规范本地 Child 与固定
`Placement::Remote` Child 均走 broker；Host 根据持久 ActorDirectory 的 activation/lease
fence 和单调序列验收 Event/Outcome，迟到的旧 worker frame 不得推进新 Run。历史直连
WebSocket 是兼容路径，不能作为规范远端 Actor 的确认或故障转移证明。Schedulable
Cluster 的物理部署能力仍需接入统一 ActorActivation 才满足 #791。

| QoS | 典型事件 | 传输语义 |
|---|---|---|
| `durable` | tool/permission/terminal boundary 与未知事件 | Maildir + receipt；背压，不丢 |
| `snapshot` | runner/token-budget/context-pressure gauge | 有界 live lane；过载可丢，后续 batch 的 sequence gap 可暴露丢失 |
| `ephemeral` | token/reasoning token/heartbeat | 有界 live lane；批量，过载可丢 |

`task_list_item_progress` 是 delta，不是全量 snapshot，因此走 `durable`。当前
`ActorEventRouter` 能拒绝失序、重复和旧 activation frame，并以有界 replay window 报告
sequence gap；Lotus Next 对其可见 Actor 使用快照/历史恢复。端到端持久 event cursor 与
所有 broker 失联边界的恢复仍需 #791 的故障注入验收。

每个 worker 固定使用一条 inbound subscription，加 control/event 两条 outbound uplink；连接数不再随
并行 Run 数量线性增加。control 与 event 队列独立，cancel/approval/admission 不会排在 token 后面；
同一 Run 的 live batch、durable event、Outcome 仍在 ordered event lane 上保持顺序。默认允许 200 个
active actor，但 warm-idle pool 单独限制为 16，避免为了并发上限长期保留 200 个空闲进程。

### 7.1 当前边界与下一阶段

当前已经有分离的 control/event uplink、有界事件 replay、Host 侧 Run receipt，以及
broker PeerPolicy 认证的 Host 容量观测和原子 slot 预留。这些基础仍不等于完整的跨 Host
Actor 管理。#791 还需：

1. 将 scheduler 预留、broker 连接代际、ActorActivation 和 EnvironmentLease 绑定到
   一次原子可恢复的 Run 准入；Host 失联后在另一个合格 Host 上重试同一 ActorId。
2. 完成不确定 ACK/receipt 的活性修复，证明断线和重放后不会丢失旧 Outcome 或双写 transcript。
3. 在高并发和重启故障注入下证明事件缺口能以规范快照/历史恢复，并证明 100+ Actor
   只对可见对象建立有界订阅；当前 UI 的 129 Actor 浏览器用例使用模拟快照。
4. 对复用的 WorkerHost 验证跨 Root/Project 的 transcript、批准、取消、workspace 和密钥清理。

滚动升级时，`execution_epoch = 0` 选择旧的逐事件 wire；新 host 发出的非零 epoch 才启用 batch
协议。`ChildOutcome.transcript` 目前只是兼容字段：host 不消费它，session checkpoint 才是 transcript
真相源。

---

## 8. 线协议小结

| 层 | 帧 / 消息 |
|---|---|
| broker 总线 | `ClientFrame{Hello,Deliver,PublishEventBatch,Subscribe,Ack}` ↔ `BrokerFrame{Welcome,Error,Message,EventBatch,Delivered}` |
| 邮箱消息 | `InboxMessage{id, from, kind, body, correlation_id}`;`InboxKind{Task,Ask,Reply,Run,Event,Outcome,McpRequest,McpReply}` |
| ask | `Ask{AskBody{question,mode}}` → `Reply{ReplyBody{answer}}`(按 correlation_id 配对)|
| mcp 代理 | `McpRequest{Manifest \| Call{tool,arguments}}` → `McpReply{manifest? \| result? \| error?}` |
| actor 直连 | `ParentFrame{Run,Cancel,Message}` ↔ `ChildFrame{EventBatch,Terminal}`（`Event` 为滚动兼容） |

---

## 9. 演进与延后(roadmap)

已交付的物理部署、ask 和 MCP 代理能力属于兼容链路。#791 已交付规范本地 Child、固定
scoped WSS broker Child Run、干净 Git EnvironmentLease v1、HostRegistry/slot 预留基础。
通用 auto/pinned 调度执行、跨 Host 迁移/故障转移、Docker/SSH/scheduled 统一身份，及
更广的 EnvironmentLease/Artifact Store 仍待验收。联邦式 broker 不在 #791 范围内。
