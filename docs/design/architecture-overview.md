# Architecture Overview — Broker-mediated remote sub-agents

> 本文是面向"怎么部署 / 项目结构 / 部署能力实现"的总览。
> 配套设计文档:`docs/remote-mailbox-broker-design.md`(broker 设计 + SHIPPED 段)、
> `docs/remote-actor-plan.md`(远程 actor 接缝 P0/P1/P2)。

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
   │  · 工具 deploy_agent       │     │                                 │
   │         ask_agent          │     │                                 │
   └────────────────────────────┘     └─────────────────────────────────┘
```

| 角色 | 子命令 | 职责 |
|---|---|---|
| **broker** | `bamboo broker serve` | 网络消息总线 + 持久邮箱;鉴权;路由/推送 |
| **orchestrator** | `bamboo serve`(根 agent) | 跑 root agent loop + 真实 MCP servers + MCP 代理服务;暴露 `deploy_agent`/`ask_agent` 工具 |
| **worker** | `bamboo broker-agent serve` | 被部署到本地/Docker/远端,连 broker,执行被指派的任务 |

**位置无关**是核心:worker 起来后只认 `broker endpoint + token + 自己的 id`;编排者只按
id 寻址。这是 **push 模型**(master 主动部署执行环境),不是互相发现。

---

## 2. 一次部署的端到端流程

```
编排者 LLM
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

### `crates/app/bamboo-server-tools` — LLM 可调用工具
| 文件 | 内容 |
|---|---|
| `ask_agent.rs` | `ask_agent` 工具(query/steer 指挥别的 agent)|
| `deploy_agent.rs` | `deploy_agent` 工具(deploy/stop/list + `DeployedRegistry` 保活)|

### `crates/app/bamboo-server` — 编排者
| 位置 | 内容 |
|---|---|
| `app_state/builder.rs` | 配了 `subagents.broker` 就 spawn `serve_mcp_proxy`(backend = 真 `McpToolExecutor`)|
| `app_state/tools.rs` | Root 工具面叠 `ask_agent` + `deploy_agent`（仅当配了 broker）|

---

## 4. 部署能力的实现原理

### ① `Deployer` trait —— "在某环境拉起 worker" 的抽象(`deploy.rs`)
三个实现生成**同一条** `bamboo broker-agent serve …` 命令,只是放在不同环境跑:
```
LocalProcessDeployer   Command::new(bamboo_bin).args(…)                         本机子进程
DockerDeployer         docker run --rm --network host -v ~/.bamboo:ro <image> … 容器
SshDeployer            ssh -tt host 'BAMBOO_BROKER_TOKEN=… bamboo …'(shell 转义) 远端
```
- **token 走环境变量,绝不进 argv**(`ps` 看不到)。
- 返回 `DeployedAgent`:`kill_on_drop` 子进程句柄 + 可选清理命令(docker `rm -f`)。

### ② 保活注册表(`deploy_agent.rs` 的 `DeployedRegistry`)
`DeployedAgent` 是 kill-on-drop 的——部署完若丢弃句柄,进程立刻被杀。所以工具把句柄存进
`Arc<Mutex<HashMap<id, DeployedAgent>>>`,**随 server 生命周期存活**;`action=stop` 取出并
优雅关闭,`action=list` 枚举。

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

## 5. CLI 命令面

```bash
# 中心 broker(可对外)
bamboo broker serve --bind 0.0.0.0:9600 --token $T          # 或 BAMBOO_BROKER_TOKEN

# 部署一个干活的 worker(本地/远端/容器),它拨回 broker
bamboo broker-agent serve --broker ws://host:9600 --token $T \
    --id w1 --model anthropic:claude-sonnet-4-6              # 或 --echo 冒烟
    [--mcp-proxy bamboo-orchestrator]                        # 把 MCP 代理回编排者

# 编排者:config 里设 subagents.broker {endpoint, token}
#   → root agent 自动获得 deploy_agent + ask_agent 两个工具
```

工具(LLM 在 loop 内调用):
- `deploy_agent(action=deploy, env=local|docker|ssh, model, image?, host?, echo?)` → `{id}`
- `deploy_agent(action=stop, id)` / `deploy_agent(action=list)`
- `ask_agent(target=id, question, mode=query|steer, timeout_secs?)` → `{answer}`

---

## 6. 配置(`subagents` 段)

```jsonc
"subagents": {
  "max_concurrent": 200,
  "broker": { "endpoint": "ws://broker-host:9600", "token": "…" },  // 启用 broker 工具 + MCP 代理服务
  "mcp_role_allowlist": [                                          // 可选(issue #54):按角色收窄代理工具面
    { "role": "researcher", "tools": ["fetch_url"] },
    { "role": "sandboxed", "tools": [] }                           // 空数组 = 显式锁死(0 工具)
  ]
}
```
配了 `subagents.broker` 才会:Root 工具面挂上 `deploy_agent`/`ask_agent`,且 server 启动
`serve_mcp_proxy`。

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

actor wire 使用 `ActorEventBatch`，路由键为 logical session，fence 由
`activation_id + execution_epoch + source_actor_id` 组成，batch 内用单调 `seq` 排序。本地 actor
和 `Schedulable` Cluster actor 走 broker path；迟到的旧 worker frame 会在 host 侧被拒绝。
固定 `Placement::Remote` 目前仍走 direct WebSocket，只共享 batch wire，尚未统一到 broker 的
durable path。

| QoS | 典型事件 | 传输语义 |
|---|---|---|
| `durable` | tool/permission/terminal boundary 与未知事件 | Maildir + receipt；背压，不丢 |
| `snapshot` | runner/token-budget/context-pressure gauge | 有界 live lane；过载可丢，后续 batch 的 sequence gap 可暴露丢失 |
| `ephemeral` | token/reasoning token/heartbeat | 有界 live lane；批量，过载可丢 |

`task_list_item_progress` 是 delta，不是全量 snapshot，因此走 `durable`。当前 host 对 sequence gap
只记录告警；“向前端发出 reload control 并从 session snapshot 恢复”仍是下一阶段契约，不能把
现有告警等同于已经完成恢复。

每个 worker 固定使用一条 inbound subscription，加 control/event 两条 outbound uplink；连接数不再随
并行 Run 数量线性增加。control 与 event 队列独立，cancel/approval/admission 不会排在 token 后面；
同一 Run 的 live batch、durable event、Outcome 仍在 ordered event lane 上保持顺序。默认允许 200 个
active actor，但 warm-idle pool 单独限制为 16，避免为了并发上限长期保留 200 个空闲进程。

### 7.1 当前边界与下一阶段

当前实现解决的是事件放大、队列无界增长和 worker uplink 随 Run 增长的问题，尚不是完整的
可重放 Session Home。合并更高并发 Cluster 执行前还需要：

1. `Outcome` 携带最终 event-seq watermark；尾部 live batch 丢失时也能检测，而不依赖下一批事件。
2. durable Event/Outcome 在 host 验证、幂等 apply 并 checkpoint cursor 后再 ACK；不能在
   `BrokerChildLink` 解码后立即 ACK。
3. control/event uplink 由统一 supervisor 管理；关键 lane 断开即撤销 worker readiness，Outcome
   投递失败不得把原 Run 标成已处理。
4. executor 目前已有本地 Run-slot 隔离：安全默认值为 1，只有无共享可变状态的实现可显式提高；
   因此真实 `BambooRuntimeExecutor` 不会并行复用 permission config / escalation bridge。下一步仍需
   worker 对外发布 `max_slots` 并由 scheduler 原子租约；全局 200 并行由多个 worker/slot 提供。
5. parent ingress 改为少量 session-hash shard，而不是每 Run 一条 WebSocket；否则 200 个本地
   worker 的 broker accepted FD 加 host client FD 会逼近常见的 `RLIMIT_NOFILE=1024`。
6. 持久化 `session_id -> home_node + home_epoch + lease`；frontend 使用
   `snapshot + cursor + live tail`，非 home gateway proxy 或订阅共享 session topic。

此外，`source_node_id` 尚未由 broker 可信地 stamp，worker role/actor identity 仍是 bearer-token
连接自报。跨节点 fencing 最终应绑定认证连接身份，而不是只信任 payload。

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

## 8. 演进与延后(roadmap)

已交付:**Change A**(actor-only)→ **Phase 0**(远程接缝)→ **Phase 1**(broker + serve + 部署 + ask)
→ **P1**(skills + URL MCP 同步)→ **P2**(MCP-over-broker 代理)。

明确延后到 **P3**:
- 用 scoped secrets envelope 取代 Docker 整目录挂载(避免把全量密钥暴露给容器)
- ssh/远端的 skills bundle 投递(content-addressed)
- 按 subagent profile / role 的 allowlist 收窄同步集
- MCP 代理断线重连;manifest 只暴露 stdio(现在暴露全部,功能正确但 SSE 多一跳)
- `bind_tls`/`wss://` + 远端 `ConnectLauncher`/`Placement::Remote` 全链路(remote-actor-plan.md P1)
- 联邦式 broker 互相指挥 —— **明确不做**(中心辐射)
