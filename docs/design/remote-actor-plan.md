# Remote Actor 方案设计:远程拉起 sub-agent

> 本文是 [《Sub-Agent 运行时设计:虚拟 Actor 模型》](./subagent-actor-runtime-design.md) 的**延续设计**。前者定义了 actor 模型本身(虚拟 actor / mailbox / 三层上下文 / 投递面 vs 上下文面);本文只回答一个问题:
>
> **如何让一个 actor 不在本机进程里跑,而在远程主机上拉起并运行?**
>
> 本文是**设计**(非实施步骤)。落地分 P0 / P1 / P2 三阶段,见 §6。

---

## 1. 目标与核心理念

> **actor 的物理位置应当是可配置的"温度",而不是写死的属性。** 同一个 `SubagentProfile`,今天在本机 spawn,明天可以拨向一台 GPU 远程主机——父端代码、线协议、引导契约一行都不用改。

子进程、loopback WebSocket、本地文件 fabric——这三者是当前 actor 模型的**本地实现**,不是 actor 模型本身。本文要做的是:把这三处抽象成 trait,让"远程"成为与"本地"并列的一种实现。

这样,`bamboo actor serve` 起的常驻 service agent、父端 spawn 的 owned child、未来的远程调度池——共享同一套 wire protocol 和 ProvisionSpec 契约,只是**谁拉起、绑在哪、如何被发现**三件事不同。

---

## 2. 现状盘点:什么已远程就绪,什么是本地死结

### 2.1 已经远程就绪(无需改协议 / 契约)

经源码核对(`crates/infra/bamboo-subagent/`),以下组件**天然与物理位置无关**:

| 组件 | 位置 | 为什么已就绪 |
|---|---|---|
| **线协议** `ParentFrame` / `ChildFrame` | `proto.rs` | 纯 JSON-over-WS-text。父→子:`Run{RunSpec}` / `Cancel` / `Message{text}`;子→父:`Event{event}` / `Terminal{status,result,error}`。与传输介质无关 |
| **父端连接** `ChildClient` | `transport.rs:232` | `connect(endpoint)` / `send()` / `next_frame()` 已是干净抽象,底层走 `MaybeTlsStream`——**`wss://` 开箱即用** |
| **执行引擎** `ChildExecutor` trait | `executor.rs` | `run(spec, events, steer, cancel)` 完全不知道"进程在哪跑"。引擎是 Echo / BambooRuntime / CliAdapter 三选一,与位置正交 |
| **引导契约** `ProvisionSpec` | `provision.rs` | 版本化 JSON、**前向兼容**(未知字段忽略、缺省字段兜底)。新增字段不破坏老 worker,父与 worker 二进制不必同步升级 |
| **发现记录** `AgentRecord.endpoint` | `proto.rs` | **endpoint 字段早已存在**,只是当前恒为 `ws://127.0.0.1:<port>`。改成 `wss://gpu-host:8443` 即可 |

### 2.2 三个本地死结(必须改造)

仅有三处把 actor 钉死在本机:

#### 死结 ① 启动 —— `spawn_worker()` `fleet.rs:45`

```rust
pub async fn spawn_worker(
    worker_bin: &Path,        // ← 本机二进制路径
    worker_args: &[String],
    spec: &ProvisionSpec,
    wait: Duration,
) -> TransportResult<SpawnedChild>
```

`Command::new(worker_bin)` + stdin 灌 `ProvisionSpec` + 轮询文件 `Fabric` 等自注册。这是**唯一**硬绑本地的启动入口,`SubprocessChildRunner` / `ActorChildRunner` 都构建在它之上。

#### 死结 ② 绑定 —— `WsServer::bind_loopback()` `transport.rs:46`

```rust
pub async fn bind_loopback() -> TransportResult<Self> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;  // ← 写死 loopback
    ...
}
```

worker 侧只听 `127.0.0.1`,外部不可达。`ws_endpoint()` 本身没问题,只是绑的地址不对。

#### 死结 ③ 发现 —— `Fabric` `discovery.rs`

本地目录里的 `*.json` 文件 + lease 过期做活性(`lease_expires_at` + `gc()`)。**跨机不可见**——远程 worker 无法把记录写进父端的本地目录。

> **核心洞察:把这三处抽象成 trait,远程化就完成了 90%。** 其余(线协议、ProvisionSpec、ChildClient、executor)一行都不用改。

---

## 3. 核心设计:抽象出四个接缝

```
┌──────────────────────── 父端(本地 server / CLI)────────────────────────┐
│                                                                          │
│  WorkerLauncher ──launch(spec)──►  placement?                            │
│                                       │                                  │
│            ┌──────────────────────────┼──────────────────────────┐       │
│            ▼                          ▼                          ▼       │
│   LocalSubprocessLauncher    ConnectLauncher            RegistryLauncher │
│   (现状 spawn_worker)        (拨向常驻 worker)          (向 control plane 申请) │
│            │                          │                          │       │
│            └──────────────┬───────────┴──────────┬───────────────┘       │
│                           ▼                      ▼                       │
│                     ChildClient ◄── resolve endpoint ─► Discovery trait  │
│                   Run / Cancel / Message            (FileFabric / RegistryFabric) │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │ ws://  或  wss:// + Bearer
                               ▼
┌──────────────────────── 远程 worker 主机 ────────────────────────────────┐
│  WsServer::bind(0.0.0.0:PORT) / bind_tls()  ──►  ChildExecutor           │
│  (ProvisionSpec 已就位:identity / executor / model / secrets)            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.1 接缝 ① 启动 —— `WorkerLauncher` trait(替换 `spawn_worker`)

```rust
/// 一个 actor 运行容器的"拉起器"——抽象掉"spawn 本地进程"还是"拨向远程"。
#[async_trait]
pub trait WorkerLauncher: Send + Sync {
    /// 按 spec 拉起 / 连接一个 actor,返回可通信的句柄。
    async fn launch(&self, spec: &ProvisionSpec, wait: Duration)
        -> TransportResult<LaunchedWorker>;
}

/// 拉起结果:一个 ChildClient 连接 + 可选的"杀进程"能力。
/// - 本地 spawn:拥有 Child 进程,kill 可用
/// - 远程 connect:无 owned 进程,kill 为 None(靠 close 连接 + worker 自身 idle 回收)
pub struct LaunchedWorker {
    pub client: ChildClient,
    pub kill_handle: Option<KillHandle>,
}
```

三个具体实现:

| 实现 | 行为 | 阶段 |
|---|---|---|
| `LocalSubprocessLauncher` | 现有 `spawn_worker` 的直接封装,**零行为变化** | P0 |
| `ConnectLauncher` | 不 spawn,直接 `ChildClient::connect(spec.placement.endpoint)`。拉起的是早已 `serve` 的常驻 worker | P1 |
| `SshLauncher` / `K8sLauncher` | 远程拉起一次性进程(spec 走 ssh stdin / pod 创建) | P2(可选) |

**关键:`LaunchedWorker` 统一返回 `ChildClient`**,后续 `Run` / `Cancel` / `Message` 逻辑对所有 placement 完全一致。

### 3.2 接缝 ② 绑定 —— loopback → 可配置 bind + TLS

`WsServer` 增加变体(`bind_loopback` 保留为默认,向后兼容):

```rust
impl WsServer {
    pub async fn bind_loopback() -> TransportResult<Self> { /* 现状,P0 默认 */ }

    /// 绑定任意地址——远程 worker 用 0.0.0.0:PORT。
    pub async fn bind(addr: SocketAddr) -> TransportResult<Self> { ... }

    /// TLS 绑定——远程暴露必须 wss://。
    pub async fn bind_tls(addr: SocketAddr, identity: TlsIdentity) -> TransportResult<Self> { ... }
}
```

`ws_endpoint()` 已返回正确的 `ws://` / `wss://` 串;父端 `ChildClient::connect("wss://...")` **开箱即用**(底层 `MaybeTLSStream` 已支持)。

### 3.3 接缝 ③ 发现 —— `Discovery` trait(替换 `Fabric`)

```rust
/// actor 发现 / 注册面。当前唯一实现是本地文件 fabric。
#[async_trait]
pub trait Discovery: Send + Sync {
    async fn publish(&self, rec: &AgentRecord) -> Result<()>;
    async fn resolve(&self, agent_id: &str) -> Result<Option<AgentRecord>>;
    async fn discover(&self) -> Result<Vec<AgentRecord>>;
    async fn withdraw(&self, agent_id: &str) -> Result<()>;
    async fn gc(&self) -> Result<usize>;
}
```

| 实现 | 行为 | 阶段 |
|---|---|---|
| `FileFabric` | 现状,本地目录 `*.json` + lease 活性。单机默认 | P0 |
| `RegistryFabric` | HTTP 调用 bamboo-server / 独立 broker 的 `/v1/agents` 端点,复用 `lease_expires_at` 语义 | P2 |

### 3.4 接缝 ④ 放置策略 + 鉴权(新增)

**领域模型缺口**:`SubagentProfile`(`bamboo-domain/src/subagent/model.rs`)目前**没有任何"在哪跑"的字段**。需新增放置策略(放 profile 做静态策略,或放运行时配置做按需覆盖):

```rust
/// 一个 actor 该在哪跑。新增字段,前向兼容(老 spec 缺省 = Local)。
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Placement {
    /// 默认,现状:父端本地 spawn。
    #[default]
    Local,
    /// 直连一个已知的常驻 worker(最小可行远程)。
    Remote { endpoint: String },       // wss://gpu-host:8443
    /// 让 control plane 按 role/capacity 分配一个 endpoint。
    Schedulable { pool: String },      // "gpu-pool"
}
```

加进 `ProvisionSpec`(`serde(default)`,缺省 `Local` → 老父端生成的 spec 仍被新 worker 接受)和 `SubagentProfile`(可选,做角色级默认)。

**鉴权**:现状 WS 完全裸连(依赖 loopback 信任)。远程化必须在握手阶段加 `Bearer` token,复用 `ProvisionSpec.secrets` 的 **scoped envelope 模式**(按需下发短时 token,非全量 config):

```
握手: ChildClient::connect → WS Subprotocol "bearer.<token>"  或  首帧 ParentFrame::Auth{token}
校验: worker 比对 spec.identity 下发的预期 token
```

---

## 4. 远程运行时的端到端流程

以 **P1(远程常驻 worker)** 为例——这是最小可行远程,也是最有代表性的:

```
1. 远程主机(一次性):
   bamboo actor serve --bind 0.0.0.0:8443 --tls --token <T>
     └─ WsServer::bind_tls(...) ──► serve() 常驻,接受连接

2. 父端(每次任务):
   spec.placement = Placement::Remote { endpoint: "wss://host:8443" }
   spec.secrets  += worker_auth_token("T")
   launcher = ConnectLauncher;
   worker = launcher.launch(&spec, wait).await?;   // ◄ 不 spawn,直接 connect
   worker.client.send(ParentFrame::Run(RunSpec{...})).await?;

3. 远程 worker:
   handle_conn → ChildExecutor::run(spec, events, steer, cancel)
   流式回吐 ChildFrame::Event ... → ChildFrame::Terminal

4. 父端:
   while let Some(frame) = worker.client.next_frame().await { ... }
   worker.client.close().await;
```

> **步骤 2 之后到 4,与本地模式逐字节相同**——`ParentFrame`/`ChildFrame`/`RunSpec`/`ChildClient` 全部复用。唯一区别:`launch` 内部是 `Command::new` 还是 `connect`。

---

## 5. 安全考量(远程化的真正成本)

裸 loopback 信任模型在远程化后**必须**收紧:

| 风险 | 现状 | 远程化要求 |
|---|---|---|
| **投递面暴露** | loopback,信任本机 | `wss://` TLS + `Bearer` token,否则任何人可投递 `Run` |
| **凭证泄露** | stdin 传 spec,本机内 | TLS 加密链路 + `ProvisionSpec.secrets` 保持 scoped envelope(按需,非全量 config) |
| **证书信任** | 无 | worker 自带 identity(`bind_tls`),或 mTLS 双向校验 |
| **活性伪造** | 本地文件,本机信任 | 注册需持 token;`lease_expires_at` + `gc()` 复用,过期即摘除 |
| **资源隔离** | 本机进程 | 远程 worker 的工作目录 / 凭证仍按 `spec.workspace` / `spec.secrets` 隔离,不继承远程主机全量环境 |

**纪律保持**:`provision.rs` 现有原则——"凭证从不在 argv(可见于 `ps`)或 env(被子进程继承),只走 stdin 一次性 envelope"——远程化同样适用:token 走 WS 握手 / spec envelope,不进远程主机的环境变量。

---

## 6. 分阶段落地

每阶段**独立可交付、可测试、无行为回退**:

### P0 — 抽象(不增能力)

把三个死结包成 trait,默认 impl **逐行复刻现状**。

- `WorkerLauncher` trait + `LocalSubprocessLauncher`(封装现有 `spawn_worker`)
- `Discovery` trait + `FileFabric`(封装现有 `Fabric`)
- `WsServer::bind(addr)` 变体(`bind_loopback` 保留默认)
- `Placement` 枚举加入 `ProvisionSpec`(`serde(default) = Local`)

**验收**:现有 e2e 测试(`tests/subagent_worker_e2e.rs`、`tests/subagent_actor_via_server.rs`)全绿,**无行为变化**。`cargo test -p bamboo-subagent` + server 集成测试通过。

### P1 — 远程常驻 worker(最小可行远程)

- worker 端:`bamboo actor serve --bind 0.0.0.0:PORT --tls --token`
- 父端:`ConnectLauncher` + `Placement::Remote`
- `bind_tls()` + WS `Bearer` 握手

**验收**:跨机器——远程 `serve` → 本地父端 `Run` → `Terminal` 回流;`cargo test` 新增 `remote_connect_e2e`(两进程,模拟跨机)。

### P2 — Control Plane(可选,规模化)

- bamboo-server 增 `/v1/agents` 注册 / 调度端点
- `RegistryFabric` + `Placement::Schedulable`
- 调度器:按 `role` / `capacity` / 健康分配 endpoint

**验收**:两个 worker 节点注册,父端按 role 自动路由;节点下线后 lease 过期、`gc()` 摘除。

---

## 7. 开放问题

| # | 问题 | 倾向 |
|---|---|---|
| 1 | `Placement` 放 `SubagentProfile`(角色级静态)还是运行时配置(按需覆盖)? | profile 设默认 + 运行时可覆盖 |
| 2 | TLS 证书:worker 自带 identity vs mTLS 双向校验? | P1 先单向(P0→P1 最小化),P2 按需 mTLS |
| 3 | 远程 worker 的 `reusable` 池化:复用现有 `serve_reusable_with_idle_timeout`,还是 control plane 托管池? | P1 复用现有池化逻辑,P2 托管 |
| 4 | `SshLauncher` 是否纳入主线? | 可选,优先 `ConnectLauncher`(常驻)与 `RegistryLauncher`(调度) |

---

## 参考

- [《Sub-Agent 运行时设计:虚拟 Actor 模型》](./subagent-actor-runtime-design.md) —— 本设计的基础(actor 模型 / mailbox / 三层上下文)
- `crates/infra/bamboo-subagent/src/` —— 当前实现源码
- `docs/reviews/actor-runtime-self-review-2026-06-12.md` —— actor runtime 自审
