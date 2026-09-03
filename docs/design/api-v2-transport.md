# API v2 传输层设计:统一 WS 传输 + Rust 自治 TLS

> 本文是 Zenith 前后端通信 v2 的**实现级 RFC**。它把已锁定的架构决策固化成一份可施工的规范,
> 覆盖**客户端面**(桌面 lotus + 移动端 → bamboo)与 **broker/actor 面**(bamboo ↔ 远程 worker)两条传输腿。
>
> 设计延续并复用既有成果:
> - [《Sub-Agent 运行时设计:虚拟 Actor 模型》](./subagent-actor-runtime-design.md) —— actor 模型基础
> - [《Remote Actor 方案设计》](./remote-actor-plan.md) —— broker/actor 远程化的 trait 接缝与 P0/P1/P2 分阶段
> - [《Remote Mailbox Broker 设计》](./remote-mailbox-broker-design.md) —— broker 的线协议(已 SHIPPED)
>
> 本文只回答:**如何把客户端面从「两条裸 SSE」收敛成「一条压缩 WSS」,并让 broker 面顺 `remote-actor-plan` 的 P1 完成 TLS 化——全部由 bamboo 一个二进制自治承载。**

---

## 0. 决策摘要(全部已锁定)

| # | 决策点 | 锁定值 |
|---|---|---|
| 1 | 范围 | 桌面 lotus + 移动端 + broker 全部迁移 v2 |
| 2 | 后端拓扑 | **无 bodhi-server**(Go);bamboo(Rust/actix-web,:9562)是**唯一后端**,直接公网暴露 |
| 3 | TLS | **Rust 自治**:bamboo 内嵌 rustls 自己终止 TLS,无 Caddy/反向代理 |
| 4 | 证书 | **手动证书文件**(现阶段);`server.tls.cert_file`/`key_file`;ACME 自动签发**推迟** |
| 5 | 认证 | **per-device token** 取代单一共享 `access_password` |
| 6 | 客户端面传输 | 2×SSE(`/events/{id}` + `/stream`)→ **1×WSS 多路复用** + `permessage-deflate` + token 合帧 |
| 7 | broker 面传输 | 顺 `remote-actor-plan` P1(`bind_tls` + `Placement::Remote` + Bearer 握手),加 deflate |

---

## 1. 目标与现状

### 1.1 要解决的问题

bamboo 要**直接暴露在公网**,供移动端(弱网、流量贵、公网 IP)与桌面端使用,并支撑远程 sub-agent worker。当前传输层在公网场景下有四类硬伤:

| 问题 | 现状(源码) | 公网后果 |
|---|---|---|
| **无 TLS** | `web_service.rs:79` `.bind()` 裸 TCP;`bamboo-broker/server.rs:57` 裸 WS;`transport.rs:54` 注释「TLS later phase」 | 凭据明文,可嗅探/篡改 |
| **无压缩** | 三条腿全 JSON 文本,无 gzip/brotli/deflate;`stream/response.rs:90` 还主动设 `no-transform` | JSON 流量白白多传数倍 |
| **SSE 逐 token 小帧** | `events/stream.rs:33` `data:{json}\n\n` 每 token 一帧;一次 500 token 回答 ≈ 500 帧 × ~120B 结构开销 ≈ **60KB 纯开销** | 移动端无线电反复唤醒,TLS 无法凑包 |
| **两条并发长连接** | 桌面同时挂 `/events/{id}` + `/stream` 两条 SSE | 双倍 keepalive 流量 + 双倍无线电唤醒 |
| **单一共享密码** | `access_control.rs:251` `access_password` + cookie;`local_bypass` 让 loopback 跳过 | 泄露即全失守,无法按设备吊销 |

### 1.2 v2 目标(可量化)

- 公网强制 TLS;**单一共享密码 → per-device 可吊销 token**。
- 客户端面:**一条 WSS** 替代两条 SSE;token 合帧 + deflate,一次对话下行 **60KB → < 15KB**(移动端基准)。
- broker 面:远程 worker 走 `wss://` + Bearer;转发事件流加压缩,降低「转发贵」。
- **不改任何逻辑事件 schema**(`AgentEvent` / `ChangeEvent` / `ClientFrame` / `BrokerFrame` / `ParentFrame` / `ChildFrame` 逐字节复用)——v2 只换**传输与编码**,不动业务语义。

---

## 2. 架构总览

```
                         公网
                          │
            ┌─────────────┴──────────────┐
            │  TLS 终止(rustls,手动证书) │   ← bamboo 自治,无 Caddy
            ▼                            ▼
   ┌─────────────────────────────────────────────┐
   │            bamboo  (:9562)                  │
   │   唯一后端 + 边缘 + broker                   │
   │                                             │
   │  面① 客户端面  /v2 (新增)                    │
   │    单条 WSS,channel 多路复用:               │
   │      • feed         (← 替代 /stream)         │
   │      • agent.{sid}  (← 替代 /events/{sid})   │
   │      • control      (← 替代零散 POST)        │
   │    + permessage-deflate + token 合帧         │
   │    + per-device token 认证                   │
   │                                             │
   │  面② broker/actor 面  (remote-actor P1)      │
   │    wss:// + Bearer + deflate                 │
   │    ParentFrame/ChildFrame 逐字节不变         │
   │                                             │
   │  面③ 旧 /v1 (保留过渡)                       │
   │    SSE ×2,仅限 loopback 桌面端               │
   └─────────────────────────────────────────────┘
        │                              │
   桌面 lotus / 移动端              远程 worker
   (bamboo.v2 JSON + deflate)      (wss + Bearer)
```

**关键原则**:三条腿共享同一套「WS + 协商编码 + deflate」传输栈,只是握手与帧语义不同。broker 面**已实现** WS(`bamboo-broker/server.rs`),客户端面从 SSE **迁移到** WS——两者最终落在同一传输原语上。

---

## 3. TLS 与证书(Rust 自治)

### 3.1 设计

bamboo 内嵌 rustls,自己终止 TLS。**无 Caddy、无独立反向代理**。证书现阶段为手动文件;ACME 自动签发(`rustls-acme`)推迟到后续阶段。

### 3.2 配置(新增字段)

在 `crates/infra/bamboo-config/src/config.rs` 的 `ServerConfig` 新增 `tls`:

```rust
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_bind")]
    pub bind: String,
    pub static_dir: Option<PathBuf>,
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// v2: TLS 配置。两者都给 → Rustls H1;缺省 → 明文 H1(桌面 loopback 不受影响)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// 手动证书(现阶段)。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// PEM 证书链(全链:leaf → intermediates → root)。
    pub cert_file: PathBuf,
    /// PEM 私钥。
    pub key_file: PathBuf,
}
```

**配置示例**(`config.json` 的 `server` 段):

```json
{
  "server": {
    "port": 9562,
    "bind": "0.0.0.0",
    "tls": { "cert_file": "/etc/bamboo/cert.pem", "key_file": "/etc/bamboo/key.pem" }
  }
}
```

### 3.3 接入点(当前实现)

**客户端面(actix-web)**——4 个启动入口统一通过
`server/h1.rs::build_h1_server` 构建服务：明文监听器使用
`HttpService::build().h1(...).tcp()`，TLS 监听器使用
`HttpService::build().secure().h1(...).rustls_0_23(...)`。`web_service.rs`
与 `entrypoints.rs` 不再各自选择 Actix 的高层 `bind/listen_rustls` 方法，
因此中间件、路由、worker 数、IPv4/IPv6 listener 与关闭生命周期仍由同一
app factory 驱动，而协议选择只有一个权威位置。

> **入站协议边界（#849）**：Bamboo 的 HTTP/WSS face 明确只支持 HTTP/1.1。
> Actix Web 4 的高层 Rustls feature 会强制启用仍依赖 `h2 0.3` 的 HTTP/2
> feature，该版本受 RUSTSEC-2026-0258 影响。Bamboo 的统一 WebSocket face
> 使用 HTTP/1.1 Upgrade，因此禁用入站 HTTP/2 不影响 WSS 合同；provider、
> MCP 等出站 Reqwest 客户端继续使用已修复的 `h2 0.4` HTTP/2 栈。不得通过
> audit ignore 或本地 fork 绕过此边界。

统一在 `server/tls.rs` 构建 `rustls::ServerConfig`，并把 ALPN 固定为
`http/1.1`：

```rust
fn build_rustls(tls: &TlsConfig) -> Result<actix_web::rustls::server::ServerConfig, String> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(File::open(&tls.cert_file)?))
        .collect::<Result<Vec<_>, _>>().map_err(|e| format!("read cert: {e}"))?;
    let key = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(File::open(&tls.key_file)?))
        .next().ok_or("no key in key_file")??;
    let provider = Arc::new(ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key.into())
        .map_err(|e| format!("rustls config: {e}"))?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(cfg)
}
```

**判定逻辑**(复用 `listeners.rs` 既有双模式):

```
server.tls 给齐 → Rustls + HTTP/1.1(0.0.0.0:port)  // 公网暴露模式
server.tls 缺省 → 明文 HTTP/1.1(127.0.0.1:port)    // 桌面 loopback 模式(行为不变)
```

桌面本地开发 `server.tls` 缺省,**零行为回退**;公网部署补两张 PEM 即可。

**broker 面(tokio-tungstenite)**——`bamboo-broker/server.rs:57`:

```rust
// 现: accept_async(stream)
// v2:
let acceptor = build_tls_acceptor(&tls)?;           // tokio_rustls::TlsAcceptor
let tls_stream = acceptor.accept(stream).await?;
let ws = accept_async(tls_stream).await?;
```

`bamboo-subagent/src/transport.rs` 的 `WsServer` 增 `bind_tls(addr, TlsIdentity::Files{cert,key})`(即 `remote-actor-plan §3.2` 已规划的变体),复用同一 `TlsConfig`。

### 3.4 证书生命周期(手动,现阶段)

| 项 | 现阶段 |
|---|---|
| 签发 | 管理员外部获取(Let's Encrypt `certbot certonly` / 自签 / 私有 CA) |
| 续期 | 外部(cron 重跑 certbot)后**重启 bamboo** 生效(暂不做热加载) |
| 热加载 | **不做**(后续可加 `SIGHUP` 重载,作为独立 enhancement) |
| 失败处理 | `server.tls` 存在但文件缺失/解析失败 → **拒绝启动**(fail-fast,不静默降级到明文) |

> 后续 `rustls-acme` 自动签发时,只需新增 `TlsConfig::Acme { domain, .. }` 变体,接入点不变。

### 3.5 依赖新增

`bamboo-server` / `bamboo-broker` / `bamboo-subagent` 的 `Cargo.toml`:

```toml
tokio-rustls = "0.26"
rustls = "0.23"
rustls-pemfile = "2"
# client face 通过 actix-http 的 H1 Rustls service，actix-web 不启用 http2
actix-http = { version = "3", default-features = false, features = ["rustls-0_23"] }
actix-server = "2"
```

---

## 4. 身份认证 v2:per-device token

### 4.1 模型

bamboo 保持 **local-first 单用户**(一个实例 = 一个 owner)。认证从「单一共享 `access_password`」升级为 **per-device token**:

- **owner 根密码** = 现有 `access_control.password_hash`(保留,作为 root 凭据,授权配对)。
- **device token** = 每台设备(手机/桌面/远程 worker)一个**不透明随机串**,服务端存 hash,可单独吊销。
- 泄露一台设备 → 吊销那一个 token,root 密码与其他设备不受影响。

### 4.2 数据模型

扩展现有 `AccessControlConfig`(`config.rs:99`),持久化进统一 `config.json`(复用 `config_manager.rs` 的合并写):

```rust
pub struct AccessControlConfig {
    pub password_enabled: bool,
    pub password_hash: Option<String>,     // owner root 密码 hash(保留)
    pub password_salt: Option<String>,
    pub updated_at: Option<String>,

    /// v2: 已签发的设备 token。空 = 仅 root 密码模式(向后兼容旧实例)。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<DeviceCredential>,
}

pub struct DeviceCredential {
    pub device_id: String,          // 服务端生成(bamboo_<12 hex>)
    pub label: String,              // 人类可读:"iPhone 15"、"MacBook"
    pub token_hash: String,         // SHA-256(token || device_salt)
    pub token_salt: String,
    pub created_at: String,         // RFC3339
    pub last_used_at: Option<String>,
    pub revoked: bool,
}
```

**Token 格式**(仅创建时返回一次,服务端只存 hash):

```
bd1_<32 hex>          // 例如 bd1_4f8a...e2c9
```

> 与现有 `password_hash`/`password_salt` 同一套 hash+salt 机制,不引入新加密依赖。

### 4.3 配对流程(默认方案,供 review)

复用 broker 的 **Hello 握手**模式(`bamboo-broker/server.rs:64`),把它推广到客户端面:

```
1. 首次配对(新设备):
   POST /v2/pair   { "root_password": "<owner 密码>", "label": "iPhone 15" }
   → 200 { "device_id": "bamboo_...", "device_token": "bd1_...", "expires_hint": "rotate-on-demand" }
   服务端校验 root 密码 → 签发 device token(存 hash)

2. 正常连接(WSS 握手首帧):
   Client → { "type": "hello", "device_id": "...", "token": "bd1_..." }
   Server 校验 token hash → 在服务端绑定连接身份 = device_id

3. 后续设备配对(已有已认证设备在场):
   已认证设备生成一次性 6 位配对码:
   POST /v2/pair/code   (持已有 device_token)
   → { "code": "842913", "ttl": 120 }
   新设备:
   POST /v2/pair   { "code": "842913", "label": "iPad" }
   → 签发新 device token
```

**loopback 桌面端**:`local_bypass` 语义保留——本地连接免 token(桌面开发零摩擦),仅公网连接强制 device token。

成功通过授权门的首个 `hello` 会收到精确的顶层帧
`{"type":"welcome"}`。JSON 子协议使用文本帧，MessagePack 子协议使用同形状的
named-map 二进制帧。`welcome` 由连接唯一的 WebSocket writer 直接写出，不经过
可丢弃的 heartbeat/sys 队列；写出失败即关闭连接。每个 socket 最多发送一次
`welcome`：后续合法或无 token 的 `hello` 不重复 ACK，但后续携带 credential 的
`hello` 仍会重新验证，无效 credential 仍立即关闭。

`welcome` 只表示该 socket 已通过权威 hello/auth gate，不携带 token、device id、
credential metadata、服务配置或 channel 数据。客户端不能以 socket open、首个
业务事件或 pong 推断认证成功；支持这一契约的服务端在
`GET /api/v1/bootstrap` 广告 `auth.ws_hello_ack.v1`。

### 4.4 管理

复用现有 admin 路由模式,新增 `/v2/devices`(GET 列表 / DELETE 吊销 / POST 轮换)。吊销即把 `revoked=true`,连接握手即时拒绝。

### 4.5 中间件升级

`handlers/settings/access_control.rs:251 enforce_access_password_middleware` 升级为 `enforce_auth_middleware`:

- loopback + 无 `server.tls` → 保持 `local_bypass`(桌面模式,零回退)
- 公网(`server.tls` 存在)→ 走 device-token 校验;`/v2/pair*` 与 `/health` 走公开白名单(参照现有 `is_public_access_route`)

---

## 5. 客户端面传输 v2:单条 WSS 多路复用

### 5.1 一条连接,N 个 channel

把现在的两条 SSE 收敛成**一条 WSS**,内部按 `channel` 多路复用。订阅/退订在同一连接内动态进行。

| channel | 方向 | 承载 | 替代的旧接口 |
|---|---|---|---|
| `feed` | 服务端→客户端 | `ChangeEvent`(账户变更流) | `GET /stream` SSE |
| `agent.{session_id}` | 服务端→客户端 | `AgentEvent`(逐 token / 工具 / 任务) | `GET /events/{id}` SSE |
| `control` | 双向 | 停止 / 审批 / 编辑 / 配对码 | 零散 `POST /stop`、`POST /child-approval/*` 等 |

### 5.2 路由

```
GET wss://<host>/v2/stream        # 唯一长连接入口
```

握手(query 协商编码/合帧):

```
Sec-WebSocket-Protocol: bamboo.v2                    # JSON 文本(桌面默认)
Sec-WebSocket-Protocol: bamboo.v2.msgpack            # 二进制(移动端/远程默认)
Sec-WebSocket-Extensions: permessage-deflate         # 压缩
?batch_ms=50                                          # token 合帧窗口(桌面 0 / 移动 50)
```

### 5.3 帧封装(包裹既有 schema,**不改业务事件**)

信封是新增的一层薄壳;内层 `event` 字段**逐字节复用**现有 `AgentEvent` / `ChangeEvent` 的 JSON:

```jsonc
// 服务端 → 客户端
{ "type": "welcome" }       // 首个已授权 hello 的可靠 ACK；每个 socket 最多一次
{
  "ch": "agent.sess_abc",     // channel
  "seq": 42,                   // 该 channel 的单调序号(用于断线续传)
  "event": { "type": "token", "content": "Hello" }   // ← 现有 AgentEvent 原样
}
{
  "ch": "feed",
  "seq": 1007,
  "event": { "type": "session_created", "session_id": "...", "ts": 1719000000 }  // ← 现有 ChangeEvent
}
{
  "ch": "agent.sess_abc", "seq": 43,
  "control": { "type": "terminal", "reason": "complete" }   // 终止标记
}
```

```jsonc
// 客户端 → 服务端
{ "type": "hello", "device_id": "...", "token": "bd1_..." }   // 首帧(认证)
{ "type": "subscribe", "ch": "feed", "since": 1006 }          // 续订阅,带 cursor
{ "type": "subscribe", "ch": "agent.sess_abc" }               // 订阅某会话实时流
{ "type": "unsubscribe", "ch": "agent.sess_abc" }
{ "type": "execute", "session_id": "sess_abc", "message": "..." }   // ← 现有 execute 语义
{ "type": "stop", "session_id": "sess_abc" }                        // ← 现有 stop
{ "type": "approve", "child_session_id": "...", "decision": "allow" }
```

> **msgpack 模式**:同一信封用 MessagePack 编码为 WS 二进制帧。schema 不变,仅序列化层切换。桌面默认 JSON(可读、易调试),移动端默认 msgpack(更小)。

### 5.4 断线续传(cursor 协议)

WS 无 `Last-Event-ID`,改为**订阅帧带 cursor**。两个 channel 复用现有续传机制,无需新发明:

| channel | cursor 来源 | 续传机制(已有) |
|---|---|---|
| `feed` | `ChangeEvent.seq` | `stream/response.rs:46 plan_replay` 的 journal 重放 + `feed_reset`(逐帧复用) |
| `agent.{sid}` | 每 session 单调序号 | `events/handler.rs:57` 的 `critical_events_to_replay` 缓存重放(逐帧复用) |

客户端重连后发 `{ "type":"subscribe", "ch":"feed", "since":<上次最大 seq> }`,服务端先补漏再追尾——与现有 SSE 行为**完全一致**,只是承载从 `EventSource` 换成 WS 帧序号。

### 5.5 token 合帧(coalescing)——移动端最大收益

现状每个 `Token`/`ToolToken` 事件单独成帧。v2 在**服务端传输层**加一个可配窗口:

```
batch_ms 窗口内,同一 channel 的 Token/ReasoningToken/ToolToken 事件:
  合并为单帧, content 字段拼接
  其余事件(ToolStart/Complete/NeedClarification/...)不合并,立即发
```

| 客户端 | `batch_ms` | 效果 |
|---|---|---|
| 桌面 lotus | 0 | 立即发,体感与现状一致 |
| 移动端 | 50 | 一次 500 token 回答:500 帧 → ~20-40 帧,结构开销 −90% |

合帧在 `events/stream.rs` 的帧出口处实现,逻辑事件层无感知。

### 5.6 心跳与保活

WS 自带 ping/pong(15s,沿用现有 SSE 的 `[KEEPALIVE]` 间隔语义),取代文本 keepalive 帧。一条连接一次心跳,替代原来两条 SSE 的双心跳。

### 5.7 与旧接口的对应(实现锚点)

| 旧接口(保留) | v2 channel | 复用逻辑 |
|---|---|---|
| `events/stream.rs`(per-session SSE) | `agent.{sid}` | `AgentEvent` 序列化 + `critical_events_to_replay` |
| `stream/response.rs`(account feed SSE) | `feed` | `plan_replay` + journal + broadcast |
| `POST /execute/{id}` / `POST /stop` / `POST /child-approval/*` | `control` 上行 | handler 直接复用 |

---

## 6. broker/actor 面传输 v2(顺 remote-actor-plan)

客户端面是本文的主体新工作;**broker 面不重发明**——执行 `remote-actor-plan.md` 的 P1,额外只要求「开 deflate」。

### 6.1 现状(已远程就绪的部分)

- 线协议 `ClientFrame`/`BrokerFrame`(`bamboo-broker/proto.rs`)、`ParentFrame`/`ChildFrame`(`bamboo-subagent/proto.rs`):纯 JSON-over-WS-text,与位置无关。
- `ChildClient::connect(endpoint)`(`transport.rs:232`):底层 `MaybeTlsStream`,**`wss://` 开箱即用**。
- `ProvisionSpec`(`provision.rs`):版本化、前向兼容。

### 6.2 v2 增量(remote-actor P1 + 本文补充)

| 项 | remote-actor-plan 已规划 | 本文补充 |
|---|---|---|
| 绑定 | `WsServer::bind_tls(addr, identity)`(§3.2) | identity = `TlsConfig::Files`(§3.3 同一套证书) |
| 鉴权 | 握手 Bearer(`§3.4`,scoped envelope) | token 即 §4 的 device token(远程 worker 也按设备管理) |
| 放置 | `Placement::Remote{endpoint}`(§3.4) | 无新增 |
| 拉起 | `ConnectLauncher`(§3.1) | 无新增 |
| **压缩** | (未涉及) | **broker 与 worker 两端 WS accept/connect 启 `permessage-deflate`** |
| **合帧** | (未涉及) | broker 转发的 `ChildFrame::Event`(逐 token)按 `batch_ms` 合帧——**直接降低「转发贵」** |

**压缩/合帧是本文对 broker 面的唯一净增**:远程 worker 的 token 事件流经 broker 中继时,逐 token 小帧同样浪费公网带宽;给中继链路加 deflate + 合帧,转发成本与客户端面同等比例下降。

### 6.3 线协议不变性(纪律)

`ParentFrame`/`ChildFrame`/`ClientFrame`/`BrokerFrame` 的 **schema 一行不改**。deflate 是 WS 扩展层、msgpack 是可选 subprotocol 协商——都在传输层,不触碰业务帧。`remote-actor-plan`「步骤 2 之后到 4 与本地模式逐字节相同」的承诺保持成立。

---

## 7. 编码与压缩协商(三条腿统一)

### 7.1 压缩:permessage-deflate(默认全开,零协议改动)

WS 标准 RFC 7692 扩展,握手自动协商。对 JSON 文本通常 −60~80%。三条腿(客户端 WS / broker WS / worker WS)统一开启。

> 现状 `accept_async(stream)` 不带配置 → 不协商 deflate。v2 改为带 `WebSocketConfig`/扩展回调开启。

### 7.2 编码:JSON 文本(默认)vs MessagePack(可选)

| subprotocol | 编码 | 谁用 | 在 deflate 基础上 |
|---|---|---|---|
| `bamboo.v2` | JSON 文本(现状一致) | 桌面 lotus、broker、worker | 已省 60-80% |
| `bamboo.v2.msgpack` | MessagePack 二进制 | 移动端、可选远程 worker | 再省 ~40% |

两端在 WS 握手 `Sec-WebSocket-Protocol` 协商;信封 schema 不变,仅序列化层。

### 7.3 收益预估(一次 500-token 对话下行)

| 方案 | 估算下行 |
|---|---|
| 现状(逐 token SSE 裸 JSON,两连接) | ~60KB + 双心跳 |
| + token 合帧 50ms | ~12KB |
| + permessage-deflate | ~4-6KB |
| + msgpack | ~2-3KB |
| 连接数 | 2 → 1(心跳/无线电 −50%) |

---

## 8. 迁移与兼容

### 8.1 双轨并存

```
/v1      (SSE ×2 + REST) ── 仅供遗留 Lotus 过渡,标记 deprecated
/api/v1  (REST)          ── Lotus Next 全 surface 的 canonical HTTP API
/v2      (单 WSS)        ── Lotus Next 全 surface 的 realtime transport
```

- **Lotus Next**:浏览器、嵌入式 WebView、桌面与移动端共享同一套
  `/api/v1` + `/v2/stream` contract；surface 不再决定协议或 feature tree。
- **遗留 Lotus**:迁移期间可继续使用 `/v1`/SSE，但 Lotus Next 不对它做运行时
  fallback；旧路径的最终退休另行跟踪。
- **broker/worker**:本地继续 loopback;远程启用 `bind_tls`(remote-actor P1)。

### 8.2 客户端发现

Lotus Next 的所有 surface（浏览器、嵌入式 WebView、桌面和移动端）使用同一个
canonical public `GET /api/v1/bootstrap` 作为 Bamboo 身份、REST/realtime
版本范围、编码能力与当前认证状态的唯一兼容性 authority。客户端不得通过
`/healthz`、`/api/v1/health`、`/v1` 路由存在性或试连 WebSocket 来猜测能力；
旧服务器的 404、非法响应、产品不匹配、版本范围无交集或必要 capability 缺失
都应显示为可诊断的不兼容状态，不得回退旧 Lotus 或另一套 endpoint。

响应 schema v1 固定为：

- `server.product = "bamboo"`，`server.version` 仅用于诊断，不用于推导能力；
- `api.name = "bamboo.agent"`，canonical base 仅为 `/api/v1`，范围 `1..=1`；
- realtime 为 `/v2/stream`、范围 `2..=2`，显式列出 JSON/MessagePack
  subprotocol；
- `capabilities` 是已实现能力的稳定、可扩展 ID 列表；
- `auth.policy` 与 `auth.request_state` 分离，后者由当前请求 cookie/header、
  locality 和同一个配置快照计算；
- 响应不含 verifier、token、device metadata、credential reference 或配置路径，
  并带 `Cache-Control: no-store` 与
  `Vary: Cookie, Authorization, X-Device-Id`。

Bootstrap 只证明 HTTP 发现契约；它不等价于当前 WebSocket 已收到 hello
acknowledgement。服务端通过 `auth.ws_hello_ack.v1` 广告 ACK 能力，客户端仍须在
每条新 socket 上发送 `hello` 并等待 `welcome`，之后才能把该连接视为
subscription-ready。Bamboo 为旧客户端保留兼容：已经由 loopback、cookie 或
header 预授权的客户端仍可 subscribe-before-hello 或完全不发送 hello；此时继续
立即服务 channel，但不会凭空发送 `welcome`。因此 `welcome` 先于订阅数据的顺序
保证针对 hello-first 客户端。

### 8.3 认证迁移

- 旧实例:`access_control.devices` 空 → 兼容 root 密码模式(`/v1` cookie 不变)。
- 升级:保留 root 密码,逐步为每台设备走 `/v2/pair` 签发 device token。
- 不做强制割接:`/v1` 在桌面 loopback 下可长期保留。

### 8.4 灰度开关

当前 `/v2/stream` 是 Bamboo 的固定能力，没有按 desktop/mobile 分叉的
`api_v2_ws` 配置。迁移灰度由制品发布与 Lotus Next capability admission 控制；
不得在同一个 Lotus Next 构建里根据 host kind 静默切回 `/v1`/SSE。

---

## 9. 分阶段落地

每阶段独立可交付、可测试、无行为回退:

### v2-P0 — 效率层(最低成本,先拿流量数字)

- 三条腿 WS accept/connect 启 `permessage-deflate`。
- 客户端面 token 合帧(`batch_ms` 可配,桌面 0)。
- **验收**:移动端一次对话 60KB → < 15KB;桌面 `batch_ms=0` 体感无差;`cargo test -p bamboo-server` 全绿。

### v2-P1 — TLS + 客户端面 WSS

- `ServerConfig.tls` + 手动证书;4 个入口统一进入 H1/Rustls builder;broker `accept_async` 套 `TlsAcceptor`。
- `GET /v2/stream` 单 WSS 入口;`feed` + `agent.{sid}` + `control` channel;cursor 续传。
- **验收**:公网 `wss://` 桌面+移动端跑通;断线重连 cursor 补漏无丢无重;`/v1` 桌面 loopback 不变。

### v2-P2 — per-device token + broker 远程化

- `AccessControlConfig.devices` + `/v2/pair` + `/v2/devices` 管理;握手 `hello` token。
- broker 执行 remote-actor P1:`bind_tls` + `Placement::Remote` + `ConnectLauncher` + Bearer。
- broker 中继事件流加合帧。
- **验收**:跨机 worker `wss://` + Bearer 跑通;设备吊销即时生效。

### v2-P3 — (可选)MessagePack

- `bamboo.v2.msgpack` subprotocol;移动端 + 远程 worker 启用。
- **验收**:同 schema 二进制编码,体积较 JSON+deflate 再 −30~40%。

---

## 10. 开放问题

| # | 问题 | 倾向 |
|---|---|---|
| 1 | device-token 配对:root 密码直配 vs 一次性配对码优先 | 桌面/首设备用 root 密码;后续设备用配对码(§4.3) |
| 2 | `agent.{sid}` 的 cursor 是否也进 journal 持久化 | 否——复用现有内存 `critical_events_to_replay`;长断线走 REST 重取会话详情 |
| 3 | WS 单连接的背压:一个慢 channel 是否阻塞其他 | 每 channel 独立 mpsc,慢消费者不阻塞快通道(参照 broker `next_pushed` 模式) |
| 4 | 证书热加载:是否进 v2-P1 | 否——现阶段重启生效;`SIGHUP` 热加载作为独立 enhancement |
| 5 | msgpack 是否进桌面 | 否——桌面留 JSON 便于调试;仅移动端/远程 worker |

---

## 11. 实现锚点速查

| 改动 | 文件:位置 |
|---|---|
| TLS 配置字段 | `crates/infra/bamboo-config/src/config.rs` `ServerConfig` + 新 `TlsConfig` |
| HTTP 启动 TLS | `server/web_service.rs:79,163`;`server/entrypoints.rs:148,293` |
| broker TLS | `crates/app/bamboo-broker/src/server.rs:57`(`accept_async` 套 `TlsAcceptor`) |
| worker TLS | `crates/infra/bamboo-subagent/src/transport.rs`(`WsServer::bind_tls`) |
| device-token 存储 | `bamboo-config/src/config.rs` `AccessControlConfig`(扩 `devices`)+ `config_manager.rs` |
| 认证中间件 | `handlers/settings/access_control.rs:251`(升级为 device-token 校验) |
| `/v2/stream` 入口 | `routes/agent.rs`(新 scope `/v2`)+ 新 `handlers/agent/ws_stream` |
| feed channel | `handlers/agent/stream/response.rs`(`plan_replay`/journal 逐帧复用) |
| agent channel | `handlers/agent/events/stream.rs`(`AgentEvent` 序列化复用) |
| token 合帧 | `handlers/agent/events/stream.rs` 帧出口 |
| Lotus Next 全 surface realtime consumer | `bigduu/lotus-next` 的 `src/services/chat/v2Stream.ts` + `accountFeed.ts` |
| canonical 客户端发现 contract | `handlers/agent/bootstrap.rs` + `routes/agent.rs` 的 `GET /api/v1/bootstrap`；Lotus Next typed consumer 由独立 Issue 接入 |

---

## 参考

- [《Remote Actor 方案设计》](./remote-actor-plan.md) —— broker/actor 远程化的 trait 接缝与分阶段(P0/P1/P2)
- [《Remote Mailbox Broker 设计》](./remote-mailbox-broker-design.md) —— broker 线协议(已 SHIPPED)
- [《Sub-Agent 运行时设计》](./subagent-actor-runtime-design.md) —— actor 模型基础
- `crates/app/bamboo-broker/src/server.rs` / `proto.rs` —— broker WS 线协议(客户端面 WSS 复用其握手/多路复用范式)
- `crates/infra/bamboo-subagent/src/transport.rs` —— parent↔child WS 传输
- `crates/core/bamboo-agent-core/src/agent/events.rs:97` —— `AgentEvent`(agent channel 载荷,逐字节复用)
- `crates/infra/bamboo-config/src/config.rs` —— `Config` / `ServerConfig` / `AccessControlConfig`
