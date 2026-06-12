# Sub-Agent 运行时设计:虚拟 Actor 模型

> 本文是 sub-agent 体系的**设计**(非实施步骤)。取代旧的 `subagent-subprocess-refactor-plan.md`——子进程只是本设计的一种 runtime 实现,不再是主题。

---

## 1. 目标与核心理念

我们真正要的不是「子进程」,而是:

> **异步 task 可以并行运行;agent 是运行 task 的容器;容器之间靠 chat(消息)通信,从而保持各自上下文独立。**

子进程、WebSocket、注册表、文件存储——全是为这个目标服务的手段。核心理念用一句话概括:

> **每个 agent 是一个「虚拟 actor」:逻辑上永远存在、可按 id 寻址;物理上按需激活。它的持久真身是一个 `Session` 文件,活进程只是这个真身的缓存。**

这样,「常驻」与「按文件恢复」不是二选一,而是同一真身的两个温度:进程热着=常驻,凉了就 deactivate 落盘、下次消息来再 load+resume(顺带白送崩溃恢复)。

---

## 2. 核心模型:虚拟 Actor

### 2.1 三个基本概念

| 概念 | 定义 | 现有对应 |
|---|---|---|
| **Task** | 异步工作单位,可并行,有输入(assignment)、跑出结果 | 一轮/一段 agent 执行 |
| **Agent(容器)** | 有隔离上下文、能跑 task 的 actor;按 id 寻址,有邮箱 | 一个 `Session` + 一次激活 |
| **通信** | actor 之间传递 `Message`;没有共享内存 | `Message/Part` + resume 注入 |

### 2.2 必须拆开的:三种「上下文」

「上下文」一词被重载,导致讨论拧巴。它其实是**三个正交、加入时机不同**的东西:

| | 含义 | 成员 | 何时加入 | 何时离开 |
|---|---|---|---|---|
| **发现上下文** (Tier 1) | 本机/项目内有谁、在哪 | 长驻 service agent | 进程 announce | deregister / 续租过期 |
| **归属上下文** (Tier 2) | 谁是我拥有的 child | 父 spawn 并认领的 child | 父 spawn+认领 | child 完成/回收 |
| **推理上下文** (LLM history) | 进入我思考的消息 | 自己 + 别人 admit 给我的 Message | **消息被 admit 那一刻** | 压缩/截断 |

> 「能被发现但不一定注册到上下文」= 发现 ∈,归属 ∉。
> 本文凡单说「上下文」默认指**第三种(推理上下文)**——其余两种叫「发现」「归属」。

### 2.3 投递面 vs 上下文面(核心纪律)

> **收到 ≠ 入上下文。**

- **投递面**:WS 上的 token / 工具事件(`AgentEvent`),高频,供 UI 与可观测,**永不自动进 LLM 上下文**。
- **上下文面**:`Message` 被 admit 进 history,低频、语义化(assignment / result / question / answer),**驱动 LLM**。

这条线现在就隐含存在(`SubAgentEvent` 流 vs `ChildCompletionCoordinator` 注入的 resume 消息)。本设计把它**提升为 core 的一等区分**,而非约定。

### 2.4 Actor 生命周期

```
        first message
  COLD ───────────────►  ACTIVATING ──► ACTIVE(跑 task,可与兄弟并行)
   ▲  (load session)                         │
   │                                         │ task 完成 / 空闲
   │      idle 超时 / 内存压力                ▼
   └──────────────────────────────────────  IDLE(热,等下条消息)
            deactivate(落盘,释放进程)
```

- **激活**:凉 actor 的 mailbox 收到消息 → **立刻激活**(eager,不惰性/批量)→ load `session.json` → drain mailbox → resume loop。
- **ACTIVE/IDLE**:热进程;按角色可配「空闲即落盘」或「保热常驻」。
- **deactivate**:空闲超阈值落盘释放进程;真身 + 未处理 mailbox 仍在文件。
- **mailbox 是一等持久件**(§3.4),与 eager 激活正交:激活策略决定「何时拉起进程」,mailbox 决定「消息如何排队/逐条处理」。两者组合,不冲突。v1 仅推迟「惰性/批量激活、优先级调度」。

---

## 3. 通信模型:何如 + 何时

### 3.1 agent 间通信 = 「被调用方是 agent 的 tool 调用」

不引入新范式:

- **A 找 B = A 发起一次调用**,载荷是 `Message`。
  - B 是 owned child → 解析成 spawn/激活。
  - B 是 service agent → 解析成 discovery 查找 + 连它。
  - **两条路只在「怎么找到 B」不同;通信协议与"调用→结果"语义完全一致。**
- **B 在自己私有上下文里跑**,产出结果 `Message`。
- **结果在调用 resolve 那一刻**,作为 tool-result **admit** 进 A 的上下文。

### 3.2 三种时序,同一套表达

| 模式 | 触发 | admit 时机 | 现成机制 |
|---|---|---|---|
| **阻塞调用** | A emit 调用并等 | B 结果返回 | `WaitingForChildren = All/Any/FirstError` + resume |
| **异步发了再收** | A spawn 后继续干别的 | A 之后主动 wait/collect | 现有 async child |
| **双向中途问答** | B 跑一半反问 A | 问题 admit→A;答 admit→B | `InputRequired` / pending 注入 |

**admit 的物理落点**:`mailbox.drain()`(§3.4)→ 经 `pending_injected_messages` 在下一轮 resume 注入。core 不新增管道,要明确的是 **policy**:什么消息、在什么安全点 drain。

### 3.3 父对 child 的控制面(补齐现状不足)

现状痛点:父持有的是「一次性内存句柄」,每轮重开父 agent 句柄就丢。本设计的控制面统一成**给 actor 的 mailbox 发 chat**(§3.4),且作用在**保留上下文的持久 actor** 上:

- `assign` 派 task · `ask` 问状态(不占 task)· `handoff` 追加新活 · `await(policy)` 等结果 · `cancel / retire` 生命周期。
- 父→子的关系做成**父 session 里的持久状态**(见 §5 索引),不依赖内存——这正是补上「持续关系」能力的关键。

### 3.4 Mailbox(一等持久收件箱)

actor 一次只处理一条消息、跑在私有上下文上;**当它正忙(跑着多轮 LLM loop)时,父发来的「问状态/追加任务/取消」必须有处可放**——这就是 mailbox 的结构性必要,而非优化。

- **介质**:每个 actor 一个 **Maildir 式文件目录**(`mailbox/new/` + `cur/`),与全局的文件式选型一脉相承。
- **并发**:**多写者(发送方)/ 单读者(actor 本人)**。发送方各自 `tmp + rename` 原子投递到 `new/`,互不竞争、无锁;actor 把 `new/→cur/` 取走,admit、处理、ack 后删除。
- **持久 + 崩溃安全**:消息落盘才算投递;进程崩了消息仍在,重新激活继续 drain。**at-least-once + 按 `msgid` 幂等 admit**。
- **凉/热统一**:投递永远 = 写 `new/`;凉 actor → 触发激活,热 actor → WS 发 wakeup 催其尽快 drain。**mailbox 是收件真相,WS 退化为「叫醒 + 传输」。**
- **每个 actor 都有(含父)**:child 的结果/反问写进**父的 mailbox**,父 drain→admit→resume——把原 `ChildCompletionCoordinator→resume` 路径也统一进同一套。

**两条通道分开:**
| 通道 | 内容 | 处理 |
|---|---|---|
| **in-band**(mailbox) | task / 询问 / 新指令 | 在 loop **安全点 drain → admit** |
| **out-of-band**(控制) | `cancel` 等 | 走 WS 控制帧,**不入 mailbox**,即时作用于运行中的 loop |

**安全点 = 每轮 LLM round 开头 drain**:既能中途被父转向/追问(下一轮上下文即含新消息),又永不在 LLM 调用中途改上下文。`cancel` 仍即时 out-of-band。

> 这把「父能持续操作运行中/凉着的 sub」从靠不住的内存句柄,变成 mailbox 上的结构性能力——正是 §3.3 痛点的根治。

---

## 4. 服务发现:两层模型

| | Tier 1 发现层 | Tier 2 归属层 |
|---|---|---|
| 装谁 | 长驻、无主的 service agent | 父 spawn 并认领的 child |
| 介质 | **文件式共享注册目录**(进程无关) | 父 server 内嵌的内存 registry + axum routes |
| 边界 | **project**(同项目内可见;跨项目放全局一小层) | 父 session |
| 进归属上下文? | 否(session 用它即开临时「使用」连接,不承担其生命周期) | 是 |
| 生命周期 | 自管:announce → 续租 → 退出自删 | 父管:register → heartbeat → deregister |

> **project = 天然的发现/隔离边界**,比「本机所有进程互发现」干净。两层**共用同一套 WS 执行协议**,区别只在「怎么找到」和「谁拥有」。

---

## 5. 存储与索引

### 5.1 三类数据,去三个地方(别混)

| 数据 | 性质 | 去处 |
|---|---|---|
| **用户全局**:config、provider 凭证、project 清单 | 跨 project、敏感 | `~/.bamboo`(不变) |
| **project 持久**:`Session`(actor 真身)、actor metadata、task 状态 | 属于某 project、要持久/可恢复 | **全局按 project key**(下) |
| **机器临时**:激活表、discovery 文件、pid、endpoint | 瞬态、绑进程 | runtime 目录,project-keyed,**不进 project 树** |

> 凭证绝不按 project 复制(泄露面 ×N);激活态绝不进持久索引(瞬态)。

### 5.2 目录布局(全局按 project key,sub-session 按父分组)

```
~/.bamboo/
└─ projects/<project-key>/                  project-key = hash(workspace),仿 ~/.claude/projects/<hash>
   ├─ project.json                          workspace 路径、创建时间
   ├─ index.json                            【项目索引】root 列表 + child→parent 全局查找表
   └─ sessions/
      └─ <parent-id>/                       按父 session 分组
         ├─ session.json                    父 session(权威)
         ├─ children.json                   【父级索引】该父所有 child 的去规范化清单
         ├─ mailbox/{new,cur}/              父收件箱(child 的结果/反问投这里)
         └─ children/
            └─ <child-id>/
               ├─ session.json              child session(权威、隔离、不 merge)
               ├─ mailbox/{new,cur}/        child 收件箱(§3.4,Maildir 式,多写者安全)
               └─ (artifacts/ logs/ 以后)
```

child 的 **session 文件**与**聚合 metadata** 都归到父目录下;每个 child 仍是独立 `session.json`(隔离不变,仅物理归属父)。**每个 actor 自带 `mailbox/`** 作为持久收件真相(§3.4)。

### 5.3 三个索引

| 索引 | 位置 | 回答 | 命中成本 |
|---|---|---|---|
| **项目索引** `index.json` | project 根 | 「有哪些 root」+「child_id 属于哪个 parent」 | 1 读,O(1) 解析 child |
| **父级索引** `children.json` | 每个 `<parent-id>/` | 「我有哪些 child、各自状态/类型/标题」 | 1 读,不扫 child 文件 |
| **激活表**(临时) | runtime 目录 | 「哪些 child 现在热、endpoint/pid」 | 瞬态,不持久 |

> `index.json` 的 **child→parent 查找表**让任何组件**只凭 child_id 就 O(1) 定位**,不必知道父、不必扫目录——这是「做好索引」的核心。

### 5.4 一致性:权威文件 + 单写者 + 可重建

1. **权威 = `session.json` 们**;索引是去规范化缓存,坏了/缺了**扫目录重建**(fsck 式)→ 软缓存,崩溃可恢复。
2. **单写者**,杜绝跨进程写竞争:
   - `children/<child>/session.json` → **只有该 child 进程写**。
   - `children.json` + `index.json` → **只有父 server 的 registry 写**;child 经 Tier 2 register/heartbeat/deregister **上报状态**,registry 据此更新索引。child 永不碰父级索引。
3. **原子写**:每个索引 temp + rename。

→ registry 的职责坐实为**持久索引维护者**;激活表(谁热)留 runtime 目录、与 `ProcessRegistry` 对账,不混进持久索引。

### 5.5 重启恢复

重启时:持久索引说「这 project 下有这些父、各父这些 child、上次状态」→ 激活表空 → 谁来消息谁**按需激活**(load `children/<id>/session.json` → resume)。父对 child 的持久句柄 = `index.json` 的 child→parent 映射 + `children.json` 的状态,**不依赖内存**。

---

## 6. 运行时:子进程层(actor 的物理激活)

- **激活 = 一个子进程**(每个 active actor 一个进程 → 真并行、崩溃隔离、资源/沙箱可独立限额)。凉 actor 不占进程。
- **WS 全双工**承载:事件下行(`AgentEvent` 帧)+ 控制上行(Run/Cancel/Message)。
- **`AgentEvent` 自带 serde**(`agent/events.rs:95`),**原样当 WS 帧传,零映射**。
- **生命周期**走 `ProcessRegistry`(注册/优雅+强杀/存活/孤儿回收)。

### Worker 两种模式

| 模式 | 入口 | 发现 | 生命周期 | 进归属? |
|---|---|---|---|---|
| 被拥有的 child | `bamboo subagent-worker`(父 spawn,stdin 喂 provider 配置) | Tier 2 registry | 父管,空闲落盘/可保热 | 是 |
| 长驻 service agent | `bamboo agent --role <x> --labels ...`(独立起) | Tier 1 文件 fabric | 自管,续租,退出自删 | 否 |

### WS 协议(草案)

```jsonc
// 父→子
{ "kind": "run",     "assignment": "<task prompt>", "reasoning_effort": "..." }
{ "kind": "cancel" }
{ "kind": "message", "text": "..." }          // 常驻多轮 / ask / handoff

// 子→父
{ "kind": "event",   "event": { "type": "token", "content": "..." } }   // AgentEvent 原样
{ "kind": "terminal","status": "completed|error|cancelled", "result": "...", "error": null }
```

`event.event` 即 `AgentEvent` serde 输出;父侧反序列化后**直接喂 `event_tx`**,无转换。

---

## 7. 代码结构:`bamboo-subagent` crate

把「注册 → 发现 → 心跳 → 存活/回收 → 激活 → 父子传输」当作**一个有边界的工程子系统**抽出。铁律:**它必须停在 engine 之下,绝不反依赖 engine/server**(否则成环)。

```
crates/infra/bamboo-subagent/
├─ proto         注册/心跳/WS 帧类型(两层共用)
├─ transport     WS client + WS server scaffolding(两层共用)
├─ discovery/    Tier 1 文件 fabric:record / publish(原子写+续租)/ discover(过滤+丢 stale)/ gc
├─ registry/     Tier 2:内存表 + liveness 扫描 + axum routes + 【持久索引维护】
├─ store/        project-keyed 目录布局 + 三索引读写(原子、可重建)
├─ mailbox/      Maildir 式收件箱:deliver(原子写 new/)/ drain(new/→cur/→ack)/ 幂等
└─ lifecycle     spawn worker + ProcessRegistry 集成 + kill 兜底
```

### 两个 trait seam(让 crate 不碰 runtime)

```rust
// ① child 侧:worker 用真实 runtime 实现;crate 的 WS server 只管调它跑 agent loop
trait ChildExecutor {
    async fn run(&self, assignment: Assignment, events: EventSink,
                 cancel: CancellationToken) -> ChildOutcome;
}

// ② 父侧:crate 暴露 fleet API;engine 的 SubprocessChildRunner 薄适配
struct SubagentFleet { /* registry + store + lifecycle */ }
impl SubagentFleet {
    async fn spawn(&self, spec: ChildSpec) -> ChildHandle;   // spawn→discover→连 WS
    // ChildHandle: Stream<AgentEvent> + terminal + cancel()
}
```

### 依赖图(无环)

```
core(agent-core) ─┐
                  ├─► bamboo-subagent ─► engine(薄适配 SubprocessChildRunner)─► server(构造+挂载+worker)
infra(infra) ─────┘
```

- crate 仅依赖 `bamboo-agent-core`(拿 `AgentEvent`)+ `bamboo-infrastructure`(拿 `ProcessRegistry`),都在 engine 之下。
- `ExternalChildRunner` trait 留在 engine,其 impl(`SubprocessChildRunner`)也留 engine,只**薄适配**到 `SubagentFleet` → 不成环。

### 现有 crate 里只剩薄胶水

| 位置 | 残留 |
|---|---|
| `bamboo-engine/external_agents/subprocess_adapter.rs` | `SubprocessChildRunner` 把 `ExternalChildRunner` 映到 `SubagentFleet` + 转 `event_tx` |
| `bamboo-server` | 构造 `Arc<SubagentRegistry>` 进 AppState、`.merge(subagent::routes())`、`subagent-worker`/`agent` 子命令提供 `ChildExecutor` |

---

## 8. core/domain 层的改动(尽量小)

- **复用**:`Session`(actor 真身)、`Message/Part`(通信单位)、`pending_injected_messages` + resume(admit 机械)、`WaitingForChildren`(时序 policy)。
- **新增的核心概念只有两个**:
  1. **投递面 vs 上下文面的一等区分**——把「事件流(不入 context)」与「admit 一个 Message(入 context)」在 core 显式分开。
  2. **会话的「根」从父子树松绑为「调用来源」**——让 owned child(根=父)与 service-agent 线程(根=某次调用)成为同一 `Session` 抽象的两种实例;`root_session_id / spawn_depth` 相应泛化。
- **不渗进 core 的**:发现(文件 fabric)、激活/寻址、WS 投递——全留 infra。core 只认 `Session + Message + admit`。

### Service agent 的上下文线程语义
默认 **(a) 无状态 RPC**:每次调用开全新临时上下文,用完即弃(最贴「不 merge」、不串台、不膨胀);可选 **(b) 按 caller 分线程**(caller 显式带 `thread_id` 才进入有状态会话)。本质仍是 `Session`,只是根=某次调用而非某个父。

---

## 9. 与现有系统的映射(复用清单)

| 设计概念 | 现成物 | 位置 |
|---|---|---|
| 可插拔「换地方执行」seam | `ExternalChildRunner` + `wants_external` | `engine/.../sdk/spawn.rs:257`、`runtime/execution/spawn.rs:39` |
| 进程生命周期 | `ProcessRegistry` | `infra/bamboo-infrastructure/process/registry.rs` |
| 子进程 + 管道范式 | MCP `StdioTransport` / bash 工具 | `infra/bamboo-mcp/transports/stdio.rs` |
| 父子等待/恢复协调 | `WaitingForChildrenState` + `ChildCompletionCoordinator` | `engine/.../session_app/` |
| admit 机械 | `pending_injected_messages` + resume | `core/.../session/runtime_metadata.rs` |
| 事件可序列化 | `AgentEvent` serde | `core/.../agent/events.rs:95` |
| project-keyed 存储范式 | `~/.claude/projects/<hash>/` 同构 | — |

---

## 10. 实施切片(设计落地的建议顺序,细节另议)

> **状态标注(2026-06-12,分支 `feat/subagent-actor-runtime`,16 checkpoints)**
> ✅ 已落地并验证 · 🟡 部分落地 · ⏸ 刻意推迟(等消费者)

1. ✅ **存储+索引+mailbox**:`store/` + `mailbox/` 模块(project-keyed 布局 + 三索引 + Maildir 收件箱,原子/可重建/幂等),纯逻辑可单测。
   *已落地:全套 + ProjectKey 防碰撞(canonicalize+hash)+ AdmittedSet 有界化 + ack O(1)。注:store/mailbox 是已测零件,server 侧消费(持久索引/凉收件)待接。*
2. ✅ **crate 骨架 + 两 seam**:`proto` / `transport` / `registry`(含索引维护)/ `discovery`,塞假 `ChildExecutor` 即可在**不起真实 runtime** 下单测。
   *已落地:含 `provision`(stdin 装备契约)与 `fleet`(spawn+发现);假执行器=EchoExecutor。*
3. ✅ **child worker(owned)+ WS**:`bamboo subagent-worker`,隔离存储 + 最小 runtime + 自身 WS server。
   *已落地:真 `agent.execute()`(canonical loop,压缩/工具全复用,不 fork loop)+ 凭证经 stdin 内存态(含 provider_instances/加密 key)。*
4. ✅ **父侧 runner 接入** `wants_external`:`ActorChildRunner`,按配置灰度,零代码回滚。
   *已落地:一行 `"subagents": {"runtime": "actor"}` 开关 + per-role overrides + `max_concurrent` 背压;真 LLM 与生产路径 e2e 双验证。*
5. 🟡 **按需激活 + 落盘恢复**:
   *已落地:重激活带全量上下文(RunSpec.messages rehydration)+ 结果回写 transcript——父存储即真身,进程即缓存。**运行中带内转向**(send_message → live WS → loop round 边界 admit)同样落地。*
   *待做:凉 actor 的 mailbox 文件收件(离线投递场景,等常驻 owned actor 需求)。*
6. ✅ **service agent 模式**:`bamboo actor serve/list/call`,Tier 1 文件 fabric,无状态 RPC(每次调用独立 session)。
7. 🟡 **健壮性/限额**:
   *已落地:孤儿回收(accept 超时自杀 + kill_on_drop + 续租/注销)、并发上限、存储 GC(7 天)、CLI 可观测(`bamboo -p` / `actor run` 流式)。*
   *待做:`Limits`(run/idle 超时、max_rounds)已随 spec 下发但 worker 未强制;注册表级心跳 reap(等 Tier-2 路由消费者)。*

### 刻意推迟项(有消费者才做,避免过度建设)

| 项 | 等待的消费者 / 触发条件 |
|---|---|
| Tier-2 注册表 HTTP 路由(`/internal/subagents`) | lotus 前端要 actor 健康视图时 |
| mailbox 接 agent loop(完整 drain→admit) | 常驻 owned actor 的离线收件需求(steering 已用引擎原生 pending 队列等效达成) |
| 凭证短期 token / 父代理模式 | 安全迭代;`SecretsEnvelope` 已留演进位 |
| `Limits` 强制执行 | 资源限额需求明确时(父侧 watchdog 已兜总超时) |

> ✅ **`bamboo -p` = 完整 headless server(已落地,真 LLM 验证)**:full AppState + root 工具面(含 SubAgent,可 spawn child 并走完整 wait/resume 协调)、树静默才退出、`-s <session>` 续跑同一会话。`--echo` 保留为裸 actor 链冒烟;单 actor 快捷路径在 `bamboo actor run`。

---

## 11. 一句话总览

**agent = 容器(其真身是一个 project-keyed、按父分组、带索引的 `Session` 文件 + 一个 Maildir 式持久 `mailbox/`);task 并行分发;actor 之间靠 chat 投递到对方 mailbox 来通信、以隔离上下文;mailbox 在每轮 round 开头 drain→admit,`cancel` 走 out-of-band;凉了落盘、mailbox 来消息即 eager 激活(白送崩溃恢复);owned child 走父内嵌 registry(Tier 2),长驻 service agent 走文件 fabric(Tier 1),project 为边界;子进程 + WS 是物理激活与传输,`AgentEvent` 零映射上线;整套「发现/注册/心跳/激活/传输/索引」收敛进 infra 的 `bamboo-subagent` crate,用两个 trait seam 与 runtime 解耦、不成环;core 只认 `Session + Message + admit`。**
