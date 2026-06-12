# Actor Runtime 自评审(2026-06-12)

> 由 4 个 actor 进程(`bamboo -p`,真 LLM)对 feat/subagent-actor-runtime 分支完成:
> 3 个并行评审员(基础设施 / 引擎接入 / worker+CLI+设计文档)+ 1 个综合评审长。
> 本文档即这次改动"用自己评审自己"的产物;发现的 ProjectKey 碰撞、AdmittedSet 无界、CLI 双重打印已在同分支修复。

## 综合评审(评审长)

## 综合架构评审：Sub-agent → 独立 Actor 子进程重构

**总体评分：7.0 / 10** — 基础设施层设计扎实、隔离干净，但当前实现仅覆盖设计文档的"一次性 worker"切片，与完整虚拟 Actor 愿景存在显著落差。

**全体一致认可的优点（去重）：**
1. **安全与最小权限**：stdin 一次性投放凭据、`ScopedCredential`/`SecretsEnvelope` 不落盘不进 argv/env，凭证绝不按 project 复制
2. **崩溃恢复与孤儿防护**：Mailbox 的 `new/cur/corrupt` + recover 语义、lease renewal + `serve_one_with_accept_timeout` 兜底
3. **前向兼容**：ProvisionSpec serde 忽略未知字段 + 默认值，新旧二进制无需锁步升级
4. **零配置可用**：`build_local_actor_runner` 从 `current_exe()` 自动派生，用户一行 `runtime="actor"` 即启用

**Top4 风险（按严重程度）：**

| # | 严重度 | 风险 |
|---|--------|------|
| 1 | 🔴 高 | **设计与实现严重脱节**：worker 是 one-shot 进程，§2.4 持久 session / §3.2 双向中途问答 / §3.3 assign·ask·handoff / §5.3 父子索引均未落地，当前只完成实施切片第 3 步 |
| 2 | 🔴 高 | **凭证明文经 WebSocket/ProvisionSpec 传递**：`extract_provider_credentials` 将 api_key 明文塞入 spec，存在落盘和日志泄露面；凭证在 runner 创建时全量快照，运行期热更新无效 |
| 3 | 🟡 中 | **`AdmittedSet` 无界增长 + ack O(n) 扫描**：长生命周期 actor 的去重集合只增不减；`upsert_child` 跨两文件写入非原子，启动时缺乏自动 rebuild 触发 |
| 4 | 🟡 中 | **`ProjectKey` 碰撞 + 路由 O(n) 双查**：路径编码折叠导致 `/a/b` 与 `/a-b` 同 key；`CompositeExternalChildRunner` 线性匹配 + `should_handle` 重复调用 |

**下一步建议（按优先级）：**

1. **补齐 rehydrate 路径与启动时自动 rebuild**：在 worker 启动时无条件执行 `rebuild_index`，使 `session.json` 真正成为权威真身；这是弥合设计落差的前提。
2. **凭据传递改为环境变量/短期 token 注入**：消除 ProvisionSpec 中的明文 api_key，同步刷新 runner 级凭证快照机制，确保热更新生效。
3. **为 `AdmittedSet` 引入 TTL/LRU 淘汰，路由层改 HashMap**：两个改动互不依赖但都影响长运行稳定性，建议在下一个迭代一并处理。

## 分组评审原文

### infra
我将并行读取这六个文件以进行代码评审。我需要先获取工作区的绝对路径,然后重新读取这些文件。## 代码评审：bamboo-subagent

### 设计亮点

1. **权威/缓存分离 + 可重建索引**：`session.json` 为唯一权威源，`index.json`/`children.json` 均为可重建缓存；`rebuild_index` 通过 `MetaExtractor` trait 解耦负载结构，crash 后保证最终收敛（已有部分写测试覆盖）。Mailbox 的 `new/cur/corrupt` + `recover` 同样实现了 at-least-once 的崩溃恢复语义。

2. **ProvisionSpec 的安全与前向兼容设计**：stdin 一次性投放（规避 `ps` / env 泄露到孙进程）、`ScopedCredential` 最小授权、serde 默认忽略未知字段 + `#[serde(default)]` 确保新旧二进制无需锁步升级，且有专门的前后向兼容测试。

3. **传输层并发模型清晰**：`Cancel` 走 `CancellationToken` 带外触发不排队于事件；`serve_one_with_accept_timeout` 实现孤儿进程防御；unbounded channel + 独立 writer task 解耦读写。

### 潜在风险 / 改进点

1. **`upsert_child` 跨两文件写入非原子**（store.rs:260-281）：`children.json` 与 `index.json` 分两次原子写，中间崩溃会导致短暂不一致；虽然注释声称"crash converges on rebuild"，但 `rebuild_index` 只能由调用方主动触发，缺少自动触发机制（如启动时无条件重建）。建议至少在文档中约定启动检查点，或提供 `upsert_child` 的补偿写路径。

2. **`AdmittedSet` 无界增长**（mailbox.rs:221）：消费侧 dedup 集合随消息累积只增不减，长生命周期 actor 会无限膨胀。`ack` 采用 O(n) 全目录线性扫描（mailbox.rs:148-166）在高吞吐下也会成为瓶颈。建议加入 LRU/TTL 淘汰策略，`ack` 改为维护文件名 → 路径的缓存映射。

3. **`ProjectKey::from_workspace` 存在碰撞风险**（store.rs:39-46）：所有非字母数字字符折叠为 `-`，`/a/b` 与 `/a-b` 或 `/a//b` 会生成相同 key；同时缺乏对路径标准化（`canonicalize`），软链/相对路径会派生不同 key 指向同一 workspace。建议至少对路径 `canonicalize` 后再编码，并考虑记录原始路径以审计。
## 代码评审：bamboo-subagent

3. **`ProjectKey::from_workspace` 存在碰撞风险**（store.rs:39-46）：所有非字母数字字符折叠为 `-`，`/a/b` 与 `/a-b` 或 `/a//b` 会生成相同 key；同时缺乏对路径标准化（`canonicalize`），软链/相对路径会派生不同 key 指向同一 workspace。建议至少对路径 `canonicalize` 后再编码，并考虑记录原始路径以审计。


### engine
我需要先确认这些文件的绝对路径。现在让我读取这三个文件的实际内容。## 代码评审

**亮点**
1. **分层路由 + 最小权限凭证**:`resolve_runtime_metadata` 的三级优先级(typed override → legacy routing → global default)清晰且被测试覆盖;`build_spec` 只向子进程下发单一 provider 的凭证,泄露面控制到位。
2. **背压与生命周期管理**:Semaphore 限并发、`_slot` 贯穿整个 actor 生命周期、drive 结束后 `close()`+`kill()` 保证资源回收,cancel 分支单独发 `Cancel` 帧,边界处理完整。
3. **友好路径零配置可用**:`build_local_actor_runner` 自动从 `current_exe()` 派生 worker,普通用户一行 `runtime="actor"` 即可启用,expert 表是可选叠加而非前置依赖。

**风险 / 改进点**
1. **凭证安全弱**:`extract_provider_credentials` 把明文 `api_key` 塞进 `ProvisionSpec` 并经 WebSocket/进程传递;应改用 env 注入或短期 token,避免 spec 落盘/进日志。
2. **路由选择 O(n) 且双查 `should_handle`**:`CompositeExternalChildRunner` 顺序线性匹配,`execute_external_child` 内重复调用 `should_handle`,大 profile 集下应建 `agent_id → runner` HashMap 一次解析。
3. **并发与凭证在 runner 级共享,粒度偏粗**:`max_concurrent` 是单 runner 的本地信号量,多个 actor profile 之间无全局上限;凭证在 `new()` 时全量快照,运行期 provider 配置热更新后子进程仍持旧 key。
## 代码评审

**风险 / 改进点**
1. **凭证安全弱**:`extract_provider_credentials` 把明文 `api_key` 塞进 `ProvisionSpec` 并经 WebSocket/进程传递;应改用 env 注入或短期 token,避免 spec 落盘/进日志。
2. **路由选择 O(n) 且双查 `should_handle`**:`CompositeExternalChildRunner` 顺序线性匹配,`execute_external_child` 内重复调用 `should_handle`,大 profile 集下应建 `agent_id → runner` HashMap 一次解析。
3. **并发与凭证在 runner 级共享,粒度偏粗**:`max_concurrent` 是单 runner 的本地信号量,多个 actor profile 之间无全局上限;凭证在 `new()` 时全量快照,运行期 provider 配置热更新后子进程仍持旧 key。


### worker
我需要使用绝对路径。先确认工作区位置。# 代码评审:Subagent Worker + Actor CLI vs 设计文档

**对齐度:中高。** §6/§7 的物理激活与传输层、`ChildExecutor` seam、`AgentEvent` 零映射、stdin ProvisionSpec、fabric 自注册 + 续租、`Worker/ParentFrame/ChildFrame` 协议均与文档高度一致;但 §2.4/§3.4/§5 定义的"虚拟 actor"上层(mailbox、按需激活、持久索引)基本未实现,当前实现只覆盖了实施切片的第 3 步(一次性 worker 进程),尚未触及第 1/5 步。

**两个亮点**
1. **隔离干净**:凭据仅通过 `SecretsEnvelope` → 内存 Config,不落 argv/env/disk;存储/技能/metrics 全部隔离到 `storage_dir`,符合 §5.1"凭证绝不按 project 复制"的纪律。CLI 侧也做到了最小权限(只下发已解析 provider 的单个 credential,`actor_cli.rs:78-89`)。
2. **续租与孤儿防护到位**:lease renewal 任务在 serve 前启动、serve 结束后立即 abort + withdraw(`subagent_worker.rs:75-94`);并用 `serve_one_with_accept_timeout(120s)` 兜底,父进程崩了 worker 不会变孤儿泄漏。

**两个风险/偏差**
1. **rehydrate 路径与"真身=Session 文件"矛盾**:worker 每次 run 都 `Session::new` 一个全新内存 session,只在 `run.messages` 非空时整体替换(`subagent_worker.rs:225-248`)。这既不是文档 §2.4 的"load session.json→resume",也不是 §3.4 的 mailbox drain→admit,而是把真身从文件降级为父进程内存里的 `Vec<Message>` 透传;持久真身 + child→parent 索引(§5.3)完全缺席。
2. **worker 单次运行(one-shot)与"持久 actor"割裂**:实现是 `serve_one_with_accept_timeout` 跑完即退,既无 IDLE/deactivate,也无法接收 `ParentFrame::Message`(中途追问/handoff)。设计 §3.2 的"双向中途问答"、§3.3 的 `assign/ask/handoff` 在当前协议处理里没有落点——`actor_cli.rs` 只发 `Run` 和 `Cancel`,CLI 与 worker 都未处理 `message` 帧,与设计描述的多轮交互能力存在明显落差。
# 代码评审:Subagent Worker + Actor CLI vs 设计文档

**两个风险/偏差**
1. **rehydrate 路径与"真身=Session 文件"矛盾**:worker 每次 run 都 `Session::new` 一个全新内存 session,只在 `run.messages` 非空时整体替换(`subagent_worker.rs:225-248`)。这既不是文档 §2.4 的"load session.json→resume",也不是 §3.4 的 mailbox drain→admit,而是把真身从文件降级为父进程内存里的 `Vec<Message>` 透传;持久真身 + child→parent 索引(§5.3)完全缺席。
2. **worker 单次运行(one-shot)与"持久 actor"割裂**:实现是 `serve_one_with_accept_timeout` 跑完即退,既无 IDLE/deactivate,也无法接收 `ParentFrame::Message`(中途追问/handoff)。设计 §3.2 的"双向中途问答"、§3.3 的 `assign/ask/handoff` 在当前协议处理里没有落点——`actor_cli.rs` 只发 `Run` 和 `Cancel`,CLI 与 worker 都未处理 `message` 帧,与设计描述的多轮交互能力存在明显落差。

