# `bamboo-subagent` 切片 1 接口规格:`store/` + `mailbox/`

> 配套 [`subagent-actor-runtime-design.md`](./subagent-actor-runtime-design.md) §5/§3.4。
> 范围:纯文件逻辑,**不依赖任何 runtime**(可用 tempdir 全量单测)。依赖仅 `bamboo-agent-core`(拿 `Session`/`Message`)+ serde/chrono/uuid/tokio::fs。

---

## 0. 公共:原子写 + 错误模型

```rust
/// 所有持久文件统一走 tmp + rename,保证读者永不见半成品。
/// 写 <dir>/.<name>.tmp.<uuid> → fsync → rename 到 <dir>/<name>。
pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), IoCtx>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io at {path}: {source}")] Io { path: PathBuf, source: std::io::Error },
    #[error("decode {path}: {source}")] Decode { path: PathBuf, source: serde_json::Error },
    #[error("corrupt index {path}, rebuild required")] CorruptIndex { path: PathBuf },
    #[error("not found: {0}")] NotFound(String),
}
pub type Result<T> = std::result::Result<T, StoreError>;
```

规则:**权威 = `session.json` 们;索引/mailbox 文件坏了不致命**——索引可 rebuild,mailbox 坏件进 `corrupt/` 隔离 + 日志,drain 继续。

---

## 1. `store/` —— project-keyed 布局 + 三索引

### 1.1 寻址类型

```rust
/// project-key = 对 workspace 绝对路径的稳定编码(仿 ~/.claude/projects/<hash>)。
pub struct ProjectKey(String);
impl ProjectKey {
    pub fn from_workspace(workspace: &Path) -> Self;   // 规范化 + 编码,确定性
    pub fn as_str(&self) -> &str;
}

/// 一个 session 的逻辑位置(root 或 某父下的 child)。
pub enum SessionLoc {
    Root  { key: ProjectKey, session_id: String },
    Child { key: ProjectKey, parent_id: String, child_id: String },
}
```

### 1.2 磁盘布局(权威)

```
<root>/projects/<key>/
  project.json                         { workspace, created_at }
  index.json                           ProjectIndex(roots + child_lookup)
  sessions/<parent-id>/
    session.json                       Session(权威)
    children.json                      ChildrenIndex(去规范化)
    mailbox/{tmp,new,cur,corrupt}/     §2
    children/<child-id>/
      session.json                     Session(权威、隔离)
      mailbox/{tmp,new,cur,corrupt}/   §2
```

> `<root>` 默认 `~/.bamboo`(由调用方注入,便于测试用 tempdir)。

### 1.3 索引数据结构

```rust
/// project 根:index.json —— 单一全局解析器。
pub struct ProjectIndex {
    pub version: u32,                          // 模式版本,便于演进
    pub roots: Vec<RootEntry>,
    pub child_lookup: BTreeMap<String, String>,// child_id -> parent_id,O(1) 定位任意 child
}
pub struct RootEntry { pub session_id: String, pub title: String, pub updated_at: DateTime<Utc> }

/// 每父:children.json —— 「列我的孩子+状态」一次读取,不扫 child 文件。
pub struct ChildrenIndex { pub version: u32, pub children: Vec<ChildEntry> }
pub struct ChildEntry {
    pub child_id: String,
    pub subagent_type: String,
    pub status: ChildStatus,
    pub title: String,
    pub responsibility: String,
    pub updated_at: DateTime<Utc>,
}
pub enum ChildStatus { Pending, Running, Idle, Completed, Error, Cancelled }
```

### 1.4 Store API

```rust
pub struct SubagentStore { root: PathBuf }

impl SubagentStore {
    pub fn open(root: PathBuf) -> Self;

    // ---- session 真身(原子读写)----
    pub async fn load_session(&self, loc: &SessionLoc) -> Result<Session>;
    pub async fn save_session(&self, loc: &SessionLoc, s: &Session) -> Result<()>;
    pub async fn session_exists(&self, loc: &SessionLoc) -> bool;

    // ---- 索引读 ----
    pub async fn list_roots(&self, key: &ProjectKey) -> Result<Vec<RootEntry>>;
    pub async fn list_children(&self, key: &ProjectKey, parent_id: &str) -> Result<Vec<ChildEntry>>;
    /// O(1):查 index.json 的 child_lookup,返回 child 的完整位置。
    pub async fn resolve_child(&self, key: &ProjectKey, child_id: &str) -> Result<Option<SessionLoc>>;

    // ---- 索引写(单写者 = registry;见不变量)----
    pub async fn upsert_root(&self, key: &ProjectKey, e: RootEntry) -> Result<()>;
    pub async fn upsert_child(&self, key: &ProjectKey, parent_id: &str, e: ChildEntry) -> Result<()>;
    pub async fn remove_child(&self, key: &ProjectKey, parent_id: &str, child_id: &str) -> Result<()>;

    // ---- 自愈 ----
    /// 扫 sessions/** 重建 index.json + 各 children.json;索引缺失/损坏时调用。
    pub async fn rebuild_index(&self, key: &ProjectKey) -> Result<()>;

    // ---- mailbox 句柄 ----
    pub fn mailbox(&self, loc: &SessionLoc) -> Mailbox;   // 解析到对应 mailbox/ 目录
}
```

### 1.5 不变量(实现与测试都据此)

1. **权威/缓存分离**:`session.json` 为真相;`index.json`/`children.json` 为去规范化缓存,**任何时候可由 `rebuild_index` 完全重建**。
2. **单写者**:
   - 每个 `session.json` 仅其拥有者进程写。
   - `index.json` + 所有 `children.json` 仅 **registry(父 server)** 写 —— `upsert_*/remove_*` 假定被串行调用(registry 内部持锁),Store 自身不跨进程加锁。
3. **原子**:所有写 = tmp+rename;`upsert_child` = 读-改-写整份 children.json(去规范化清单小,可整写)+ 同步更新 index.json 的 child_lookup。
4. **child_lookup 与 children.json 双更新需同序**:先写 child 的 `children.json`,再写 `index.json`;崩在中间 → 下次 `rebuild_index` 收敛(幂等)。

---

## 2. `mailbox/` —— Maildir 式持久收件箱

### 2.1 消息类型

```rust
pub struct MsgId(String);                         // uuid v7(时间有序)
pub struct AgentRef { pub session_id: String, pub role: Option<String> }

pub struct InboxMessage {
    pub id: MsgId,                                 // 幂等键
    pub from: AgentRef,
    pub kind: InboxKind,
    pub body: Message,                             // domain Message(chat 内容)
    pub created_at: DateTime<Utc>,
}
pub enum InboxKind { Task, Ask, Handoff, Reply }   // 对应 §3.3 控制面动词;cancel 不走这里(out-of-band)

/// drain 返回:消息 + 它在 cur/ 的落点(供 ack)。
pub struct Delivered { pub msg: InboxMessage, pub cur_path: PathBuf }
```

### 2.2 目录与文件名

```
mailbox/
  tmp/      写入中(rename 前的暂存)
  new/      已投递、未处理     文件名: <unix_nanos>-<msgid>.json  (前缀保证 drain 有序)
  cur/      已取走、处理中
  corrupt/  解析失败隔离
```

### 2.3 Mailbox API

```rust
pub struct Mailbox { dir: PathBuf }

impl Mailbox {
    pub fn at(dir: PathBuf) -> Self;
    pub async fn ensure_dirs(&self) -> Result<()>;                 // 建 tmp/new/cur/corrupt

    // ---- 发送方:多写者、无锁(各自 tmp→rename 到 new/)----
    pub async fn deliver(&self, msg: &InboxMessage) -> Result<MsgId>;

    // ---- 接收方:单读者 = actor 本人 ----
    /// 取走 new/ 全部:逐个 rename new/X → cur/X(claim),解析返回(按文件名/时间有序)。
    /// 坏件 → 移入 corrupt/ 并跳过,不中断。
    pub async fn drain(&self) -> Result<Vec<Delivered>>;
    /// admit+持久成功后调用:删除 cur/X。
    pub async fn ack(&self, id: &MsgId) -> Result<()>;
    /// 激活时调用:上次崩溃残留在 cur/ 的消息当「重投递」再次返回(配合幂等 admit)。
    pub async fn recover(&self) -> Result<Vec<Delivered>>;

    pub async fn is_empty(&self) -> Result<bool>;                  // new/ 是否有件(WS wakeup 后快速判定)
}
```

### 2.4 投递 / drain→admit / 幂等(语义)

- **deliver(多写者)**:序列化 → 写 `tmp/<uuid>` → fsync → rename 到 `new/<nanos>-<msgid>.json`。rename 原子,跨进程并发无需锁、无丢失。
- **drain(单读者)**:`new/` 按文件名升序(=时间序)逐个 `rename → cur/`,解析。**claim 与处理解耦**:即便处理中崩溃,消息留在 `cur/`,下次 `recover` 重出。
- **admit 落点**(agent loop 每轮 round 开头):
  ```
  let batch = mailbox.drain().await?;                 // + recover() 于激活首轮
  for d in batch {
      if admitted_set.contains(&d.msg.id) { mailbox.ack(&d.msg.id).await?; continue; } // 幂等去重
      admit_to_context(d.msg.body);                   // → pending_injected_messages → resume
      admitted_set.insert(d.msg.id.clone());          // 落在 session.runtime_metadata,随 session 持久
  }
  persist_session().await?;                            // 先持久 admitted_set + 上下文
  for d in &batch { mailbox.ack(&d.msg.id).await?; }   // 再 ack 删除
  ```
  **at-least-once**:崩在 persist 与 ack 之间 → 重投递 → `admitted_set` 去重。`cancel` 永不进 mailbox,走 WS 控制帧即时打断。

### 2.5 不变量

1. **多写者安全无锁**:仅靠 tmp+rename 原子性;`new/` 文件名 `<nanos>-<msgid>` 唯一且有序。
2. **claim 与 ack 分离**:`drain` 把消息从 `new/` 移到 `cur/`(领取);`ack` 才删除。崩溃→`cur/` 残留→`recover` 重出。
3. **幂等由消费者保证**:`admitted_set`(session 状态)按 `msgid` 去重;mailbox 本身只保证「至少一次」。
4. **坏件不致命**:解析失败移 `corrupt/`,drain 继续。

---

## 3. 单元测试清单(全程 tempdir,无 runtime)

**store/**
- [ ] `save_session`/`load_session` round-trip;`save` 不留半成品(注入 rename 前 panic,文件仍为旧版或不存在,绝不半截)。
- [ ] `upsert_child` 后 `list_children` 命中;`resolve_child` 经 `index.json` O(1) 返回正确 `SessionLoc`。
- [ ] `remove_child` 同步清掉 children.json 与 child_lookup。
- [ ] 删除 `index.json` + 某 `children.json` 后 `rebuild_index`,结果与重建前逐字段相等(幂等)。
- [ ] 双更新中途中断(写完 children.json、未写 index.json)→ `rebuild_index` 收敛。

**mailbox/**
- [ ] `deliver` 后 `drain` 返回该消息;`new/` 空、`cur/` 持有;`ack` 后 `cur/` 空。
- [ ] **多写者**:并发 spawn N 个 `deliver` → `drain` 一次性取全 N 条、无丢失、id 唯一。
- [ ] **有序**:`drain` 按 `created_at` 升序返回。
- [ ] **崩溃恢复**:`drain` 后不 `ack` 直接重建 Mailbox → `recover` 返回 `cur/` 残留。
- [ ] **幂等**:同 `msgid` 重投递,消费者 `admitted_set` 去重(测 helper)。
- [ ] **坏件**:`new/` 放一个非法 JSON → `drain` 跳过它并移入 `corrupt/`,其余正常返回。
- [ ] `is_empty` 在有/无 `new/` 件时正确。

---

## 4. 交付边界

本切片只交付 `store/` + `mailbox/` 两个模块及其单测;**不含** WS/transport、registry 的 axum routes、worker、激活逻辑(那些在切片 2+,依赖本切片的类型)。`ChildStatus`/`SessionLoc`/`InboxMessage` 等类型在此定义,后续切片复用。
