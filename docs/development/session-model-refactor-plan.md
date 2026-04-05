# Session-Level Model Selection Refactor Plan

## 1. 背景与目标

本次重构目标是把 **主模型（model）** 和 **reasoning effort** 从“全局统一配置”迁移为 **session 级配置**，同时保持：

- 全局 provider 配置里的 `model` / `reasoning_effort` 仅作为 **新 session 初始默认值**
- `fast_model` 和 `vision_model` 继续保持 **全局 provider 级配置**
- 用户可以在不同 session 下独立修改 `model` 和 `reasoning effort`
- 修改 session 的 model/reasoning 行为，本质上是 **修改该 session.json**
- `config.json` 当前过重，尤其 `model_limits` 建议拆分为独立文件

工作区：
- 后端：`bamboo`
- 前端：`lotus`

---

## 2. 当前实现现状（基于代码勘察）

## 2.1 后端 session 已经有天然承载位：`Session.model`

后端 `Session` 结构已经直接包含 `model` 字段：

- `bamboo/src/agent/core/agent/types.rs:614`
- `bamboo/src/agent/core/agent/types.rs:652`

```rust
pub struct Session {
    ...
    pub model: String,
    pub metadata: std::collections::HashMap<String, String>,
    ...
}
```

这意味着：

- **session-level model 已经有后端持久化字段**
- 当前 session 文件 `session.json` 已经能保存 model
- 不需要为 model 再额外发明复杂 metadata key

---

## 2.2 session 文件真实持久化位置已经稳定

后端存储层：

- `bamboo/src/agent/core/storage/v2.rs:3`
- `bamboo/src/agent/core/storage/v2.rs:5`
- `bamboo/src/agent/core/storage/v2.rs:6`

存储结构：

- `sessions/<root_id>/session.json`
- `sessions/<root_id>/children/<child_id>/session.json`

因此“用户在某个 session 下改 model/reasoning，就是改 session file”的产品目标，与现有后端架构是完全匹配的。

---

## 2.3 当前 chat 请求已经会把请求 model 写回 session.model

后端 chat handler：

- `bamboo/src/server/handlers/agent/chat/handler/mod.rs:42`
- `bamboo/src/server/handlers/agent/chat/handler/mod.rs:111`

关键逻辑：

```rust
let model = validate_and_normalize_model(req.model.as_str())?;
...
session.model = model;
```

这说明当前行为是：

- 前端每次 `chat` 都必须带 `model`
- 后端会在接收消息时把 model 覆盖写入 session

所以实际上系统已经“半实现”了 session-level model，**但当前请求入口与 UI 心智仍未完全围绕 session 配置收敛**。这里需要明确目标：

- `InputContainer` 中切 model 的行为，定义上就是**修改当前 session model**
- 后端执行链路应以 `session.model` / `session.reasoning_effort` 为主
- request 中携带的 model/reasoning 只保留为兼容迁移输入，不再作为长期主驱动

---

## 2.4 execute 当前仍然是 request-driven，但目标必须改为 session-driven

后端 execute handler：

- `bamboo/src/server/handlers/agent/execute/handler/mod.rs:167`
- `bamboo/src/server/handlers/agent/execute/handler/mod.rs:173`
- `bamboo/src/server/handlers/agent/execute/handler/mod.rs:294`

当前实现里的关键逻辑：

```rust
let model = validate_and_normalize_model(&req.model)?;
...
spawn_agent_execution(... model, ...)
```

同时 reasoning 的当前优先级是：

- 请求 `req.reasoning_effort`
- 否则 provider config 默认值 `config_snapshot.get_reasoning_effort()`

并且 execute 会把 effective reasoning 写到 session metadata：

- `bamboo/src/server/handlers/agent/execute/handler/mod.rs:240`
- `bamboo/src/server/handlers/agent/execute/handler/mod.rs:242`

```rust
session.metadata.insert("reasoning_effort", ...)
```

因此当前 execute 的真实问题是：

- **当前实现仍以 request 为主，不符合 session-driven 目标**
- **model 没有以 session 作为主来源**
- **reasoning 没有以 session config 作为主来源**
- session metadata 中的 reasoning 更像“上一次执行结果记录”，不是清晰的 session 配置源

本次重构里，这一条需要被明确收敛为：

- `execute` 的 model 来源应首先看 `session.model`
- `execute` 的 reasoning 来源应首先看 `session.reasoning_effort`
- request 中的 model/reasoning 只保留为兼容过渡输入，不能继续定义 execute 的长期主行为

---

## 2.5 respond auto-resume 当前也依赖 request model，但目标应与 session-driven execute 对齐

后端 `respond submit`：

- `bamboo/src/server/handlers/agent/respond/handlers/submit.rs:27`
- `bamboo/src/server/handlers/agent/respond/handlers/submit.rs:111`
- `bamboo/src/server/handlers/agent/respond/handlers/submit.rs:127`

当前行为：

- `respond` 请求可携带 `model`
- auto resume 时，如果没有 `model`，返回 `not_requested`
- reasoning 优先级是 `req.reasoning_effort`，否则 fallback 到 `session.metadata["reasoning_effort"]`

这说明当前“暂停后恢复”仍然依赖请求里重新带 model，不是 session 自己自洽。

目标应改为：

- auto resume 默认直接读取 `session.model`
- reasoning 默认读取 `session.reasoning_effort`
- request 中的 model / reasoning 仅保留为兼容输入，不再是恢复执行的主路径

---

## 2.6 前端当前 model 来源仍偏向 provider config，需要改造成 session-aware

前端 `useActiveModel`：

- `lotus/src/pages/ChatPage/hooks/useActiveModel.ts:24`
- `lotus/src/pages/ChatPage/hooks/useActiveModel.ts:29`
- `lotus/src/pages/ChatPage/hooks/useActiveModel.ts:36`

```ts
const config = providerConfig.providers[currentProvider];
if ("model" in config && config.model) {
  return config.model;
}
```

这表示：

- 当前实现里还没有明确的 session-level active model hook
- 当前 UI 里“当前会话在用哪个 model”仍容易被 provider 全局配置主导

目标应改为：

- 前端要有明确的 session-aware model 读取逻辑
- 当前会话的 model 展示与请求发送，都应以 `chat.config.model` / session 接口返回值为主

---

## 2.7 前端当前的 model 切换 UI，目标应明确收敛为“改当前 session model”

`InputContainer` 里的 model dropdown：

- `lotus/src/pages/ChatPage/components/InputContainer/index.tsx:606`
- `lotus/src/pages/ChatPage/components/InputContainer/index.tsx:621`

当前实现逻辑仍在写 provider config：

```ts
nextProviders[currentProvider] = {
  ...(nextProviders[currentProvider] || {}),
  model: value,
};
await settingsService.saveProviderConfig({ provider: currentProvider, providers: nextProviders });
```

但根据本次重构目标，**这里的产品定义要明确为：用户在聊天页切换 model，就是在修改当前 session model**。

因此这里不能再把“改 provider config”当成目标方案，而应改成：

- 用户在聊天页下拉选择 model
- 改的是当前 session 的 `model`
- 其他 session 不受影响
- provider config 只负责新 session 默认值，不再承接此处的交互语义

---

## 2.8 前端 reasoning 已经“近似 session 级”，但持久化位置错误

前端 input state：

- `lotus/src/pages/ChatPage/store/slices/inputStateSlice.ts:15`
- `lotus/src/pages/ChatPage/store/slices/inputStateSlice.ts:19`
- `lotus/src/pages/ChatPage/store/slices/inputStateSlice.ts:61`

当前 reasoningEffort：

- Zustand 中按 `sessionId -> inputState.reasoningEffort`
- 同时还写入 localStorage：
  - `chat_input_reasoning_by_session_v1`
  - `chat_input_reasoning_last_used_v1`

这说明：

- 前端 UX 上已经接近“session 各自 reasoning”
- 但真正权威来源不是 session.json，而是浏览器 localStorage
- 这会导致：
  - 跨端不一致
  - 刷新后与后端 session 状态可能漂移
  - respond/execute 时只能“临时传参”

---

## 2.9 create/patch session API 目前太弱，不足以承载 session-level config

后端 session API：

- `CreateSessionRequest` 当前只有：`title` / `system_prompt` / `model`
  - `bamboo/src/server/handlers/agent/sessions/types.rs:59`
- `PatchSessionRequest` 当前只有：`title` / `pinned`
  - `bamboo/src/server/handlers/agent/sessions/types.rs:99`

因此现状问题：

- create session 还不能显式写 reasoning effort
- patch session 根本不能改单 session 的 model / reasoning
- frontend 也没有官方 session config patch 通道

---

## 2.10 `config.json` 确实已经变重；`model_limits` 目前挂在 root extra

后端 `Config`：

- `bamboo/src/core/config.rs:177`
- `bamboo/src/core/config.rs:182`

```rust
#[serde(default, flatten)]
pub extra: BTreeMap<String, Value>,
```

`model_limits` 当前不是强类型字段，而是 root extra key，被前端通过 `/bamboo/config` 直接存入：

- `lotus/src/pages/SettingsPage/ModelLimitsSettings.tsx:271`
- `lotus/src/pages/SettingsPage/ModelLimitsSettings.tsx:359`

同时后端 budget 模块仍保留对 `model_limits.json` 兼容/加载路径：

- `bamboo/src/agent/core/budget/limits.rs:149`

所以当前是典型的“双轨状态”：

- UI/配置管理倾向于统一进 `config.json`
- budget resolver 仍保留独立文件通道

这正好给“拆出 model limits 独立文件”提供了低风险路径。

---

## 3. 目标状态设计

## 3.1 配置边界重定义

### 全局配置（provider-level / app-level）

继续放在全局配置中：

- `provider`
- `providers.<provider>.model` → **仅作为新 session 默认 model**
- `providers.<provider>.reasoning_effort` → **仅作为新 session 默认 reasoning effort**
- `providers.<provider>.fast_model` → 全局
- `providers.<provider>.vision_model` → 全局
- API keys / base_url / hooks / mcp / skills / tools 等

### Session 配置（session-level）

放在 `session.json`：

- `model`
- `reasoning_effort`
- 已有的 workspace / prompt / selected skills 等 session metadata

### 独立配置文件

建议拆出：

- `model_limits.json`

可选后续再考虑：

- `model_mappings.json`（如果 anthropic/gemini mapping 后续继续膨胀）
- `skills.json`
- `tools.json`

但本次优先只拆 `model_limits`，避免重构面过大。

---

## 3.2 session.json 的推荐结构

当前 `Session` 已有 `model: String`，建议补充一个强类型字段承载 reasoning，而不是继续塞 metadata：

```rust
pub struct Session {
    ...
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    pub metadata: HashMap<String, String>,
    ...
}
```

### 为什么建议新增强类型字段，而不是继续 metadata？

因为它满足以下要求：

- 语义明确
- API schema 更稳定
- 前端类型安全更好
- session file 可读性更高
- 避免 metadata 既当“配置”又当“执行痕迹”导致混淆

### metadata 中建议保留什么？

- `reasoning_effort_source` 可以继续作为调试痕迹
- `workspace_path`
- `base_system_prompt`
- `selected_skill_ids`
- 其他 runtime/supplemental 数据

但 **`reasoning_effort` 不再作为 metadata 主来源**。

---

## 4. 推荐优先级规则

## 4.1 新 session 创建时

### model

```text
request.model
> provider default model
> "unknown"（仅兼容旧逻辑，不建议长期保留）
```

### reasoning_effort

```text
request.reasoning_effort
> provider default reasoning_effort
> None
```

### 解释

- 全局 model / reasoning 不再表示“所有 session 共用”
- 只表示“新建 session 时预填什么”

---

## 4.2 chat 时

### 推荐行为

对于已有 session：

```text
request.model（若显式提供） -> 覆盖 session.model
否则沿用 session.model
```

但为了最终产品语义清晰，推荐前端在正常聊天中 **始终基于 session 当前配置传值**，后端作为兜底：

```text
effective_model = request.model ?? session.model ?? provider default
```

不过如果前端改造完成，后端最好直接收敛为：

- 已有 session 正常聊天时，request.model 可选
- 若缺失则使用 session.model

---

## 4.3 execute 时

### model

目标优先级：

```text
session.model
> provider default model
> request.model（仅兼容迁移输入，不作为长期主路径）
```

更准确地说，产品语义必须是：

- **execute 是 session-driven**
- 正常执行时，应直接读取 `session.model`
- 只有在兼容旧客户端、session 尚未完成迁移时，才临时吸收 request.model 并回写 session

### reasoning_effort

目标优先级：

```text
session.reasoning_effort
> provider default reasoning_effort
> request.reasoning_effort（仅兼容迁移输入）
> None
```

### 设计含义

- session 是真正配置中心
- provider config 只在 session 未显式配置时兜底
- request 只保留为兼容迁移输入，不再定义 execute 的主行为

---

## 4.4 respond auto-resume 时

目标优先级：

### model

```text
session.model
> provider default model
> request.model（仅兼容迁移输入）
```

### reasoning_effort

```text
session.reasoning_effort
> provider default reasoning_effort
> request.reasoning_effort（仅兼容迁移输入）
> None
```

### 关键变化

`respond submit` 不应再因为前端没传 `model` 就返回 `not_requested`。

应该改为：

- 如果 session 有可执行 model，就自动 resume
- 只有 session / provider default 都无法解析有效 model 时才返回 `invalid_model`
- request 字段只在兼容旧前端时临时吸收，不再主导恢复逻辑

这会让 session 的恢复行为真正自洽，并且与 session-driven execute 保持一致。

---

## 5. 后端改造方案（bamboo）

## 5.1 Phase A：先把 session-level reasoning 做成强类型

### 变更点

#### 1) 扩展 `Session`

文件：
- `bamboo/src/agent/core/agent/types.rs`

新增：

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub reasoning_effort: Option<ReasoningEffort>,
```

并更新：

- `Session::new`
- `Session::new_child`

默认填 `None`

---

#### 2) 新建 session 时写入 reasoning_effort

文件：
- `bamboo/src/server/handlers/agent/sessions/types.rs`
- `bamboo/src/server/handlers/agent/sessions/handlers/crud/create.rs`

扩展 `CreateSessionRequest`：

```rust
pub struct CreateSessionRequest {
    pub title: Option<String>,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
}
```

构造 session 时：

```text
req.reasoning_effort.or(config.get_reasoning_effort())
```

写入 `session.reasoning_effort`

---

#### 3) 扩展 patch session API

文件：
- `bamboo/src/server/handlers/agent/sessions/types.rs`
- `bamboo/src/server/handlers/agent/sessions/handlers/crud/patch.rs`

把 `PatchSessionRequest` 扩为：

```rust
pub struct PatchSessionRequest {
    pub title: Option<String>,
    pub pinned: Option<bool>,
    pub model: Option<String>,
    pub reasoning_effort: Option<Option<ReasoningEffort>>,
}
```

> 推荐对 `reasoning_effort` 使用三态 patch 语义：
> - 字段缺失：不改
> - `"reasoning_effort": "high"`：设置
> - `"reasoning_effort": null`：清空，回落到 provider default

如果不想引入复杂 patch 类型，也可以先做简化版：

```rust
pub model: Option<String>,
pub reasoning_effort: Option<ReasoningEffort>,
pub clear_reasoning_effort: Option<bool>,
```

但长期看三态更干净。

---

## 5.2 Phase B：统一 execute / respond / chat 的来源优先级

### execute

文件：
- `bamboo/src/server/handlers/agent/execute/types.rs`
- `bamboo/src/server/handlers/agent/execute/handler/mod.rs`

推荐把 `ExecuteRequest.model` 从 required 改为 optional，并把 execute 明确改成 **session-driven**：

```rust
pub model: Option<String>
```

执行时推荐逻辑：

```rust
let effective_model =
    non_empty(session.model)
    .or_else(|| config_snapshot.get_default_model())
    .or(req.model)
```

更准确的实现语义应是：

- 先读取 `session.model`
- session 没值时再回落到 provider default
- 只有兼容旧客户端时，才吸收 `req.model`，并立刻回写到 `session.model`

同时：

```rust
let effective_reasoning_effort =
    session.reasoning_effort
    .or(config_snapshot.get_reasoning_effort())
    .or(req.reasoning_effort)
```

同理：

- 先看 `session.reasoning_effort`
- 再看 provider default
- `req.reasoning_effort` 只作为兼容迁移输入

并在执行前将最终值写回 session：

- `session.model = effective_model.clone()`
- `session.reasoning_effort = effective_reasoning_effort`

这样 session.json 永远反映“当前会话配置”，且 execute 的主路径完全是 session-driven。

---

### respond submit

文件：
- `bamboo/src/server/handlers/agent/respond/handlers/submit.rs`

改造：

- `requested_model` 不再是 auto resume 的唯一来源
- 默认直接读 `session.model`，其次 provider default
- request model 只在兼容期吸收
- reasoning 用 `session.reasoning_effort` 而非 metadata 主导

建议改成：

```text
auto_resume_status =
  if effective_model exists -> execute(...)
  else -> invalid_model
```

这样 `respond` 与 execute 使用同一套 session-driven 配置来源。

---

### chat

文件：
- `bamboo/src/server/handlers/agent/chat/types.rs`
- `bamboo/src/server/handlers/agent/chat/handler/mod.rs`

建议分两步走：

#### 过渡期
保留 `ChatRequest.model: String` 必填，兼容当前前端。

#### 最终目标
改成：

```rust
pub model: Option<String>
pub reasoning_effort: Option<ReasoningEffort>
```

并按 session/provider default 兜底。

不过为了降低第一阶段风险，**chat 接口可以后置改造**，先改 create/patch/execute/respond。

---

## 5.3 Phase C：把 session 摘要 / 获取接口补齐 session config

当前 `SessionSummary` 不包含 model / reasoning：

- `bamboo/src/server/handlers/agent/sessions/types.rs:5`

建议补充：

```rust
pub model: String,
#[serde(default, skip_serializing_if = "Option::is_none")]
pub reasoning_effort: Option<ReasoningEffort>,
```

这样前端列表页和聊天页切换 session 时，不必额外拉完整 history 才知道当前 session model。

如果担心 summary 变重，也可以：

- `SessionSummary` 带 `model`
- `GetSessionResponse` 带 `model + reasoning_effort + workspace_path`

但从使用便利性看，建议 summary 直接带。

---

## 5.4 Phase D：建立明确的 session config API（推荐）

如果你们想把 session 配置语义做清晰，建议新增专门结构，而不是继续让 patch session 很泛。

例如：

### Option A：继续复用 `PATCH /sessions/{id}`

优点：
- 变动最小

缺点：
- session config 和 UI 元数据混在一个 patch 里

### Option B：新增专门接口

```text
GET   /api/v1/sessions/{id}/config
PATCH /api/v1/sessions/{id}/config
```

返回：

```json
{
  "model": "gpt-5.3-codex",
  "reasoning_effort": "high",
  "workspace_path": "/Users/...",
  "base_system_prompt": "..."
}
```

我更推荐 **Option B**，因为这次重构的核心就是“session config 是一等对象”。

---

## 6. 前端改造方案（lotus）

## 6.1 前端状态分层重构

当前主要问题：

- `useActiveModel()` 只看 provider config
- model dropdown 改的是 provider config
- reasoningEffort 虽然按 session 存，但权威来源是 localStorage

### 目标分层

#### Provider-level
来自 `providerStore.providerConfig`：
- provider default model
- provider default reasoning
- fast_model
- vision_model

#### Session-level
来自 `chat.config` 或 `session summary`：
- current session model
- current session reasoning_effort

#### Request-level
只作为临时 override，正常 UI 不应主要依赖它

---

## 6.2 扩展前端 ChatItem.config

当前 `ChatItem.config` 里已有：

- `systemPromptId`
- `baseSystemPrompt`
- `workspacePath`
- `tokenUsage`
- `syncCursor`
- 等等

建议补：

```ts
config: {
  ...
  model?: string;
  reasoningEffort?: ReasoningEffort | null;
}
```

这样 session-level 配置可以自然进入前端 store。

---

## 6.3 新增 session-level hooks

### 替换/重构 `useActiveModel`

当前：
- `useActiveModel()` -> provider global model

建议改为：

### `useSessionModel(sessionId)`

优先级：

```text
chat.config.model
> providerConfig.providers[currentProvider].model
> undefined
```

### `useSessionReasoningEffort(sessionId)`

优先级：

```text
chat.config.reasoningEffort
> provider default reasoning_effort
> "medium"（仅 UI 默认展示值）
```

### `useFastModel()` / `useVisionModel()`

保持当前设计：

```text
provider fast_model / vision_model
> active provider model fallback
```

但注意 fallback 最终应该参考 **session active model**，不是 provider global model。

也就是说后续最好改成：

- `useFastModel(sessionId?)`
- `useVisionModel(sessionId?)`

fallback 到 `session model`

不过你们本次明确说 fast/vision 继续全局，因此第一阶段可不动 UI，只要后端逻辑不被误改即可。

---

## 6.4 聊天输入区 model dropdown 改成 patch session，不再改 provider config

当前：
- `InputContainer.handleModelSelect()` 还在改 `settingsService.saveProviderConfig(...)`

这里的目标不是“可以考虑改 session”，而是已经明确：

- **`InputContainer` 切 model，就是改当前 session model**
- 改法应当是 `patchSession`
- 这个交互不应再写 provider 全局配置

### 关键替换点

文件：
- `lotus/src/pages/ChatPage/components/InputContainer/index.tsx:606`

目标逻辑：

```ts
await agentClient.patchSession(sessionId, { model: value });
updateSession(sessionId, {
  config: {
    ...currentChat.config,
    model: value,
  },
});
```

### 注意事项

- 没有 sessionId 时，先创建 session 再更新，或者在创建时直接带 model
- 如果是“新会话首页还没真正建 session”状态，需要定义 UX：
  - 方案 A：临时保存在草稿态，首条消息发送时 createSession 带上
  - 方案 B：用户一改 model 就立即 createSession

推荐 **方案 A**，更省后端对象创建。

---

## 6.5 reasoning dropdown 改成 patch session，不再依赖 localStorage 为主

当前 reasoning 控件：
- `InputContainer` 调 `setInputReasoningEffort(sessionId, ...)`
- 再写 localStorage

目标：

- 改为 patch session config
- localStorage 仅用于“未建 session 时的草稿态”

### 建议优先级

#### 已有 session
用户改 reasoning 时：

```text
PATCH /sessions/{id} { reasoning_effort: "high" }
```

然后更新 `chat.config.reasoningEffort`

#### 尚未创建 session
先暂存在本地 draft state，发送第一条消息创建 session 时带上。

### 最终要点

- `inputStateSlice` 不再作为 reasoning 的权威来源
- 它只负责 input UI 草稿状态
- 权威状态应转移到 session backend data

---

## 6.6 sendMessage / execute / respond 全部改为从 session config 取值

当前：
- `useMessageStreaming` 使用 `activeModel = useActiveModel()`
- `InputContainer` respond 也使用 `activeModel`
- `QuestionDialog` 也使用 `activeModel`

目标：

### sendMessage
改成：

```text
model = currentChat.config.model ?? provider default model
reasoning = currentChat.config.reasoningEffort ?? provider default reasoning
```

### execute
必须与后端 session-driven 目标一致：

```text
前端正常执行时直接发送当前 session config
后端执行时以 session.json 为主来源
```

### respond
同样对齐 session config：

```text
model = currentChat.config.model ?? provider default model
reasoning = currentChat.config.reasoningEffort ?? provider default reasoning
```

### 好处

- 当前 session 的所有请求都自洽
- provider config 改动不会影响旧 session
- 自动恢复不会错用新 provider model
- 前后端都围绕 session 配置运行，不再出现“前端以全局 model 驱动、后端以 request 驱动”的语义漂移

---

## 6.7 create session 流程调整

当前 `CreateSessionRequest` 只有 model 没有 reasoning。

建议前端创建 session 时直接传：

```json
{
  "title": "...",
  "system_prompt": "...",
  "model": "session-default-model",
  "reasoning_effort": "medium"
}
```

这样一创建就把 session config 固化。

对于“空白新会话尚未落地”的 UI，可维护一个 `draftSessionConfig`：

```ts
{
  model?: string;
  reasoningEffort?: ReasoningEffort;
}
```

首条消息发送前：

1. resolve draft config
2. create session
3. send chat
4. execute

---

## 7. 推荐的数据模型演进

## 7.1 后端 Session（推荐）

```rust
pub struct Session {
    pub id: String,
    pub title: String,
    pub pinned: bool,
    pub kind: SessionKind,
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    pub spawn_depth: u32,
    pub messages: Vec<Message>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub task_list: Option<TaskList>,
    pub pending_question: Option<PendingQuestion>,
    pub model: String,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub metadata: HashMap<String, String>,
    ...
}
```

## 7.2 前端 ChatItem.config（推荐）

```ts
config: {
  systemPromptId: string;
  baseSystemPrompt: string;
  lastUsedEnhancedPrompt?: string | null;
  workspacePath?: string;
  model?: string;
  reasoningEffort?: ReasoningEffort | null;
  tokenUsage?: ...;
  syncCursor?: ...;
}
```

---

## 8. 配置拆分方案：先拆 `model_limits`

## 8.1 为什么先拆它

`model_limits` 有几个特点：

- 独立度高
- 数据量可能较大
- 编辑频率与 provider config 不同
- 已经有 legacy `model_limits.json` 兼容路径

所以非常适合第一优先级拆分。

---

## 8.2 建议目标

### 全局配置主文件

`~/.bamboo/config.json`

只保留：

- provider/provider configs
- proxy
- hooks
- tools/skills
- mcp
- setup/app-level 状态

### 独立文件

`~/.bamboo/model_limits.json`

内容：

```json
[
  {
    "vendor": "OpenAI",
    "model_pattern": "gpt-5.3-codex",
    "max_context_tokens": 400000,
    "max_output_tokens": 128000,
    "safety_margin": 1000,
    "note": "..."
  }
]
```

---

## 8.3 后端实现建议

### 不要在 `Config.extra` 中继续长期持有 `model_limits`

短期兼容：

- 读取时：如果 `config.json.model_limits` 存在，迁移/合并到 `model_limits.json`
- 写入时：UI 新接口直接写 `model_limits.json`
- `/bamboo/config` 返回时可不再注入 `model_limits`，或仅在兼容期注入只读副本

### 更好的做法

为 model limits 提供专门 endpoint：

```text
GET  /api/v1/bamboo/model-limits
PUT  /api/v1/bamboo/model-limits
GET  /api/v1/bamboo/model-limits/defaults
```

而不是继续通过通用 `bamboo/config` root patch。

---

## 8.4 前端实现建议

把 `ModelLimitsSettings.tsx` 改为使用专门 API：

当前：

- `getBambooConfig()` 读 root `model_limits`
- `setBambooConfig({ model_limits })` 写 root config

建议改成：

- `getModelLimits()`
- `setModelLimits()`
- `getModelLimitDefaults()` 保持

这样：

- 配置边界更清晰
- `config.json` 瘦身
- 后端不用再通过 `extra` 承载结构化大字段

---

## 9. 推荐迁移路径（低风险分阶段）

## Phase 1：后端先支持 session-level config，但保持兼容

### 后端
1. `Session` 新增 `reasoning_effort`
2. `CreateSessionRequest` 支持 `reasoning_effort`
3. `PatchSessionRequest` 支持 `model` / `reasoning_effort`
4. `SessionSummary` / `GetSessionResponse` 增加 `model` / `reasoning_effort`
5. `execute` 改为 **session-driven**，主读取 `session.model` / `session.reasoning_effort`
6. `respond` 改为与 execute 相同的 **session-driven** 恢复逻辑
7. 继续接受旧请求格式，但 request 字段只用于兼容迁移输入

### 前端
1. 先把 session 数据读回来并存入 `chat.config`
2. 不立刻删旧逻辑，先让 UI 能感知 session model/reasoning

### 风险
低

---

## Phase 2：前端把聊天页 model/reasoning 控件改为 session patch

### 前端
1. `InputContainer` model dropdown 改 patch session
2. reasoning dropdown 改 patch session
3. `useActiveModel` 演化为 session-aware hook
4. `sendMessage/execute/respond` 使用 session config

### 后端
无需大改，只消费 Phase 1 提供的 session config

### 风险
中等，但可控

---

## Phase 3：把全局 model/reasoning 语义收敛为“新 session 默认值”

### 产品/实现变化
1. Settings 页文案调整
2. Provider config 的 `model` / `reasoning_effort` UI 文案改为：
   - Default model for new sessions
   - Default reasoning effort for new sessions
3. 旧 session 完全不受 provider 改动影响

### 风险
低

---

## Phase 4：拆出 `model_limits.json`

### 后端
1. 新增 model limits 专用读写接口
2. budget resolver 统一从 `model_limits.json` 读取
3. 兼容迁移 root `config.json.model_limits`

### 前端
1. `ModelLimitsSettings` 改用专用 API
2. 不再通过 `bamboo/config` 读写 `model_limits`

### 风险
低到中等

---

## 10. 兼容与迁移细节

## 10.1 旧 session 的迁移

已有 session.json 只有：
- `model`
- 没有 `reasoning_effort`

迁移方式：

- 不需要离线批量迁移
- 反序列化时 `reasoning_effort = None`
- 首次 execute / patch / respond 后再回填

这是最安全的 lazy migration。

---

## 10.2 旧前端仍传 request model/reasoning 时

后端应继续接受：

- `chat.model`
- `execute.model`
- `execute.reasoning_effort`
- `respond.model`
- `respond.reasoning_effort`

但这些字段只应被视作 **兼容迁移输入**，而不是长期的主驱动来源。

推荐策略：

- 新值进来后，优先回写 session
- 一旦 session 已有稳定配置，执行链路继续以 session 为主

这保证前后端可以分步上线，同时不破坏 session-driven 目标。

---

## 10.3 `session.model == "unknown"` 的历史包袱

当前 create_session fallback：

- `bamboo/src/server/handlers/agent/sessions/handlers/crud/create.rs:75`

```rust
unwrap_or_else(|| "unknown".to_string())
```

建议：

### 过渡期
保留兼容

### 最终目标
避免创建出 `unknown` model session：

```text
request.model
> provider default model
> 报错/拒绝创建
```

否则会导致后续 execute/respond 逻辑需要额外兜底。

---

## 11. 推荐接口变更清单

## 11.1 后端 API

### CreateSessionRequest

```json
{
  "title": "string?",
  "system_prompt": "string?",
  "model": "string?",
  "reasoning_effort": "low|medium|high|xhigh|max|null"
}
```

### PatchSessionRequest

```json
{
  "title": "string?",
  "pinned": true,
  "model": "string?",
  "reasoning_effort": "low|medium|high|xhigh|max|null"
}
```

> 如果需要区分“未传”和“传 null”，请用 patch wrapper 或额外 clear 字段。

### SessionSummary / GetSessionResponse

返回新增：

```json
{
  "model": "gpt-5.3-codex",
  "reasoning_effort": "high"
}
```

### ExecuteRequest

最终建议：

```json
{
  "model": "string?",
  "reasoning_effort": "... ?",
  "client_sync": { ... }
}
```

---

## 11.2 前端状态接口

### ChatItem.config

新增：

```ts
model?: string;
reasoningEffort?: ReasoningEffort | null;
```

### hooks

新增/重构：

- `useSessionModel(sessionId)`
- `useSessionReasoningEffort(sessionId)`
- `useSessionModelInfo(sessionId)`

---

## 12. 回归测试建议

## 12.1 后端测试

### session create / patch
- 创建 session 时不传 model，使用 provider default
- 创建 session 时显式传 model/reasoning，覆盖 provider default
- patch session model
- patch session reasoning
- patch 清空 reasoning 后 execute fallback 到 provider default

### execute
- session model 主导 execute
- session reasoning 主导 execute
- session 无值时正常 fallback 到 provider default
- 旧客户端仍传 request model/reasoning 时，会被兼容吸收并回写 session

### respond
- 不传 model 时仍可 auto resume（如果 session 有 model）
- 不传 reasoning 时使用 session reasoning
- session reasoning 为空时 fallback provider default
- 恢复执行与 execute 使用同一套 session-driven 取值逻辑

### storage
- session.json 正确持久化 `reasoning_effort`
- 旧 session.json 可正常反序列化

---

## 12.2 前端测试

### UI 行为
- 在 A session 改 model，不影响 B session
- 在 A session 改 reasoning，不影响 B session
- 切换 session 后控件显示各自值
- provider default 仅影响新建 session

### 请求行为
- sendMessage 使用当前 session config
- execute 使用当前 session config
- respond 使用当前 session config

### 新会话
- 新建 session 首次继承 provider defaults
- 修改 provider default 后，已有 session 不变化，新 session 使用新默认

---

## 13. 最推荐的落地顺序

如果按工程风险排序，我建议：

### Step 1
后端先做：
- `Session.reasoning_effort`
- create/patch session 支持 model/reasoning
- session summary 返回 model/reasoning

### Step 2
后端再做：
- execute/respond 的 session-driven 主路径
- request 字段仅保留兼容迁移入口

### Step 3
前端再做：
- session-aware model/reasoning state
- InputContainer 改 patch session
- send/execute/respond 读 session config

### Step 4
最后做：
- `model_limits` 独立文件和专用接口

这是最稳的顺序，因为：

- 先把后端能力建起来
- 再让前端逐步切流
- 最后再收拾全局配置瘦身

---

## 14. 结论

这次重构的核心不是“新增大量复杂功能”，而是 **把真正的配置主语从 global provider 切换到 session**。

### 一句话总结

- `provider.model` / `provider.reasoning_effort`：以后只是 **new session defaults**
- `InputContainer` 切 model：定义上就是 **改当前 session model**
- `session.model` / `session.reasoning_effort`：才是 **运行时真实来源**
- `execute` / `respond auto-resume`：都应改为 **session-driven**
- `fast_model` / `vision_model`：继续保留全局
- `model_limits`：优先拆出 `config.json`

### 当前代码基础其实不错

因为：

- session.json 已存在
- `Session.model` 已存在
- reasoning 也已接近 session 级 UX
- model limits 已保留独立文件兼容路径

所以这不是推倒重来，而是一次 **配置边界校正 + 数据主权回归到 session** 的重构。

---

## 15. 建议的首批实际改动文件

### bamboo
- `src/agent/core/agent/types.rs`
- `src/server/handlers/agent/sessions/types.rs`
- `src/server/handlers/agent/sessions/handlers/crud/create.rs`
- `src/server/handlers/agent/sessions/handlers/crud/patch.rs`
- `src/server/handlers/agent/sessions/handlers/crud/query.rs`
- `src/server/handlers/agent/execute/types.rs`
- `src/server/handlers/agent/execute/handler/mod.rs`
- `src/server/handlers/agent/respond/handlers/submit.rs`

### lotus
- `src/pages/ChatPage/hooks/useActiveModel.ts`
- `src/pages/ChatPage/components/InputContainer/index.tsx`
- `src/pages/ChatPage/hooks/useChatManager/useMessageStreaming.ts`
- `src/components/QuestionDialog/QuestionDialog.tsx`
- `src/pages/ChatPage/store/slices/chatSessionSlice.ts`
- `src/pages/ChatPage/store/slices/inputStateSlice.ts`
- `src/pages/ChatPage/services/AgentService.ts`
- `src/pages/SettingsPage/ModelLimitsSettings.tsx`
- `src/services/common/ServiceFactory.ts`
