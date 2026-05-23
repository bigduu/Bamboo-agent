# Bamboo Prompt / Message / Cache 重构方案

## 1. 背景与核心问题

当前 Bamboo 的 prompt 组织方式里，较多动态信息被直接拼进 system prompt。这会导致：

1. **system prompt 每轮都会变化**
2. 很难稳定命中 provider 的 **prompt cache**
3. compression 虽然能减 token，但没有系统性地服务于 **stable prefix**
4. Chat Completions / Responses 两条请求路径在语义分层上没有统一设计

当前实现的关键现状包括：

- `prompt_setup::apply_system_prompt_contexts(...)` 会把 workspace / instruction / env / skill / tool guide 拼进 system prompt
- `round_prelude::refresh_round_prompt_context(...)` 又会在每轮把 external memory / task list / plan runtime / plan mode 注入 system prompt
- `prepare_hybrid_context(...)` 和 compression 主要围绕 `Session.messages` 做预算裁剪与摘要回写
- OpenAI Chat Completions 路径直接序列化 `messages`
- OpenAI Responses 路径支持 `instructions`，但当前 Bamboo 自身的运行时组织还没有充分利用这一点

这意味着 Bamboo 当前对 provider 的请求前缀不稳定，prompt cache 很难稳定命中。

---

## 2. 重构目标

这次重构的核心目标不是简单“瘦身 prompt”，而是建立一套稳定、统一、可演进的 prompt 架构：

1. **稳定规则进入 stable instructions/system**
2. **动态运行时上下文迁移到 message 层**
3. **Chat Completions 与 Responses 共享同一套内部抽象**
4. **compression 从“只减 token”升级成“帮助 prompt cache 命中”**
5. **尽量减少第一阶段对持久化 schema 的冲击**

一句话概括：

> 把 Bamboo 的请求前缀拆成“稳定指令层”和“动态上下文层”，让 prompt cache 可以命中稳定前缀，同时保留 Bamboo 当前的 session / message / compression 机制。

---

## 3. 设计原则

### 3.1 先建立中间抽象，再改 provider 序列化

不要先在 OpenAI provider 上打补丁，而是先让 Bamboo 内部知道：

- 哪些是 stable instructions
- 哪些是 dynamic runtime context
- 哪些是 actual conversation window

### 3.2 第一阶段尽量不改 Session 持久化 schema

当前很多逻辑已经依赖：

- `Session.messages`
- `Session.prompt_snapshot`
- `Session.conversation_summary`
- `Session.compression_events`

所以第一阶段优先做运行时 envelope 重构，不强制改持久层 message taxonomy。

### 3.3 先让 system / instructions 稳定下来

第一阶段最重要的不是形式最优雅，而是：

> stable instructions 在同一 session 的多轮请求中尽量保持不变

只要这一点成立，cache hit 就会先得到显著改善。

### 3.4 内部抽象优先贴近 Responses 语义

Responses 原生区分：

- `instructions`
- `input`

这和目标设计天然契合。因此 Bamboo 内部抽象应更接近 Responses，再向 Chat Completions 适配。

---

## 4. 当前实现分析

## 4.1 system prompt 在两阶段被不断改写

第一阶段来自 `prompt_setup`：

- `base_prompt`
- `workspace_context`
- `instruction_context`
- `env_context`
- `skill_context`
- `tool_guide_context`

第二阶段来自每轮刷新：

- `external_memory`
- `task_list`
- `plan_runtime_context`
- `plan_mode_instructions`

这意味着当前 effective system prompt 在 session 生命周期内高度不稳定。

## 4.2 Compression 有两层机制

### A. 真正的历史压缩

在 context pressure 达到阈值后：

- 选取较旧消息
- 调用 summarizer 生成 `conversation_summary`
- 把旧消息标记为 `compressed = true`
- 后续 prepared context 不再带这些旧消息

### B. Prompt-side microcompact

在真正发给模型前，`prepare_hybrid_context(...)` 会尝试：

- compact 老的超长 tool output
- compact 老的超长 assistant analysis

这类压缩不改 session 历史，只改本次 request 里的 message 形态。

## 4.3 现有 compression 对 cache 的帮助是局部的

当前已经有一些有利于 prompt cache 的逻辑：

- 优先 compact 老旧、冗长、低价值的大块 tool output
- 保留最近 user turns 与最近 tool chains
- 用 summary 替代被压缩的历史消息

但它仍有明显限制：

1. **system prompt 仍在变化**，cache 前缀先天不稳定
2. `conversation_summary` 目前作为 system message 注入 prepared context，仍然污染前缀稳定性
3. external memory / task list / plan mode 也都继续进入 system
4. compression 和 prompt 架构尚未统一设计

---

## 5. 目标架构

重构后的请求结构应拆成四层：

1. **Stable Instructions**
2. **Stable Prefix Messages**
3. **Dynamic Context Messages**
4. **Conversation Messages**

### 5.1 Stable Instructions

放入 system / instructions：

- base system prompt
- 稳定的 repo / workspace 规则（如仓库 policy、固定工具策略）
- 稳定工具使用原则
- 稳定技能触发规则
- plan mode 的规则性约束（如果 plan mode 能视为 session-stable）

特点：

- 同一 session 多轮内尽量不变
- 是 cache 最有价值的前缀

### 5.2 Stable Prefix Messages

作为 synthetic messages 保留，但尽量 session-stable：

- 如需要的固定说明块
- 少量不适合塞入 system、但足够稳定的 host context

### 5.3 Dynamic Context Messages

迁出 system，改为 message：

- task snapshot
- external memory
- conversation summary
- recovery snapshot
- plan runtime state
- plan mode 当前状态
- 必要的 env snapshot

特点：

- 允许每轮变化
- 应放在 stable instructions 之后、conversation 之前
- 应视为“host runtime context”，而不是新的用户请求

### 5.4 Conversation Messages

就是当前窗口化后的历史消息与近期上下文：

- recent user / assistant turns
- tool calls / tool results
- prepared context 剩余窗口

---

## 6. 目标中间抽象：PromptEnvelope

建议新增运行时抽象 `PromptEnvelope`，作为 Bamboo 统一的请求组织结构。

```rust
#[derive(Debug, Clone, Default)]
pub struct PromptEnvelope {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<bamboo_agent_core::Message>,
    pub dynamic_context_messages: Vec<bamboo_agent_core::Message>,
    pub conversation_messages: Vec<bamboo_agent_core::Message>,
    pub observability: PromptEnvelopeObservability,
}
```

### 6.1 观测字段

```rust
#[derive(Debug, Clone, Default)]
pub struct PromptEnvelopeObservability {
    pub stable_instructions_chars: usize,
    pub stable_prefix_message_count: usize,
    pub dynamic_context_message_count: usize,
    pub conversation_message_count: usize,
    pub stable_prefix_hash: Option<String>,
    pub dynamic_context_hash: Option<String>,
    pub included_block_types: Vec<ContextBlockType>,
}
```

### 6.2 内部 ContextBlock 表达

```rust
#[derive(Debug, Clone)]
pub struct ContextBlock {
    pub block_type: ContextBlockType,
    pub priority: ContextBlockPriority,
    pub stability: ContextBlockStability,
    pub title: String,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
}
```

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContextBlockType {
    Workspace,
    InstructionOverlay,
    ToolGuide,
    SkillContext,
    ConversationSummary,
    TaskSnapshot,
    ExternalMemory,
    MemoryRecall,
    PlanModeState,
    PlanRuntimeState,
    EnvSnapshot,
    RecoverySnapshot,
}
```

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContextBlockPriority {
    Critical,
    High,
    Medium,
    Low,
}
```

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextBlockStability {
    Stable,
    SessionStable,
    RoundDynamic,
}
```

### 6.3 为什么最终仍然使用 `Message`

第一阶段不建议让 provider 直接吃 `ContextBlock`，而应继续使用 `Message` 作为 runtime carrier：

1. 当前 `LLMProvider::chat_stream_with_options(...)` 就收 `&[Message]`
2. `prepare_hybrid_context(...)`、segmenter、compression 都基于 `Message`
3. 这样迁移范围更可控

因此内部形态应为：

```text
ContextBlock -> synthetic Message
PromptEnvelope -> provider-specific request
```

---

## 7. Synthetic Context Message 规范

第一阶段建议把 dynamic context block 最终转成 synthetic `user` message，而不是继续塞进 system。

原因：

1. 对 Chat Completions 和 Responses 都最稳
2. 避免重复引入第二个动态 system 层
3. 与现有 `Message` 模型兼容最好

### 7.1 推荐模板

```text
<!-- BAMBOO_CONTEXT_BLOCK_START -->
context_type: task_snapshot
priority: high
stability: round_dynamic
title: Current Task Snapshot

This is runtime context from the host system.
It is not a new user request.
Follow the latest real user request and recent tool results over this block when they conflict.

... block content ...
<!-- BAMBOO_CONTEXT_BLOCK_END -->
```

### 7.2 推荐 metadata

建议第一阶段就写入 `Message.metadata`：

```json
{
  "bamboo_context_block": {
    "type": "task_snapshot",
    "priority": "high",
    "stability": "round_dynamic"
  }
}
```

这样后续便于：

- observability
- UI 调试
- 精准测试断言
- 后续 envelope-aware compression

---

## 8. Chat Completions / Responses 映射

## 8.1 Chat Completions

目标映射：

- 一个稳定的 `system` message：`stable_instructions`
- 若有 `stable_prefix_messages`，跟在后面
- 再跟 `dynamic_context_messages`
- 最后跟 `conversation_messages`

即：

```text
messages = [
  system(stable_instructions),
  ...stable_prefix_messages,
  ...dynamic_context_messages,
  ...conversation_messages,
]
```

## 8.2 Responses

Responses 应充分利用 `instructions` 字段。

目标映射：

- `instructions = stable_instructions`
- `input = stable_prefix_messages + dynamic_context_messages + conversation_messages`

即：

```text
instructions = stable_instructions
input = [
  ...stable_prefix_messages,
  ...dynamic_context_messages,
  ...conversation_messages,
]
```

### 关键原则

Responses 路径不要再把同一份 stable instructions 以 `system` message 重复塞入 `input`。否则：

1. 破坏语义分层
2. 影响前缀稳定性
3. 浪费 token

---

## 9. Compression 如何与 cache 协同

## 9.1 当前 compression 需要从“减 token”升级成“保护 stable prefix”

当前 compression 的价值主要是：

- 压旧消息
- 保最近消息
- 缩 tool output

但重构后需要新增一个设计目标：

> compression 不应再继续污染 stable instructions；它应尽量作用于 dynamic context 与 conversation window。

## 9.2 summary 应迁出 system

当前 `conversation_summary` 通过 `compression_summary_message(...)` 被包装成 system message 注入 request。这个做法不利于 cache。

重构后建议：

- `conversation_summary` 继续存储在 `Session.conversation_summary`
- 但 request 中不再把它作为 system message 注入
- 改为 `ConversationSummary` 类型的 dynamic context block

这样 summary 仍能保连续性，但不会破坏 stable prefix。

## 9.3 recovery message 也应逐步 block 化

当前 `apply_compression_plan(...)` 会插入一个 `post-compaction-recovery` assistant message。它的价值是保留：

- recently modified files
- active tasks
- key decisions

这类信息未来更适合改造成 `RecoverySnapshot` block，而不是作为普通对话历史消息混入 conversation。

第一阶段可以先兼容保留当前 recovery message，但在 request 组装时优先把它视为 dynamic context，而不是普通 assistant 历史。

## 9.4 prompt-side microcompact 应继续保留

当前已经存在的：

- tool output compact
- old assistant analysis compact

这些机制应继续保留，因为它们天然有助于 cache-friendly request：

- 减少低价值大块文本
- 稳定老上下文的文本形态
- 降低超长 tool 输出对 request prefix 的干扰

但它们应定位为：

> conversation / dynamic block 层的轻量裁剪，而不是 system 层的常规修补

---

## 10. 运行时组装流程改造

## 10.1 当前链路的问题

当前链路中，`prompt_setup` 和 `round_prelude` 都会直接改写 system message。导致：

- prompt 结构隐式耦合
- 各模块都能改 system
- 很难判断哪些内容是 stable，哪些是 dynamic

## 10.2 目标链路

建议改成：

```text
session setup
  -> build_stable_prompt_frame(...)
  -> persist stable prompt metadata

per round
  -> build_dynamic_context_blocks(...)
  -> prepare_conversation_window(...)
  -> assemble_prompt_envelope(...)
  -> compact_dynamic_blocks_if_needed(...)
  -> serialize envelope to provider request
```

这套链路的核心改变是：

- stable 和 dynamic 显式分层
- provider request 在最后一步统一生成
- 各模块不再直接改 system message

---

## 11. 模块改造建议

## 11.1 `prompt_setup.rs`

### 当前职责

- base prompt
- workspace / instruction / env context
- skill context
- tool guide context
- system merge
- prompt snapshot

### 目标职责

收缩为 stable frame builder，只负责 stable layers。

### 建议新增核心函数

```rust
pub struct StablePromptFrame {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
}
```

```rust
pub fn build_stable_prompt_frame(
    session: &Session,
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    activated_discoverable_tools: &BTreeSet<String>,
) -> StablePromptFrame
```

### 处理内容

保留：

- base prompt
- workspace context（若 session-stable）
- instruction context
- env context（若可视为 session-stable）
- stable skill rules
- stable tool guide

不再处理：

- task list
- external memory
- plan runtime
- per-round summary
- per-round plan state

---

## 11.2 `prompt_context/external_memory.rs`

### 当前

直接改写 system message。

### 目标

改造成 block builder：

```rust
pub async fn build_external_memory_block(
    session: &Session,
    prompt_memory_flags: PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) -> Option<ContextBlock>
```

保留现有加载逻辑：

- session note
- relevant memories
- project memory index
- project dream / global dream

但最终不直接写 system。

---

## 11.3 `prompt_context/task.rs`

### 目标

改为：

```rust
pub fn build_task_snapshot_block(session: &Session) -> Option<ContextBlock>
```

建议不要直接无脑搬运 full `format_task_list_for_prompt()`；可以分层：

优先保留：

- in_progress
- pending
- blocked
- progress summary

复杂详情可作为 fallback。

---

## 11.4 `prompt_context/plan_mode.rs`

应拆成两部分：

### A. 规则性约束

放入 stable instructions 中，例如 plan mode 的“禁止写文件 / 禁止执行命令”等规则性内容。

### B. 当前状态

新增：

```rust
pub fn build_plan_mode_state_block(session: &Session) -> Option<ContextBlock>
```

block 内容只放：

- 当前是否 active
- 当前 phase
- 当前限制摘要

而不是把整段硬规则在每轮再拼一次。

---

## 11.5 `prompt_context/plan_runtime.rs`

改为：

```rust
pub fn build_plan_runtime_block(
    session: &Session,
    app_data_dir: Option<&std::path::Path>,
) -> Option<ContextBlock>
```

---

## 11.6 `compression_tooling.rs`

### summary

当前 `compression_summary_message(...)` 返回 system message。建议未来改成 block builder：

```rust
pub fn build_conversation_summary_block(
    summary: &ConversationSummary,
) -> Option<ContextBlock>
```

### recovery snapshot

当前 recovery 逻辑：

```rust
build_post_compaction_recovery_message(...)
```

建议未来替换为：

```rust
pub fn build_recovery_snapshot_block(
    compressed_messages: &[Message],
    session: &Session,
) -> Option<ContextBlock>
```

第一阶段可以兼容保留 recovery assistant message，但新 request path 应优先将其视为 dynamic context。

---

## 12. Provider 接入策略

## 12.1 第一阶段不改 `LLMProvider` trait

当前：

```rust
chat_stream_with_options(
    messages: &[Message],
    ...,
    options: Option<&LLMRequestOptions>,
)
```

第一阶段建议保留这个 trait，不额外扩大 provider 改造面。

## 12.2 改造 `execute_llm_stream(...)`

核心思路：

1. 先构建 `PromptEnvelope`
2. 再根据 provider / protocol 序列化成最终 request shape

### Chat Completions

- `messages = envelope_to_chat_messages(envelope)`

### Responses

- `responses.instructions = Some(envelope.stable_instructions)`
- `input_messages = stable_prefix + dynamic + conversation`
- 避免重复把 stable instructions 放入 input

## 12.3 `execute_llm_stream(...)` 的预期变化

当前 `execute_llm_stream(...)` 直接使用 `prepared_context.messages`。重构后应变成：

```text
prepared_context.messages
  -> 视为 conversation layer
  -> 与 stable frame + dynamic blocks 一起组装成 PromptEnvelope
  -> 再映射为 provider request
```

---

## 13. PromptSnapshot 与持久化策略

## 13.1 第一阶段不建议大改 `PromptSnapshot` schema

当前 `PromptSnapshot` 仍然围绕 effective system prompt 组织，字段很多：

- `base_system_prompt`
- `workspace_context`
- `instruction_context`
- `env_context`
- `skill_context`
- `tool_guide_context`
- `external_memory`
- `task_list`
- `effective_system_prompt`

如果第一阶段直接重做，会波及：

- 存储兼容
- UI
- 大量现有测试

## 13.2 第一阶段建议写 metadata，不急于改 domain schema

建议先把新架构的 envelope 观测信息写入 `session.metadata`：

- `runtime_prompt_envelope_version`
- `runtime_prompt_stable_hash`
- `runtime_prompt_dynamic_hash`
- `runtime_prompt_dynamic_block_types`
- `runtime_prompt_dynamic_block_count`
- `runtime_prompt_stable_chars`

等新架构稳定后，再考虑把它升级为新的 `PromptEnvelopeSnapshot` 结构。

---

## 14. 实施步骤

## Step 1：引入 PromptEnvelope / ContextBlock 类型与 renderer

### 目标

先增加新的运行时抽象，不改变现有行为。

### 产物

- `PromptEnvelope`
- `PromptEnvelopeObservability`
- `ContextBlock`
- `render_context_block_message(...)`

### 验收

- 单元测试通过
- 不影响现有 provider 请求路径

## Step 2：把 dynamic context builder 从 system 注入中抽出来

### 目标

先实现下列 block builder，但暂时允许旧逻辑继续存在：

1. `build_task_snapshot_block`
2. `build_conversation_summary_block`
3. `build_external_memory_block`
4. `build_plan_mode_state_block`
5. `build_plan_runtime_block`

### 验收

- builder 输出语义和现有 system section 对齐
- builder 内容可独立测试

## Step 3：改 `execute_llm_stream(...)`，引入 envelope request path

### 目标

在真正发送给 provider 前，统一走 PromptEnvelope。

### Chat Completions

- `system(stable_instructions)`
- `stable_prefix_messages`
- `dynamic_context_messages`
- `conversation_messages`

### Responses

- `instructions = stable_instructions`
- `input = stable_prefix + dynamic + conversation`

### 验收

- 现有 provider tests 不退化
- request shape 正确
- tool calling 保持正常

## Step 4：停用每轮 system 注入的动态块

behind flag 切换：

```rust
config.prompt_architecture_v2: bool
```

当开关打开时，以下内容不再改写 system：

- external memory
- task list
- plan runtime
- plan mode 当前状态
- conversation summary

### 验收

- system 不再每轮漂移
- old path 仍可 fallback

## Step 5：将 summary / recovery 全量 block 化

### 目标

让 compression 真正服务于 cache 命中。

### 验收

- summary 不再作为 system message 注入
- recovery 信息仍能保留连续性
- prepared context 语义保持正确

## Step 6：加入 observability

记录：

- stable hash
- dynamic hash
- block types
- block chars
- envelope version

用于验证 cache-friendly 架构是否真正稳定。

---

## 15. 推荐函数签名草案

### 15.1 types

```rust
#[derive(Debug, Clone, Default)]
pub struct PromptEnvelope {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
    pub dynamic_context_messages: Vec<Message>,
    pub conversation_messages: Vec<Message>,
    pub observability: PromptEnvelopeObservability,
}
```

```rust
#[derive(Debug, Clone, Default)]
pub struct PromptEnvelopeObservability {
    pub stable_instructions_chars: usize,
    pub stable_prefix_message_count: usize,
    pub dynamic_context_message_count: usize,
    pub conversation_message_count: usize,
    pub stable_prefix_hash: Option<String>,
    pub dynamic_context_hash: Option<String>,
    pub included_block_types: Vec<ContextBlockType>,
}
```

```rust
#[derive(Debug, Clone)]
pub struct ContextBlock {
    pub block_type: ContextBlockType,
    pub priority: ContextBlockPriority,
    pub stability: ContextBlockStability,
    pub title: String,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
}
```

### 15.2 stable frame builder

```rust
pub struct StablePromptFrame {
    pub stable_instructions: String,
    pub stable_prefix_messages: Vec<Message>,
}
```

```rust
pub fn build_stable_prompt_frame(
    session: &Session,
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
    activated_discoverable_tools: &BTreeSet<String>,
) -> StablePromptFrame;
```

### 15.3 dynamic block builder

```rust
pub async fn build_dynamic_context_blocks(
    session: &Session,
    config: &AgentLoopConfig,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) -> Vec<ContextBlock>;
```

### 15.4 assemble / serialize

```rust
pub fn assemble_prompt_envelope(
    stable: StablePromptFrame,
    dynamic_blocks: Vec<ContextBlock>,
    conversation_messages: Vec<Message>,
) -> PromptEnvelope;
```

```rust
pub fn render_context_block_message(block: &ContextBlock) -> Message;
```

```rust
pub fn envelope_to_chat_messages(envelope: &PromptEnvelope) -> Vec<Message>;
```

```rust
pub struct ResponsesPromptView {
    pub instructions: Option<String>,
    pub input_messages: Vec<Message>,
}
```

```rust
pub fn envelope_to_responses_view(envelope: &PromptEnvelope) -> ResponsesPromptView;
```

### 15.5 specific block builders

```rust
pub fn build_task_snapshot_block(session: &Session) -> Option<ContextBlock>;
```

```rust
pub fn build_conversation_summary_block(
    summary: &ConversationSummary,
) -> Option<ContextBlock>;
```

```rust
pub async fn build_external_memory_block(
    session: &Session,
    prompt_memory_flags: PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) -> Option<ContextBlock>;
```

```rust
pub fn build_plan_mode_state_block(session: &Session) -> Option<ContextBlock>;
```

```rust
pub fn build_plan_runtime_block(
    session: &Session,
    app_data_dir: Option<&std::path::Path>,
) -> Option<ContextBlock>;
```

```rust
pub fn build_recovery_snapshot_block(
    compressed_messages: &[Message],
    session: &Session,
) -> Option<ContextBlock>;
```

---

## 16. 测试矩阵

## 16.1 PromptEnvelope 单元测试

### T1. stable frame 不包含 task / memory / summary

断言：

- `stable_instructions` 含 base / repo / tool policy
- 不含 external memory / task list / summary markers

### T2. dynamic block 分类正确

断言：

- task -> `TaskSnapshot`
- summary -> `ConversationSummary`
- plan state -> `PlanModeState`

### T3. context block message 模板稳定

断言：

- 有 start/end marker
- 有 “not a new user request”
- metadata 正确

## 16.2 provider serialization 测试

### T4. Chat Completions shape 正确

断言：

- 第一条为 stable system
- dynamic blocks 作为 user synthetic messages 出现
- conversation messages 在最后

### T5. Responses shape 正确

断言：

- `instructions == stable_instructions`
- input 中不重复包含 stable system
- dynamic blocks 出现在 input

## 16.3 compression 协同测试

### T6. summary block 不再进入 system

断言：

- summary 只出现在 dynamic block / synthetic message
- stable system 不含 summary marker

### T7. recovery block 仍可保连续性

断言：

- request 中 recovery 信息存在
- 文件路径 / active task / key decisions 仍可被保留

## 16.4 execute_llm_stream 集成测试

### T8. Responses 请求设置 instructions

mock provider 记录：

- request messages
- `options.responses.instructions`

断言：

- instructions 被正确设置
- request messages 不重复包含 stable system

### T9. Chat Completions 仍使用单 stable system

断言：

- 第一条是 stable system
- dynamic blocks 跟在后面

## 16.5 tool calling 回归测试

### T10. tool chain continuity 不退化

断言：

- tool_call_id 顺序不变
- tool results 顺序不变
- responses / completions 均能继续 tool calling

## 16.6 plan mode 回归测试

### T11. plan mode 安全约束仍生效

断言：

- stable instructions 仍包含 plan mode 规则
- dynamic block 提供当前状态
- agent 行为不退化

## 16.7 cache 观测测试

### T12. stable hash 在多轮内保持稳定

场景：

- task 变化
- summary 变化
- external memory 变化

断言：

- stable hash 不变
- dynamic hash 变化

---

## 17. Rollout 策略

## 17.1 behind flag

建议新增：

```rust
config.prompt_architecture_v2: bool
```

默认关闭。

## 17.2 开发期只在测试 / 本地环境开启

先验证：

- unit tests
- integration tests
- 本地 dev

## 17.3 双写观测

开 v2 时记录：

- old effective system length
- new stable instruction length
- dynamic block count
- stable hash
- dynamic hash

便于对比 old/new request 结构。

## 17.4 默认开启后保留 fallback

在 v2 稳定前，旧路径至少保留 1~2 个 release 周期。

---

## 18. 风险与边界控制

## 18.1 第一阶段不要同时改太多层

首批 PR 不建议同时做：

- PromptEnvelope 引入
- provider wire shape 改造
- compression summary format 重写

否则很难定位回归来源。

## 18.2 推荐 PR 切分

### PR 1

引入 `PromptEnvelope` / `ContextBlock` / renderer / 单测

### PR 2

改 `execute_llm_stream(...)`，behind flag 走新 envelope request path

### PR 3

迁出 `task + summary`

### PR 4

迁出 `external memory`

### PR 5

迁出 `plan mode + plan runtime`

### PR 6

把 recovery / summary 完整 block 化

---

## 19. 第一阶段必须落地的范围

### 必做

- PromptEnvelope 抽象
- stable instructions 固化
- task / summary 从 system 迁出
- Responses 使用 `instructions`
- Chat Completions 使用单 stable system

### 可延后

- dynamic block 的精细 budget competition
- PromptSnapshot schema 重构
- recovery block 的完全持久化重构
- env block 的细粒度建模
- envelope-aware compression

---

## 20. 建议优先修改的文件

按优先顺序：

1. `bamboo/crates/bamboo-engine/src/runtime/runner/session_setup/prompt_setup.rs`
2. `bamboo/crates/bamboo-engine/src/runtime/runner/round_prelude.rs`
3. `bamboo/crates/bamboo-engine/src/runtime/runner/round_lifecycle/stream_execution.rs`
4. `bamboo/crates/bamboo-engine/src/runtime/runner/prompt_context/external_memory.rs`
5. `bamboo/crates/bamboo-engine/src/runtime/runner/prompt_context/task.rs`
6. `bamboo/crates/bamboo-compression/src/compression_tooling.rs`
7. `bamboo/crates/bamboo-infrastructure/src/llm/providers/common/openai_responses.rs`
8. `bamboo/crates/bamboo-infrastructure/src/llm/providers/common/openai_compat.rs`

---

## 21. 推荐实施顺序

如果按最低风险推进，我建议是：

### 第 1 步

新增 `PromptEnvelope` / `ContextBlock` / renderer，不接业务逻辑

### 第 2 步

先迁 `task + summary`

原因：

- 这两者最动态
- 对 cache 收益最大
- 改动相对可控

### 第 3 步

改 `execute_llm_stream(...)`，让 Responses 开始走 `instructions`

### 第 4 步

迁 `external memory`

### 第 5 步

迁 `plan mode / plan runtime`

### 第 6 步

最后再统一 `recovery snapshot` 与 PromptSnapshot 优化

---

## 22. 结论

这次重构的本质不是“把 system prompt 改短”，而是：

> 为 Bamboo 建立一套稳定、分层、可缓存的 prompt 请求结构。

真正的关键变化有三点：

1. **稳定规则留在 stable instructions/system**
2. **动态上下文从 system 迁移到 synthetic context messages**
3. **Responses 路径显式使用 `instructions`，Chat Completions 路径保留单一稳定 system**

配合现有 compression 与 microcompact 机制，这套架构将使 Bamboo 的 prompt cache 命中率显著提升，同时保留当前 session / tool / compression 体系的兼容性。
