# Zenith vs Claude Code：Agent 上下文管理与记忆系统对比

## 结论摘要

- **Zenith / Bamboo** 的设计更像一个**显式分层、可观测、可操作的 prompt runtime**：它把 workspace / instruction / env / skill / tool guide / external memory / task list 都做成了系统 prompt 的独立 section，并且把这些 section 持久化为 `PromptSnapshot`。在“记忆”上，它不只有会话 note，还引入了 **跨会话 Dream Notebook** 与 **结构化 durable memory store**（session / project / global 三层作用域）。另外，Zenith 对**大体量工具输出**已经形成多层治理链路：工具执行后可按场景压缩、超大 tool message 入 session 时会立即截断、进入下一轮 loop 前还会再次补做 oversized tool message compaction。
- **Claude Code** 的设计更像一个**围绕 token/cache 优化过的生产级上下文管线**：它在真实发请求前，会经过 `compact boundary -> tool result budget -> snip -> microcompact -> context collapse -> autocompact` 这样一条比较成熟的压缩流水线，并且把 `CLAUDE.md` / rules / user memory / auto memory / team memory 统一注入到上下文中。
- 如果只看**上下文管理（context management）**，更准确的说法是：**Claude Code 在“整段会话上下文整形 / collapse / autocompact / cache 优化”上更成熟；Zenith 在“工具大结果的运行时过滤与分场景压缩”上已经做得相当积极**。因此若比较“整条 context pipeline”的成熟度，Claude Code 仍然更强；若比较“tool output ingress/egress compression”的工程治理，Zenith 并不弱，甚至在早期截断上更直接。
- 如果看**记忆系统（memory system）**，Zenith / Bamboo 更“体系化”：不仅有可写 session note，还有跨会话 Dream notebook，以及可查询/合并/清理的 durable memory 文档系统。
- 一句话：
  - **Claude Code 更像“高性能上下文压缩引擎 + 文件型记忆体系”**
  - **Zenith 更像“分层 prompt 操作系统 + 多层 durable memory 平台”**

---

## 1. 上下文管理：整体架构差异

### Zenith / Bamboo

Zenith 的上下文管理核心特点是：**把 prompt 的各类来源显式拆层，并把结果结构化保存**。

#### 1.1 Prompt 分层拼装

`prepare_session_for_loop()` 会在 session 进入 agent loop 前，统一准备：
- skill context
- tool guide context
- system prompt contexts
- task context
- oversized tool message compaction

核心入口：
- `bamboo/src/agent/loop_module/runner/session_setup.rs:33`
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:264`

`apply_system_prompt_contexts()` 会把这些 section 拼起来：
- `base_prompt`
- `workspace_context`
- `instruction_context`
- `env_context`
- `skill_context`
- `tool_guide_context`

并把结果写回 system message，同时落地 `PromptSnapshot`：
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:265`
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:351`
- `bamboo/src/agent/core/agent/types.rs:575`

#### 1.2 PromptSnapshot 是很强的工程点

Zenith 不只是“拼 prompt”，而是把 prompt 的 major sections 结构化保存下来：
- `base_system_prompt`
- `workspace_context`
- `instruction_context`
- `env_context`
- `skill_context`
- `tool_guide_context`
- `dream_notebook`
- `session_memory_note`
- `task_list`
- `effective_system_prompt`

定义见：
- `bamboo/src/agent/core/agent/types.rs:575`

这意味着 Zenith 在**可调试性 / 可解释性 / UI 可视化 / prompt diff** 上有天然优势。

#### 1.3 Workspace / instruction / env 注入非常显式

- workspace context：`bamboo/src/server/app_state/mod.rs:165`
- env context：`bamboo/src/server/app_state/mod.rs:158`
- instruction layer（收集 AGENTS.md / CLAUDE.md）：`bamboo/src/server/instruction_layer.rs:56`

尤其 instruction layer 会沿 workspace 向上收集祖先目录中的 `AGENTS.md` / `CLAUDE.md`：
- `bamboo/src/server/instruction_layer.rs:56`
- `bamboo/src/server/instruction_layer.rs:76`

这个设计比很多 agent runtime 更“工程化”：**repo policy 不是隐式读文件，而是显式编译进 prompt section**。

#### 1.4 Task list 也是 prompt 的一级公民

Zenith 把任务列表直接注入系统 prompt：
- `bamboo/src/agent/loop_module/task_context/prompt.rs:20`
- `bamboo/src/agent/loop_module/runner/prompt_context.rs:45`

这和 Bamboo 的开发者规范是一致的：Task 是共享状态的一部分，不只是 UI。

---

### Claude Code

Claude Code 的上下文管理核心特点是：**所有上下文都服务于“真实发给模型的消息集合”最优化**。

#### 1.5 发请求前会走一条成熟的 context pipeline

在主查询链路里，Claude Code 会先：
1. 从 compact boundary 之后取消息
2. 做 tool result budget
3. 做 snip
4. 做 microcompact
5. 做 context collapse
6. 再做 autocompact

关键入口：
- `claude-code/src/query.ts:365`
- `claude-code/src/query.ts:379`
- `claude-code/src/query.ts:401`
- `claude-code/src/query.ts:414`
- `claude-code/src/query.ts:440`
- `claude-code/src/query.ts:454`

这个流水线比 Zenith 当前可见实现更“重 context budget engineering”。

#### 1.6 /context 命令展示的是“模型实际看到的上下文”

Claude Code 的 `/context` 不看原始 REPL 历史，而是显式复用查询前变换：
- 先 `getMessagesAfterCompactBoundary`
- 再 `projectView`
- 再 `microcompactMessages`

见：
- `claude-code/src/commands/context/context.tsx:18`
- `claude-code/src/commands/context/context.tsx:39`
- `claude-code/src/commands/context/context-noninteractive.ts:49`
- `claude-code/src/commands/context/context-noninteractive.ts:58`

这说明它的上下文模型非常清晰：**“UI 显示的上下文” ≈ “API 实际收到的上下文”**。

#### 1.7 System prompt section cache

Claude Code 也有 system prompt section 的缓存机制：
- `claude-code/src/constants/systemPromptSections.ts:16`
- `claude-code/src/constants/systemPromptSections.ts:43`

这和 Zenith 的 `PromptSnapshot` 不完全一样：
- Zenith 更偏**可观察、可拆解、可落盘**
- Claude Code 更偏**prompt cache 稳定性与运行时性能**

#### 1.8 系统/用户上下文分离

Claude Code 把上下文拆成：
- `getSystemContext()`：git status、cache breaker 等系统态信息
- `getUserContext()`：CLAUDE.md / memory files / currentDate 等用户态上下文

见：
- `claude-code/src/context.ts:116`
- `claude-code/src/context.ts:155`

这个拆分很实用，但它不像 Zenith 那样把每个 section 的结果保存成一个结构化快照对象。

---

## 2. 记忆系统：整体架构差异

### Zenith / Bamboo

Zenith 的记忆体系是**三层并存**：
1. **Session Memory Note**：会话级、可写、topic 化
2. **Dream Notebook**：跨会话、只读、后台 consolidate
3. **Durable Memory Store**：结构化 memory 文档库，支持 query / merge / purge / inspect

#### 2.1 Session note：工具驱动、topic-aware

`session_note` 工具是 Zenith 当前最直接的“会话记忆”入口：
- `bamboo/src/agent/tools/tools/memory_note.rs:1`
- `bamboo/src/agent/tools/tools/memory_note.rs:22`
- `bamboo/src/agent/tools/tools/memory_note.rs:101`

特点：
- append / replace / read / clear / list_topics
- 12k char 上限
- 支持 topic，把不同 workstream 分开
- 有并发锁，避免 session note 并发写坏

这是**可控的 agent 自写记忆**，而不是纯后台自动抽取。

#### 2.2 Session note 会被直接注入 system prompt

每轮都会把 session note 读出来，按 topic 截断后注入 external memory block：
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:39`
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:86`
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:184`

而且会在 context pressure 高时提醒模型“先把重要状态写进 session_note”：
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:228`

这点非常重要：Zenith 不是被动压缩，而是尝试让 agent **跨 compression boundary 主动保存 state**。

#### 2.3 Dream Notebook：跨会话 consolidated memory

Zenith 有一个非常独特的设计：**Dream Notebook**。

- 后台任务 `auto_dream` 定期扫描近期 root sessions
- 读取它们的 `conversation_summary` 或 outline
- 让后台模型综合生成一个 markdown notebook
- 持久化到全局 memory view

见：
- `bamboo/src/server/services/auto_dream.rs:17`
- `bamboo/src/server/services/auto_dream.rs:57`
- `bamboo/src/server/services/auto_dream.rs:175`
- `bamboo/src/server/services/auto_dream.rs:242`

Dream notebook 会作为 **read-only cross-session memory** 注入 prompt：
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:61`
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:168`

这是 Claude Code 当前可见实现里没有的：Claude Code 有 auto memory / team memory，但没有一个“后台跨多个 session 综合压缩出的全局 notebook”机制。

#### 2.4 Durable Memory Store：结构化 memory 平台

Zenith 的 `MemoryStore` 远不止“保存一个 markdown 文件”。

它定义了：
- scope: `session / project / global`
- type: `user / feedback / project / reference`
- status: `active / stale / superseded / contradicted / archived`
- retrieval metadata: keywords / entities / embedding_ready / last_accessed_at
- relations: supersedes / contradicted_by / related

定义见：
- `bamboo/src/agent/core/memory_store/types.rs:6`
- `bamboo/src/agent/core/memory_store/types.rs:24`
- `bamboo/src/agent/core/memory_store/types.rs:44`
- `bamboo/src/agent/core/memory_store/types.rs:105`

`MemoryStore` 支持：
- topic note 读写
- dream view 读写
- scope query
- 文档 list/query/merge/purge/inspect/rebuild

入口见：
- `bamboo/src/agent/core/memory_store/store.rs:42`
- `bamboo/src/agent/core/memory_store/store.rs:70`
- `bamboo/src/agent/core/memory_store/store.rs:181`
- `bamboo/src/agent/core/memory_store/store.rs:216`

对应 tool 暴露为 `memory`：
- `bamboo/src/server/tools/memory.rs:17`
- `bamboo/src/server/tools/memory.rs:158`

所以 Zenith 的 memory 不是“几个 markdown instruction files”，而是**已经开始平台化**。

---

### Claude Code

Claude Code 的记忆体系是**文件型主导 + 自动抽取补充**。

大致分成四类：
1. `CLAUDE.md` / `.claude/CLAUDE.md` / `.claude/rules/*.md`
2. 用户级 memory（如 `~/.claude/CLAUDE.md`）
3. 自动抽取的 auto memory
4. 可选的 team memory

#### 2.5 CLAUDE.md / rules 是记忆与指令的统一入口

`claudemd.ts` 顶部就写了加载顺序：
1. managed memory
2. user memory
3. project memory
4. local memory

见：
- `claude-code/src/utils/claudemd.ts:2`
- `claude-code/src/utils/claudemd.ts:4`
- `claude-code/src/utils/claudemd.ts:6`

并且会拼成一段统一指令块：
- `claude-code/src/utils/claudemd.ts:89`
- `claude-code/src/utils/claudemd.ts:1153`
- `claude-code/src/utils/claudemd.ts:1194`

`getClaudeMds()` 本质上是把各种 memory files 汇总成一段 prompt text：
- `claude-code/src/utils/claudemd.ts:1153`

这与 Zenith 最大的差别是：**Claude Code 的 memory 很大一部分仍然是“文件即记忆，拼进去就是上下文”**。

#### 2.6 路径感知规则很强

Claude Code 的 `.claude/rules/*.md` 支持 frontmatter `paths`，按目标路径筛选：
- `claude-code/src/utils/claudemd.ts:1249`
- `claude-code/src/utils/claudemd.ts:1354`
- `claude-code/src/utils/claudemd.ts:1369`

这比 Zenith 现在的 instruction layer 更细粒度：
- Zenith 更像 repo-level / ancestor-level policy aggregation
- Claude Code 更像 **path-conditioned instruction routing**

#### 2.7 Session Memory：后台维护一份当前会话摘要文件

Claude Code 有 `SessionMemory`：
- `claude-code/src/services/SessionMemory/sessionMemory.ts:1`
- `claude-code/src/services/SessionMemory/sessionMemoryUtils.ts:1`

它会在后台、按阈值触发：
- 初始阈值：`minimumMessageTokensToInit`
- 增量阈值：`minimumTokensBetweenUpdate`
- 工具调用阈值：`toolCallsBetweenUpdates`

默认值见：
- `claude-code/src/services/SessionMemory/sessionMemoryUtils.ts:31`

更新方式：
- 不是主 agent 直接写，而是**fork 一个子 agent** 去更新会话 memory 文件
- 只允许它 edit 这一个 memory 文件

见：
- `claude-code/src/services/SessionMemory/sessionMemory.ts:272`
- `claude-code/src/services/SessionMemory/sessionMemory.ts:315`
- `claude-code/src/services/SessionMemory/sessionMemory.ts:357`
- `claude-code/src/services/SessionMemory/sessionMemory.ts:460`

这个设计和 Zenith 的 `session_note` 最大区别：
- **Zenith：前台 agent 自主写 note，强显式**
- **Claude Code：后台子 agent 自动维护 summary file，弱显式但更自动**

#### 2.8 Extract Memories：durable memory 自动抽取

Claude Code 还有一套 `extractMemories`：
- `claude-code/src/services/extractMemories/extractMemories.ts:1`
- `claude-code/src/services/extractMemories/prompts.ts:1`

它在完整 query loop 结束后运行，fork 一个 memory extraction subagent，把最近消息抽取成 durable memory 文件，写入 auto-memory 目录：
- `claude-code/src/services/extractMemories/extractMemories.ts:415`
- `claude-code/src/services/extractMemories/extractMemories.ts:437`
- `claude-code/src/services/extractMemories/extractMemories.ts:472`
- `claude-code/src/services/extractMemories/extractMemories.ts:598`

它支持：
- auto memory
- team memory
- MEMORY.md 索引
- memory 文件 taxonomy 与 frontmatter

所以 Claude Code **并不是只有 CLAUDE.md 文件记忆**，它还有自动 durable memory 抽取；只是整体形态仍然偏“文件与索引驱动”，不像 Zenith 的 `MemoryStore` 那么强 schema / query / lifecycle 化。

---

## 3. 压缩与摘要：谁更强？

### Claude Code 更强

这是两者差异最大的地方。

Claude Code 的上下文压缩是一个完整产品级流水线：
- `microcompactMessages()`：清理历史大 tool results，偏 cache-editing / token优化
  - `claude-code/src/services/compact/microCompact.ts:253`
- `sessionMemoryCompact`：优先用 session memory 做压缩
  - `claude-code/src/commands/compact/compact.ts:55`
  - `claude-code/src/services/compact/sessionMemoryCompact.ts:1`
- `context collapse`：把多段旧消息折叠成摘要视图
  - `claude-code/src/commands/context/context.tsx:18`
  - `claude-code/src/query.ts:428`
- `autocompact`：真正超预算时自动压缩
  - `claude-code/src/query.ts:454`

尤其 `query.ts` 中有非常明确的排序与策略说明：
- 先 microcompact
- 再 collapse
- 再 autocompact
- collapse 若足够就避免单一 summary 粗暴替代

见：
- `claude-code/src/query.ts:412`
- `claude-code/src/query.ts:428`
- `claude-code/src/query.ts:453`

这是一套非常成熟的 context budget engineering。

### Zenith / Bamboo 已经有多层压缩与结果治理，但“整段会话级 context pipeline”仍略弱于 Claude Code

Zenith 不只是有 budget compression 骨架，它实际上已经有至少三层和“大结果治理”相关的机制：
- `ConversationSummary`
- `CompressionEvent`
- message `compressed` / `compressed_by_event_id`
- budget compression tooling
- **post-execution output compressor**（按工具场景压缩 Bash / Read / Grep / WebFetch 结果）
- **超大 tool message 运行时截断**（tool result 入 session 即截断，loop 前再补扫）

定义见：
- `bamboo/src/agent/core/agent/types.rs:530`
- `bamboo/src/agent/core/agent/types.rs:696`

### 第一层：tool execution 后按场景压缩输出

在 tool execution 阶段，Zenith 会在结果写入 session 之前先走 `output_compressor::maybe_compress()`：
- `bamboo/src/agent/loop_module/runner/tool_execution.rs:131`
- `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/mod.rs:225`

它不是只压 Bash，而是会识别场景：
- Bash / BashOutput
- Read
- Grep
- WebFetch

见：
- `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/mod.rs:18`
- `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/mod.rs:41`

具体例子：
- Bash generic：去 ANSI、折叠空行、限制 stdout 200 行 / stderr 80 行、再做 byte cap
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/bash_generic.rs:8`
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/bash_generic.rs:45`
- Read：大文件读取会折叠长注释块、空行，并 cap 到 400 行
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/read_code.rs:14`
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/read_code.rs:46`
- Grep：按文件限制匹配条数，并 cap 到 200 行
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/grep_results.rs:13`
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/grep_results.rs:40`
- WebFetch：去噪、折叠导航/短菜单行，并 cap 到 300 行
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/web_fetch.rs:14`
  - `bamboo/src/agent/loop_module/runner/tool_execution/output_compressor/scenarios/web_fetch.rs:59`

### 第二层：超大 tool result 写入 session 时立即截断

Zenith 在 `Session::add_message()` 中，如果消息角色是 `Tool`，会立刻调用 `truncate_tool_message_content()`：
- `bamboo/src/agent/core/agent/types.rs:809`
- `bamboo/src/agent/core/agent/types.rs:811`

截断阈值是明确的：
- `MAX_TOOL_MESSAGE_BYTES = 256 * 1024`
- 保留 head 160KB + tail 64KB
- 中间插入 truncation marker 和 omitted byte 信息

见：
- `bamboo/src/agent/core/agent/types.rs:40`
- `bamboo/src/agent/core/agent/types.rs:41`
- `bamboo/src/agent/core/agent/types.rs:42`
- `bamboo/src/agent/core/agent/types.rs:43`
- `bamboo/src/agent/core/agent/types.rs:1056`

### 第三层：session 进入下一轮 loop 前，再补做历史 oversized tool message compaction

在 `prepare_session_for_loop()` 中，还会再次调用：
- `bamboo/src/agent/loop_module/runner/session_setup.rs:115`
- `bamboo/src/agent/loop_module/runner/session_setup/compaction.rs:4`

这说明 Zenith 对大 tool output 的治理不是一次性的，而是“写入时 + 下一轮前”双保险。

### 第四层：若整体上下文仍然超预算，再走 budget compression + summary

真正的会话级压缩落点在：
- `bamboo/src/agent/core/budget/compression_tooling.rs:368`
- `bamboo/src/agent/core/budget/compression_tooling.rs:376`
- `bamboo/src/agent/core/budget/compression_tooling.rs:387`
- `bamboo/src/agent/core/budget/compression_tooling.rs:388`

这里它会：
- 标记历史消息 `compressed = true`
- 写入 `compressed_by_event_id`
- push `CompressionEvent`
- 写 `conversation_summary`

而 budget preparation 也会把 summary 作为混合上下文的一部分：
- `bamboo/src/agent/core/budget/preparation.rs:961`
- `bamboo/src/agent/core/budget/preparation.rs:985`

### 与 Claude Code 的真正差异

因此，更准确的差异不是“Zenith 没有成熟压缩”，而是：
- **Zenith 更强在 tool-output 级 runtime compression / filtering / truncation**
- **Claude Code 更强在整段会话级的 context collapse / autocompact / cache-aware pipeline**

也就是：
- Claude Code：**压缩是主查询链路的核心 runtime 主线能力**
- Zenith：**压缩已经覆盖 tool output 与 budget 两层，但整段会话级 context pipeline 仍不如 Claude Code 产品化**

---

## 4. 观测性与可调试性：谁更强？

### Zenith 更强

Zenith 在“让开发者看到 prompt 到底由什么构成”这方面非常突出：
- `PromptSnapshot` 直接保存 major sections
- `PromptAssemblyReport` 记录 section layout / lengths / flags
- task list / external memory / skill context 都是显式 section

关键位置：
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:95`
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:130`
- `bamboo/src/agent/loop_module/runner/session_setup/prompt_setup.rs:341`
- `bamboo/src/agent/core/agent/types.rs:575`

Claude Code 当然也很强，但更多是 runtime 与 analytics 层面的观测，比如 `/context`、section cache、usage breakdown；它没有 Zenith 这种“把 prompt section snapshot 当作会话一等数据结构”的味道。

所以如果你的目标是：
- debug prompt 注入
- 看每层上下文到底来自哪里
- UI 上清楚展示 prompt 分段

**Zenith 的架构更优秀。**

---

## 5. 自动化 vs 显式控制：哲学差异

### Zenith：更偏“显式控制 + agent 自管理状态”

典型例子：
- `session_note` 由 agent 主动调用
- context pressure 时明确提醒 agent 去存关键状态
- external memory block 直接教 agent 如何读写记忆

证据：
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:141`
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:151`
- `bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:228`

这更适合：
- agentic workflow
- 长任务执行
- 需要 agent 明确知道“我应该把什么存下来”的场景

### Claude Code：更偏“后台自动维护 + 生产优化”

典型例子：
- session memory 是 post-sampling hook 自动更新
- extract memories 是 stop hook 自动 fork 子 agent 跑
- 用户通过 `/memory` 更多是在编辑 memory files，而不是操作一个结构化 memory API

证据：
- `claude-code/src/services/SessionMemory/sessionMemory.ts:373`
- `claude-code/src/services/extractMemories/extractMemories.ts:593`
- `claude-code/src/commands/memory/memory.tsx:21`

这更适合：
- 默认行为要“开箱即用”
- 尽量不让主 agent 显式分心维护记忆
- 更强的产品化体验

---

## 6. 哪个系统更先进？分维度看

### 6.1 仅看上下文管理

**Claude Code 更先进。**

原因：
- pre-API context transform 更成熟
- compaction / collapse / microcompact / autocompact 链条完整
- 更关注 prompt cache 命中与 token 工程

代表文件：
- `claude-code/src/query.ts:365`
- `claude-code/src/services/compact/microCompact.ts:253`
- `claude-code/src/commands/compact/compact.ts:96`

### 6.2 仅看记忆系统

**Zenith / Bamboo 更有平台潜力。**

原因：
- session / project / global 三层 scope
- dream notebook 跨会话 consolidate
- durable memory 有 type / status / relations / retrieval 元数据
- query / merge / purge / inspect / rebuild 这些操作都已具备

代表文件：
- `bamboo/src/agent/core/memory_store/types.rs:6`
- `bamboo/src/agent/core/memory_store/store.rs:216`
- `bamboo/src/server/services/auto_dream.rs:175`
- `bamboo/src/server/tools/memory.rs:158`

### 6.3 看“可解释性 / 开发者可控性”

**Zenith 更强。**

### 6.4 看“产品级稳定性 / 上下文优化成熟度”

**Claude Code 更强。**

---

## 7. 如果你要让 Zenith 追上或超越 Claude Code，最值得补的点

### 高优先级建议

1. **把 budget compression 提升为显式 runtime pipeline**
   - 让 Zenith 也具备：轻量清理 → collapse → autocompact 的多阶段策略
   - 参考 Claude Code `query.ts` 的顺序控制

2. **把 context collapse 做成“投影视图”而不是只靠 summary 替换**
   - Claude Code 的强点在于不是一刀切 summarization，而是可回放的 collapsed view

3. **补 path-conditioned instruction rules**
   - 类似 Claude Code `.claude/rules/*.md + frontmatter.paths`
   - Zenith 当前 instruction layer 是目录祖先聚合，但不够细粒度

4. **让 durable memory 更自然进入 prompt selection**
   - 现在 Zenith 的 durable memory store 已很强，但主 prompt 注入层主要还是 dream + session note
   - 下一步应该做：基于 query/project/task 自动检索 durable memory 并按 relevance 注入

5. **把 PromptSnapshot 与 compression event 结合到 UI**
   - 这会成为 Zenith 区别于 Claude Code 的非常强的可视化优势

### 中优先级建议

6. **保留 Claude Code 的“后台自动抽取”优点**
   - Zenith 已有 auto_dream，但还可以加 project/global durable memory auto extraction

7. **把 session_note 与 durable memory 建立升级路径**
   - 例如：session note 中稳定条目自动建议转为 durable memory

---

## 8. 最终判断

如果你问：

### “谁的 agent 上下文管理更强？”
**Claude Code 更强。**
因为它把 token budget、cache、history shrink、collapse、autocompact 做成了一条生产级主路径。

### “谁的记忆系统更完整、更有长期潜力？”
**Zenith / Bamboo 更强。**
因为它已经从“文件型记忆”走向“结构化、分 scope、可查询、可合并、可归档的 durable memory 平台”，再加上 Dream notebook，长期上限更高。

### “谁更适合做 agent runtime 基础设施？”
**Zenith。**
因为它的 prompt section、task、external memory、instruction layer、durable memory 已经有明显的平台化方向。

### “谁现在更像经过大量真实用户打磨的产品？”
**Claude Code。**
因为它在上下文压缩、缓存稳定性、自动记忆维护这几个“脏活累活”上明显更成熟。

---

## 最短版总结

- **Claude Code 赢在：上下文压缩与运行时工程化。**
- **Zenith 赢在：prompt 分层建模与记忆系统的平台化。**
- **短期产品力看 Claude Code，长期架构潜力看 Zenith。**

---

## 9. Claude Code 的 context collapse / autocompact 与 Zenith 的 mid-turn / budget compression：算法链路级对比

这一节只看“上下文压缩主链路”，不再讨论 memory store。

### 9.1 触发条件：两者的设计中心完全不同

#### Claude Code：以主查询链路为中心的 pre-API 触发

Claude Code 的主查询链路会在真正发请求前依次尝试：
- tool-result budget / snip / microcompact
- context collapse
- autocompact

关键位置：
- `claude-code/src/query.ts:365`
- `claude-code/src/query.ts:412`
- `claude-code/src/query.ts:428`
- `claude-code/src/query.ts:453`

其中 autocompact 的阈值模型是：
- 先计算 `effective context window`
- 减去输出保留与 buffer
- 当 token count 超过 `autoCompactThreshold` 时触发

见：
- `claude-code/src/services/compact/autoCompact.ts:33`
- `claude-code/src/services/compact/autoCompact.ts:72`
- `claude-code/src/services/compact/autoCompact.ts:160`
- `claude-code/src/services/compact/autoCompact.ts:225`

同时它还会显式**禁止某些场景触发 autocompact**：
- `session_memory`
- `compact`
- `marble_origami`（collapse 自己）
- 开启 context collapse 时，主动 suppress autocompact

见：
- `claude-code/src/services/compact/autoCompact.ts:169`
- `claude-code/src/services/compact/autoCompact.ts:201`
- `claude-code/src/services/compact/autoCompact.ts:215`

这说明 Claude Code 的核心思想是：
> **如果 collapse 已经接管上下文管理，就不要让 autocompact 抢跑。**

#### Zenith：以预算暴露率为中心的 host-side 压缩

Zenith 的 host compression 触发更直接：
- 先估算当前 active context 的 exposure
- 当 usage 达到 `compression_trigger_percent` 时触发 host compression
- 或者 usage 达到 `FORCE_CONTEXT_COMPRESSION_PERCENT = 98%` 时强制 fallback

关键位置：
- `bamboo/src/agent/loop_module/runner/round_lifecycle/context_preparation.rs:22`
- `bamboo/src/agent/loop_module/runner/round_lifecycle/context_preparation.rs:79`
- `bamboo/src/agent/loop_module/runner/round_lifecycle/context_preparation.rs:81`
- `bamboo/src/agent/loop_module/runner/round_lifecycle/context_preparation.rs:83`

预算参数定义见：
- `bamboo/src/agent/core/budget/types.rs:12`
- `bamboo/src/agent/core/budget/types.rs:15`
- `bamboo/src/agent/core/budget/types.rs:43`
- `bamboo/src/agent/core/budget/types.rs:49`

默认值：
- trigger = 85%
- target = 40%

也就是说，Zenith 的核心思想是：
> **只要上下文暴露率触线，就在 host 侧生成 summary 并归档旧消息。**

### 9.2 状态模型：Claude Code 是 commit-log collapse，Zenith 是 archived-message summary

#### Claude Code：collapse 是“可恢复的投影视图状态机”

Claude Code 的 context collapse 并不只是往消息数组里塞一个 summary，而是把 collapse 结果当成一种**可恢复状态**来存：

- 每次 collapse commit 都记录一条 transcript entry
- 同时记录 staged queue + spawn state snapshot
- resume 时通过 `restoreFromEntries()` 重建 collapse store

持久化位置：
- `claude-code/src/utils/sessionStorage.ts:1541`
- `claude-code/src/utils/sessionStorage.ts:1563`

恢复位置：
- `claude-code/src/utils/sessionRestore.ts:121`
- `claude-code/src/utils/sessionRestore.ts:131`

这意味着 Claude Code 的 collapse 更像：
- **commit log**
- **snapshot**
- **projectView replay**

而不是一次性重写 transcript。

#### Zenith：compression 是“消息归档 + summary 注入 + event 记录”

Zenith 的状态模型是：
- 在 `Session.messages` 中把旧消息标成 `compressed = true`
- 给这些消息打上 `compressed_by_event_id`
- 写入 `CompressionEvent`
- 写入 `conversation_summary`
- 准备上下文时，把 summary 作为一条 system summary message 注入

关键位置：
- `bamboo/src/agent/core/budget/compression_tooling.rs:376`
- `bamboo/src/agent/core/budget/compression_tooling.rs:385`
- `bamboo/src/agent/core/budget/compression_tooling.rs:387`
- `bamboo/src/agent/core/budget/compression_tooling.rs:388`
- `bamboo/src/agent/core/budget/preparation.rs:58`
- `bamboo/src/agent/core/budget/preparation.rs:159`

这个模型更像：
- **archived messages on session**
- **single current summary**
- **UI-visible compression events**

所以两者的本质差别是：
- **Claude Code：collapse store + replay projection**
- **Zenith：session-native archival + summary injection**

### 9.3 压缩粒度：Claude Code 面向“API-round / conversation span”，Zenith 面向“message segments + tool chains”

#### Claude Code 的粒度

Claude Code 在 compact 里会按 API round / compact boundary 工作；当发生 PTL retry 时，还会按 `groupMessagesByApiRound()` 从最老组开始剥离：
- `claude-code/src/services/compact/compact.ts:243`
- `claude-code/src/services/compact/compact.ts:257`

partial compact 时，也显式区分：
- `from`：总结 pivot 之后，保留之前
- `up_to`：总结 pivot 之前，保留之后

见：
- `claude-code/src/services/compact/compact.ts:765`
- `claude-code/src/services/compact/compact.ts:772`

这说明 Claude Code 的压缩粒度偏：
- **conversation spans / api rounds / selected windows**

#### Zenith 的粒度

Zenith 的 `prepare_hybrid_context()` 先把消息切成 `MessageSegment`，然后做 budget selection：
- `bamboo/src/agent/core/budget/preparation.rs:57`
- `bamboo/src/agent/core/budget/preparation.rs:83`
- `bamboo/src/agent/core/budget/preparation.rs:137`

它的保留/丢弃原则很明确：
- 保持 tool-chain 原子性
- 优先保留第一个 user、最后一个 user、最后一个 assistant textual outcome
- 先删最老的 tool chains
- 再删非工具段
- 最后才动 protected anchors

见：
- `bamboo/src/agent/core/budget/preparation.rs:238`
- `bamboo/src/agent/core/budget/preparation.rs:251`
- `bamboo/src/agent/core/budget/preparation.rs:268`
- `bamboo/src/agent/core/budget/preparation.rs:282`
- `bamboo/src/agent/core/budget/preparation.rs:294`

所以 Zenith 的粒度偏：
- **message segments**
- **tool chains**
- **protected anchor messages**

### 9.4 cache 关系：Claude Code 是显式 cache-aware，Zenith 是 cache-friendly 但较弱耦合

#### Claude Code

Claude Code 的 collapse/autocompact 与 prompt cache 是强耦合设计：
- collapse 开启时 suppress autocompact，避免彼此竞争
- compaction 后统一做 cache cleanup / resetContextCollapse / clear memoized contexts
- autocompact 里还会显式发 `notifyCompaction()`

关键位置：
- `claude-code/src/services/compact/autoCompact.ts:201`
- `claude-code/src/services/compact/autoCompact.ts:302`
- `claude-code/src/services/compact/postCompactCleanup.ts:31`
- `claude-code/src/services/compact/postCompactCleanup.ts:42`
- `claude-code/src/services/compact/postCompactCleanup.ts:59`

这是典型的：
> **压缩策略必须考虑 cache 是否被打断。**

#### Zenith

Zenith 也有 prompt-side tool output cache compaction，但它更像预算系统中的一个辅助优化，而不是整个 runtime 的中心：
- 在 `prepare_hybrid_context()` 里，先尝试把旧的长 tool outputs 替换成 cached summary
- 保护最近若干 user turns 与 recent tool chains
- 只有超过 trigger limit 才做

关键位置：
- `bamboo/src/agent/core/budget/preparation.rs:73`
- `bamboo/src/agent/core/budget/preparation.rs:346`
- `bamboo/src/agent/core/budget/preparation.rs:382`
- `bamboo/src/agent/core/budget/preparation.rs:414`
- `bamboo/src/agent/core/budget/preparation.rs:527`
- `bamboo/src/agent/core/budget/preparation.rs:564`
- `bamboo/src/agent/core/budget/preparation.rs:593`

也就是说，Zenith 的 cache-aware 压缩是：
- **存在，而且设计得不差**
- 但不像 Claude Code 那样成为整条 runtime pipeline 的统领逻辑

### 9.5 恢复机制：Claude Code 更强，Zenith 更简单直接

#### Claude Code

- collapse commit 持久化到 transcript
- snapshot 持久化到 transcript
- resume 时恢复 collapse store
- query 时 `projectView()` 重建 collapsed view

这是个标准的：
- **event log + snapshot restore** 模型

#### Zenith

- 压缩结果直接写回 `Session`
- `compressed` 标记保留在消息上
- `conversation_summary` 保留在 session 上
- `CompressionEvent` 保留在 session 上
- 下次 prepare context 时直接利用这些 session 内状态

这是个标准的：
- **session object authoritative state** 模型

### 9.6 最后的算法级判断

#### Claude Code 的优势

Claude Code 更像一个：
- **上下文操作系统的主查询调度器**
- 关注 query 前的整体会话整形
- 关注 collapse 与 autocompact 的互斥、衔接和 cache 影响
- 关注 resume 后能重建 collapse 视图

#### Zenith 的优势

Zenith 更像一个：
- **session-native budget manager + tool-output runtime compressor**
- 关注消息进入 session 后如何被归档、摘要、复用
- 关注 tool chains / anchors / budget fitting
- 关注把压缩结果变成 session 的一部分

#### 用一句话概括

- **Claude Code：更像“可恢复的 collapsed view state machine + pre-API compaction orchestrator”**
- **Zenith：更像“session-native archival summarizer + segment-aware budget reducer”**

#### 如果你要把 Zenith 往 Claude Code 的方向补齐，最关键的是

1. 给 host compression 增加 **collapse-store / replay projection** 思想，而不只是 `conversation_summary`
2. 明确区分：
   - tool-output compression
   - prompt-cache compaction
   - conversation-span collapse
   - hard-limit autocompact
3. 让这些策略之间形成可观测的优先级与互斥关系，而不是主要依赖 budget trigger

