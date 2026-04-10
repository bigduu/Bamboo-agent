# Claude Code vs Bamboo Memory Robustness Check

日期：2026-04-09

## TL;DR

结论先说：**你的感觉是对的，Claude Code 当前的记忆系统在“主对话可消费性、索引入口、一致性、抗陈旧信息能力”上，确实比 Bamboo 更健壮。**

Bamboo 现在不是“没有 memory 基础设施”，而是 **基础设施很多，但真正进入主会话推理链路的部分太少**：

- `session_note`：会话级，可写，进 prompt
- `Dream Notebook`：跨 session，只读，进 prompt
- durable `memory` store：project/global/session 三层作用域，支持 query/get/write/merge/purge/inspect/rebuild
- `MEMORY.md` / `RECENT.md` / `STALE.md` / `lexical.json` / `graph.json`：会生成在磁盘上

但问题在于：

1. **Bamboo 的 cross-session 注入并不是 canonical memory index，而是一个后台定时生成的 Dream notebook 片段**。
2. **这个 Dream notebook 是全局的，不按当前项目过滤。**
3. **Dream notebook 的生成逻辑是“按上次 consolidate 之后的新 session 重新总结并覆盖整份文件”，不是像 Claude Code 的 `MEMORY.md` 那样作为稳定索引持续累积。**
4. **Bamboo 虽然已经会生成 `MEMORY.md` 和多个索引 JSON，但主对话默认并不会像 Claude Code 一样自动消费这些索引。**
5. **durable memory 的召回依赖模型主动调用 `memory query/get`，不是系统级主动 recall。**

这几个点叠加起来，就会产生你描述的体验：

- cross-session 区块看起来像“上一轮 session 的内容”
- unrelated project / unrelated workspace 的内容会混进来
- 已经写到 durable memory / MEMORY.md 的内容，不一定真的在主对话里被看见

---

## 1. Claude Code 为什么更稳

### 1.1 `MEMORY.md` 是明确的主入口索引

Claude Code 的 memory prompt 明确要求 memory 是两步写入：

1. 每条 memory 写到自己的 markdown 文件
2. 在 `MEMORY.md` 中增加一个一行索引入口

相关位置：
- `claude-code/src/memdir/memdir.ts:219`
- `claude-code/src/memdir/memdir.ts:227`
- `claude-code/src/memdir/memdir.ts:229`

而且它对 `MEMORY.md` 有明确的截断策略：
- 行数上限：200 行
- 字节上限：25KB
- 超限会追加 warning，提醒把细节放进 topic file

相关位置：
- `claude-code/src/memdir/memdir.ts:34`
- `claude-code/src/memdir/memdir.ts:57`
- `claude-code/src/memdir/memdir.ts:95`

这意味着 Claude Code 的 memory 入口非常清楚：

- `MEMORY.md` = 稳定索引
- 各 memory file = 详细内容

### 1.2 `MEMORY.md` / memory files 真的会进入主上下文

Claude Code 的 system prompt 会加载 memory prompt：
- `claude-code/src/constants/prompts.ts:495`

而 user context 会把 memory files 经过 `getMemoryFiles()` / `getClaudeMds()` 汇总后注入：
- `claude-code/src/context.ts:155`
- `claude-code/src/context.ts:170`
- `claude-code/src/context.ts:172`

也就是说，**Claude Code 的 memory 不是“存在磁盘上但要模型自己想起来去查”，而是天然在主对话上下文链路里有入口。**

### 1.3 Claude Code 有“按当前 query 选相关 memory”的主动召回

Claude Code 不只依赖 `MEMORY.md`。
它还会扫描 memory 目录中各个 `.md` 文件的 frontmatter header，然后让 side-query 模型为当前用户请求选择最相关的最多 5 个 memory 文件：

- `claude-code/src/memdir/memoryScan.ts:35`
- `claude-code/src/memdir/findRelevantMemories.ts:39`
- `claude-code/src/memdir/findRelevantMemories.ts:46`
- `claude-code/src/memdir/findRelevantMemories.ts:98`
- `claude-code/src/utils/attachments.ts:2215`
- `claude-code/src/utils/attachments.ts:2236`
- `claude-code/src/utils/attachments.ts:2361`

这意味着 Claude Code 的 recall 路径是：

- 稳定入口：`MEMORY.md`
- 动态 recall：根据当前 query 再挑 0~5 条最相关 memories

这是 Bamboo 当前最明显缺失的一环。

### 1.4 Claude Code 明确处理“记忆可能陈旧”

Claude Code 对超过 1 天的 memory 会附加 freshness/staleness 提醒，明确告诉模型：

> memory 是点时间观察，不是 live state；文件路径、函数、行为都可能已经过期，需要先验证。

相关位置：
- `claude-code/src/memdir/memoryAge.ts:33`
- `claude-code/src/memdir/memoryAge.ts:49`
- `claude-code/src/utils/attachments.ts:2327`
- `claude-code/src/tools/FileReadTool/FileReadTool.ts:749`

此外，memory prompt 本身也把“先验证、再相信 memory”写进了规则：
- `claude-code/src/memdir/memoryTypes.ts:201`
- `claude-code/src/memdir/memoryTypes.ts:216`
- `claude-code/src/memdir/memoryTypes.ts:245`

这让 Claude Code 在“旧 memory 误导当前代码状态”这件事上更防守型。

### 1.5 Claude Code 的后台记忆更新隔离更强

它的 session memory 和 extract memories 都是 forked subagent 跑的，并且工具权限被严格限制在 memory 文件范围：

- Session memory：`claude-code/src/services/SessionMemory/sessionMemory.ts:272`
- 只允许编辑目标 memory file：`claude-code/src/services/SessionMemory/sessionMemory.ts:315`
- Durable extract：`claude-code/src/services/extractMemories/extractMemories.ts:166`
- 发现主 agent 已经写入 memory 时会跳过，避免重复/竞争：`claude-code/src/services/extractMemories/extractMemories.ts:121`
- 提取流程有 cursor / in-progress / trailing run 防重叠：`claude-code/src/services/extractMemories/extractMemories.ts:296`

这让 Claude Code 的后台更新更像一个受控、幂等、低污染的辅助系统。

---

## 2. Bamboo 现在的问题在哪里

### 2.1 Cross-session 注入使用的是 Dream notebook，不是 durable memory index

Bamboo 每轮会把 external memory 注入 system message：
- 读取 Dream notebook：`bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:61`
- 注入 section：`bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:169`
- 同时注入 session note topics：`bamboo/src/agent/loop_module/runner/prompt_context/external_memory.rs:86`

这说明 Bamboo 主 prompt 默认看到的跨 session 信息，**主要是 Dream notebook**，而不是 durable memory 的 `MEMORY.md` 视图，也不是 query-time relevant memory recall。

### 2.2 Dream notebook 是全局的，不按当前项目过滤

`auto_dream` 收集候选 session 时只筛选：
- root session
- updated_at >= since
- 去重 root_session_id

但**不按当前 project_key 过滤**：
- `bamboo/src/server/services/auto_dream.rs:91`
- `bamboo/src/server/services/auto_dream.rs:121`
- `bamboo/src/server/services/auto_dream.rs:454`

然后写入的是全局 dream view：
- `bamboo/src/agent/core/memory_store/store.rs:234`
- `bamboo/src/server/services/auto_dream.rs:469`

这意味着当前在 `zenith` 会话里，看到的 cross-session 内容，可能来自：

- Bamboo 仓库的别的工作流
- Bodhi / Lotus / Pavilion
- 甚至 `~/.bamboo` 本身的运维排查会话

这就是**跨项目污染**的第一大来源。

### 2.3 Dream notebook 不是累积式 canonical notebook，而是“最近一批 session 的覆盖式总结”

这是我认为最关键的问题。

`run_auto_dream_once_with_store()` 会：

1. 读取当前 Dream notebook
2. 只用它来解析 `Last consolidated at`
3. 收集“上次 consolidate 之后”的 sessions
4. 基于这些 sessions 重新生成 notebook
5. **直接覆盖写回 Dream notebook**

相关位置：
- 读取 existing dream：`bamboo/src/server/services/auto_dream.rs:445`
- 解析 since：`bamboo/src/server/services/auto_dream.rs:449`
- 只收集 since 之后的 sessions：`bamboo/src/server/services/auto_dream.rs:454`
- 重新生成并覆盖写回：`bamboo/src/server/services/auto_dream.rs:459`
- `bamboo/src/server/services/auto_dream.rs:470`

注意：**它没有把 existing Dream notebook 作为输入继续做 merge / refine。**
也就是说，Dream notebook 当前更像：

> “最近一批 session 的摘要快照”

而不是：

> “稳定累积的跨 session canonical memory”

所以你看到“cross session 内容其实就是上一个 session 的内容”，不是错觉，而是**当前设计天然会这样**。

### 2.4 Dream notebook 是后台定时任务，天然滞后

Bamboo 的 dream 是定时跑的：
- `DREAM_INTERVAL_SECS = 60 * 30`：`bamboo/src/server/services/auto_dream.rs:18`
- 启动时注册后台任务：`bamboo/src/server/app_state/builder.rs:174`
- 定时执行：`bamboo/src/server/services/auto_dream.rs:507`

也就是说：

- 新 session 刚发生时，Dream 不会立刻反映
- 当前 session 很可能看到的是 30 分钟内上一次 consolidate 结果

这就是**时间滞后**的第二个原因。

### 2.5 Bamboo 的 durable memory 虽然会生成 `MEMORY.md` / indexes，但主对话并不会自动消费

`MemoryStore::refresh_scope_artifacts()` 确实会在 durable memory 更新后生成：

- `lexical.json`
- `graph.json`
- `recent.json`
- `stale_candidates.json`
- `taxonomy.json`
- `views/MEMORY.md`
- `views/RECENT.md`
- `views/STALE.md`

相关位置：
- `bamboo/src/agent/core/memory_store/store.rs:1182`
- `bamboo/src/agent/core/memory_store/store.rs:1212`
- `bamboo/src/agent/core/memory_store/store.rs:1242`
- `bamboo/src/agent/core/memory_store/store.rs:1315`

但是我在代码里没有看到像 Claude Code 那样：

- 主 prompt 自动加载 durable `MEMORY.md`
- 或者按 query 自动做 relevant-memory recall

目前主消费路径主要是：
- Dream notebook 自动注入
- session note 自动注入
- durable memory 通过 `memory query/get` 工具由模型主动调用

相关位置：
- `bamboo/src/server/tools/memory.rs:467`
- `bamboo/src/agent/core/memory_store/store.rs:251`

因此 Bamboo 当前的 durable memory 更像：

> 存储平台已经有了，但默认召回链路不够强

### 2.6 这会导致“写进去了，但模型不一定看到”

Bamboo 的 durable memory 写入后会刷新索引和视图，这是好的。
但如果模型不主动执行：

```json
{"action":"query", ...}
```

那么这些信息并不会像 Claude Code 的 `MEMORY.md` 那样天然进入当前推理上下文。

所以体验上就会变成：

- 盘上有 durable memory
- 甚至已经生成了 `MEMORY.md`
- 但 agent 默认仍然主要看 Dream notebook + session_note

这就是“基础设施很完整，但实际 recall 体验不稳定”的根本原因。

### 2.7 project key 解析存在次级串 scope 风险

Bamboo 的 durable memory 在没有显式 `project_key` 时，会尝试从 session workspace 推导；如果拿不到，会回退到 configured default workspace：

- `bamboo/src/server/tools/memory.rs:52`
- `bamboo/src/server/tools/memory.rs:65`
- `bamboo/src/agent/core/memory_store/store.rs:63`
- `bamboo/src/agent/tools/tools/workspace_state.rs:37`
- `bamboo/src/agent/tools/tools/workspace_state.rs:52`

这在正常情况下是方便的，但也意味着：

- 如果 session workspace 没绑定对
- 或某些后台流程没有正确继承 workspace
- 或使用了默认 workspace 作为 fallback

那么 durable memory 可能写到/查到错误的 project scope。

我没有在这次检查中确认它已经发生，但从设计上看，这是**潜在错 scope 风险点**。

---

## 3. 本机实证：你当前的 Bamboo 数据也印证了这个问题

本机 `~/.bamboo/memory/v1` 中，以下文件确实存在：

- 全局 Dream notebook：`/Users/bigduu/.bamboo/memory/v1/scopes/global/views/DREAM_NOTEBOOK.md`
- 全局 durable memory index：`/Users/bigduu/.bamboo/memory/v1/scopes/global/views/MEMORY.md`
- 多个 project 级 `views/MEMORY.md`
- 多个 `lexical.json` / `graph.json` / `recent.json`

说明 Bamboo 的 durable memory artifact 生成是工作的。

但我直接读了当前全局 Dream notebook，内容是：

- `~/.bamboo` 本地工作区诊断
- legacy session JSON / migration backup / encryption key
- `/api/v1/health` / `/api/v1/sessions` / SSE 排查

而不是我们这次正在做的 `zenith` vs `claude-code` memory 对比。

这正说明：

1. 当前注入到 prompt 的 cross-session 视图，确实可能还是上一次/上一批 session 的内容
2. 而且内容甚至可能属于不同工作流

---

## 4. 结构化对比

### Claude Code

优点：

- `MEMORY.md` 作为稳定索引，主入口非常清晰
- memory files 会进入主上下文
- query-time relevant memory recall 很强
- 有 freshness / drift 提醒
- 后台 extractor / session memory 用隔离 subagent，权限小、竞争少
- worktree / path / memory dir 处理比较成熟

弱点：

- 更偏文件系统和 prompt 工程，memory graph / merge / contradiction 管理不如 Bamboo 丰富
- structured memory platform 能力不如 Bamboo 的 durable memory store 完整

### Bamboo

优点：

- session / project / global 三层 scope
- type/status/relations/index/view 很完整
- merge / purge / inspect / rebuild 能力成熟
- session note topic 化设计很实用
- Dream + durable memory 的架构野心比 Claude Code 更大

弱点：

- cross-session 主入口不是 canonical index，而是 Dream snapshot
- Dream 是全局的，不按 project filter
- Dream 是覆盖式 recent-summary，不是累积式 canonical memory
- Dream 定时刷新，容易滞后
- durable `MEMORY.md` 和索引默认没有进入主 prompt
- durable memory 召回靠模型主动调用工具，不够自动

---

## 5. 我认为最关键的根因排序

### P0 — Dream notebook 不是 canonical cumulative memory，而是 recent batch overwrite

这是最核心的问题。

影响：
- cross-session 内容像“上一轮 session 摘要”
- 稳定性差
- 历史连续性弱

### P0 — Dream notebook 是全局视图，不按项目过滤

影响：
- `zenith` 看到 `~/.bamboo` / `bodhi` / `lotus` 的历史
- 串项目、串工作流

### P1 — durable memory 的 `MEMORY.md` / indexes 没有像 Claude Code 那样自动进入主上下文

影响：
- 有存储，没入口
- recall 命中依赖 agent 自觉调用工具

### P1 — 缺少 query-time relevant memory recall

影响：
- 无法像 Claude Code 那样按当前用户请求挑最相关的 3~5 条 memory 注入
- memory 召回更“被动”

### P2 — Dream 刷新周期 30 分钟，导致体验滞后

影响：
- 当前 session 看见的 cross-session context 经常过旧

### P2 — project key fallback 设计增加错 scope 风险

影响：
- 在某些缺 metadata / default workspace 场景下，durable memory 可能误归档到错误 project

---

## 6. 修复建议（按优先级）

### 方案 A：先修最影响体验的主召回链路

#### A1. 停止把 Dream notebook 当作唯一 cross-session 主入口

建议改成：

- Dream notebook 只做“辅助全局运营摘要”
- 主会话默认注入的跨 session 信息改为：
  - 当前 project 的 durable `views/MEMORY.md`
  - 再加少量 query-time relevant memory recall

也就是把 Claude Code 的两层结构引进 Bamboo：

- **稳定索引层**：project `MEMORY.md`
- **动态召回层**：relevant memories for current query

#### A2. 把 durable `MEMORY.md` 注入主 prompt

在 `external_memory.rs` 或新的 prompt section 中，增加：

- current project 的 `views/MEMORY.md`（短截断）
- 必要时还有 `RECENT.md`

至少要做到：

- Claude Code 有 `MEMORY.md` 主入口
- Bamboo 也有自己的 canonical memory index 主入口

#### A3. 增加 query-time relevant durable memory recall

建议在 Bamboo 加一个轻量 recall 过程：

1. 当前用户消息到来
2. 在当前 project scope 的 `lexical.json` / docs 中先做 lexical shortlist
3. 用 fast model 或规则重排
4. 注入前 3~5 条 memory 摘要到 prompt
5. 附带 freshness/staleness note

这一步会立刻拉近和 Claude Code 的体验差距。

---

### 方案 B：修正 Dream notebook 的定位和算法

#### B1. Dream 要么 project-scoped，要么显式分层

至少做：

- `global dream`
- `project dream`

当前会话优先注入：
- 当前 project dream
- 不要默认注入 global dream

#### B2. Dream 生成不要只基于 `since last consolidated` 的新 sessions 覆盖整份 notebook

应该改成二选一：

1. **Refine 模式**：旧 notebook + 新 sessions -> 新 notebook
2. **Periodic full rebuild 模式**：基于一个更明确的 session window / durable memory window 重建

但无论哪种，都不能是现在这种：

> 只看增量 session，然后整份 dream 覆盖掉旧 dream

#### B3. 缩短触发延迟，或改成事件驱动

可以考虑：

- session finalize 后 debounce 触发一次 project dream update
- 或保留定时任务，但增加“最近活跃项目优先刷新”

---

### 方案 C：把 durable memory 真正用起来

#### C1. 主 prompt 显式区分三层 memory

建议改成：

1. session note：当前会话 continuity
2. project memory index：当前项目 durable knowledge
3. optional global dream：跨项目运营/习惯性线索

而不是当前这样主要靠：

- session note
- global dream

#### C2. 给 `memory query/get` 增加自动化调用钩子

例如：
- 当用户提到 “remember / recall / 上次说过 / 之前我们决定过” 时，系统可优先触发 recall
- 或在 planner / prelude 阶段加 lightweight heuristic

#### C3. 在 durable memory recall 上增加 staleness guard

参考 Claude Code：

- 超过 N 天的 memory 自动附加“可能过时，先验证”的提醒
- 对 file path / symbol / config claim 做更强的验证提示

---

## 7. 最小可执行改造路径

如果你想低风险、快速提升体验，我建议按下面顺序做：

### 第一步（最高 ROI）

1. **在当前 project scope 下，把 `views/MEMORY.md` 注入 prompt**
2. **Dream 改成 project-scoped 优先，global dream 不默认注入**
3. **给 recalled durable memory 加 freshness note**

### 第二步

4. **新增 relevant durable memory recall**（基于 lexical shortlist + fast model rerank）
5. **把 Dream 从覆盖式 recent-summary 改为 refine/cumulative**

### 第三步

6. **把 recall 触发做成系统级，而不是纯靠 agent 主动调用 `memory query`**
7. **减少/消除 project key fallback 带来的错 scope 风险**

---

## 8. 最后一句结论

如果用一句话概括这次检查结果：

> **Claude Code 的优势不是“memory 文件更多”，而是它把 memory 做成了主上下文的一等公民：有入口索引、有相关性召回、有陈旧性防护。Bamboo 现在则更像“存储平台先行、召回链路滞后”，所以你会明显感觉到 cross-session 信息不稳、像上一轮 session、也缺少 MEMORY.md 索引入口。**

所以方向上不是推翻 Bamboo 的 memory store，反而是：

> **保留 Bamboo 已经很强的 structured memory store，但把 Claude Code 那套“MEMORY.md + relevant recall + freshness guard”补进主 prompt/runtime。**
