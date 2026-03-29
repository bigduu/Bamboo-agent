# Documentation Tasks - 未文档化的公共项

## 优先级分类

### 🔴 P0 - 核心模块（最高优先级）

#### 1. agent/core/tools/agentic.rs (36 items)
**最重要** - 智能代码审查和代理工具系统
- [ ] 3 structs: `ToolGoal`, `AgenticContext`, `SmartCodeReviewTool`
- [ ] 3 enums: `InteractionRole`, `Interaction`, `ToolResult`
- [ ] 2 traits: `ToolExecutor`, `AgenticTool`
- [ ] 27 functions (包括所有交互和工具执行逻辑)
- [ ] 1 type alias

#### 2. agent/llm/models.rs (21 items)
**关键** - LLM API 模型定义
- [ ] 4 enums: `Role`, `Content`, `ContentPart`, `ToolChoice`
- [ ] 17 structs: 所有请求/响应模型（ChatCompletion, Message, 等）

#### 3. agent/core/tools/registry.rs (17 items)
**核心** - 工具注册表系统
- [ ] 1 struct: `ToolRegistry`
- [ ] 1 enum: `RegistryError`
- [ ] 1 trait: `Tool`
- [ ] 14 functions (注册、查找、执行工具)
- [ ] 1 type alias

#### 4. agent/core/tools/types.rs (5 items)
**基础** - 工具类型定义
- [ ] 5 structs: `ToolCall`, `FunctionCall`, `ToolResult`, `ToolSchema`, `FunctionSchema`

#### 5. agent/core/agent/types.rs (11 items)
**核心** - Agent 核心类型
- [ ] 2 enums: `Role`, `MessageContent`
- [ ] 3 structs: `Message`, `PendingQuestion`, `Session`
- [ ] 6 functions (Session 管理方法)

#### 6. agent/core/agent/events.rs (2 items)
**重要** - Agent 事件系统
- [ ] 1 enum: `AgentEvent`
- [ ] 1 struct: `TokenUsage`

---

### 🟡 P1 - 重要模块

#### 7. agent/core/composition/mod.rs (29 items)
DSL 工具工作流系统
- [ ] 2 structs: `SequenceBuilder`, `ParallelBuilder`
- [ ] 17 functions (build_sequence, build_parallel, 等)
- [ ] 5 modules
- [ ] 5 re-exports

#### 8. agent/tools/guide/mod.rs (14 items)
工具指南系统
- [ ] 3 structs: `ToolGuideSpec`, `ToolExample`, `EnhancedPromptBuilder`
- [ ] 1 enum: `ToolCategory`
- [ ] 1 trait: `ToolGuide`
- [ ] 7 functions
- [ ] 2 modules

#### 9. agent/core/tools/accumulator.rs (10 items)
工具调用累加器
- [ ] 2 structs: `ToolCallAccumulator`, `PartialToolCall`
- [ ] 8 functions

#### 10. agent/core/storage/jsonl.rs (9 items)
JSONL 存储实现
- [ ] 1 struct: `JsonlStorage`
- [ ] 1 trait: `Storage`
- [ ] 7 functions

---

### 🟢 P2 - 模块文档

#### 11-15. 所有 mod.rs 文件
为所有模块添加模块级文档（`//!`）
- [ ] `agent/core/mod.rs` (15 items)
- [ ] `agent/core/tools/mod.rs` (14 items)
- [ ] `agent/llm/mod.rs` (20 items)
- [ ] `agent/tools/tools/mod.rs` (46 items)
- [ ] `agent/tools/mod.rs` (9 items)

---

### 🔵 P3 - 工具实现

#### 16. agent/tools/tools/*.rs (19 files, 19 items)
每个工具的 `new()` 函数
- [ ] `apply_patch.rs`
- [ ] `conclusion_with_options.rs`
- [ ] `create_todo_list.rs`
- [ ] `execute_command.rs`
- [ ] `file_exists.rs`
- [ ] `get_current_dir.rs`
- [ ] `get_file_info.rs`
- [ ] `git_diff.rs`
- [ ] `git_status.rs`
- [ ] `git_write.rs`
- [ ] `list_directory.rs`
- [ ] `read_file.rs`
- [ ] `read_file_range.rs`
- [ ] `registry.rs`
- [ ] `search_in_file.rs`
- [ ] `search_in_project.rs`
- [ ] `set_workspace.rs`
- [ ] `update_todo_item.rs`
- [ ] `write_file.rs`

---

### ⚪ P4 - 其他模块

#### 17-25. 其他文件
- [ ] `agent/llm/provider.rs` (4 items)
- [ ] `agent/llm/error.rs` (2 items)
- [ ] `agent/loop_module/runner.rs` (3 items)
- [ ] `agent/tools/guide/builtin_guides.rs` (4 items)
- [ ] `agent/tools/guide/context.rs` (5 items)
- [ ] `agent/llm/providers/copilot/auth/handler.rs` (8 items)
- [ ] `error.rs` (2 items: `BambooError`, `Result`)
- [ ] `agent/core/tools/executor.rs` (4 items)
- [ ] 其他提供商文件

---

## 统计

- **总文件数**: 68 个文件
- **总项目数**: 412 个未文档化项
- **预计时间**: 2-3 周完整文档化

## 建议工作流程

1. **Week 1**: 完成 P0 级别（核心模块）
2. **Week 2**: 完成 P1 级别（重要模块）+ P2 级别（模块文档）
3. **Week 3**: 完成 P3 和 P4 级别（工具实现和其他）

## 文档标准

每个文档应包含：

### Structs
```rust
/// Brief description.
///
/// Detailed explanation of purpose and usage.
///
/// # Fields
///
/// * `field1` - Description
/// * `field2` - Description
///
/// # Examples
///
/// ```
/// use module::Struct;
/// let s = Struct::new();
/// ```
```

### Enums
```rust
/// Brief description.
///
/// Detailed explanation.
///
/// # Variants
///
/// * `Variant1` - Description
/// * `Variant2` - Description
///
/// # Examples
///
/// ```
/// let variant = Enum::Variant1;
/// ```
```

### Functions
```rust
/// Brief description.
///
/// Detailed explanation.
///
/// # Arguments
///
/// * `arg1` - Description
/// * `arg2` - Description
///
/// # Returns
///
/// Description of return value
///
/// # Errors
///
/// When this function returns an error
///
/// # Examples
///
/// ```
/// let result = function(arg1, arg2)?;
/// ```
```

### Traits
```rust
/// Brief description.
///
/// Detailed explanation of the trait's purpose.
///
/// # Required Methods
///
/// * `method1` - Description
///
/// # Provided Methods
///
/// * `method2` - Description
///
/// # Implementors
///
/// List of types that implement this trait.
```

### Modules
```rust
//! Module description.
//!
//! Detailed explanation of the module's purpose.
//!
//! # Overview
//!
//! High-level description of the module structure.
//!
//! # Usage
//!
//! ```
//! use module::Type;
//! ```
```

## 进度追踪

- [ ] P0 级别完成 (0/6)
- [ ] P1 级别完成 (0/5)
- [ ] P2 级别完成 (0/5)
- [ ] P3 级别完成 (0/19)
- [ ] P4 级别完成 (0/9)

**完成百分比**: 0% (0/412)
