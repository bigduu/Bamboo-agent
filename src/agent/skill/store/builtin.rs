use std::collections::HashMap;

use crate::types::SkillDefinition;

/// Script content embedded at compile time
pub const INIT_SKILL_SCRIPT: &str = include_str!("builtin_scripts/init_skill.py");
pub const VALIDATE_SKILL_SCRIPT: &str = include_str!("builtin_scripts/validate_skill.py");

const SKILL_CREATOR_PROMPT: &str = r#"# Skill Creator

This skill provides guidance for creating effective skills for Bamboo.

## About Skills

Skills are modular, self-contained folders that extend Bamboo's capabilities by providing specialized knowledge and workflows. They are stored in `~/.bamboo/skills/` as individual folders.

### What Skills Provide

1. **Specialized workflows** - Multi-step procedures for specific domains
2. **Tool integrations** - Instructions for working with specific tools or APIs
3. **Domain expertise** - Project-specific knowledge, schemas, business logic
4. **Bundled resources** - Scripts, references, and assets for complex tasks

## Core Principles

### Concise is Key

The context window is limited. Skills share context with conversation history and system prompts.

**Default assumption:** The AI is already very smart. Only add context it doesn't already have.

Prefer concise examples over verbose explanations.

### Skill Anatomy

Every skill is a folder containing:

```
skill-name/
├── SKILL.md (required)
│   ├── YAML frontmatter (required)
│   │   ├── id: skill-name (kebab-case, matches folder name)
│   │   ├── name: Display Name
│   │   ├── description: When to use this skill (important for triggering)
│   │   ├── category: Category for grouping
│   │   ├── tags: [] (searchable tags)
│   │   ├── tool_refs: [] (tools this skill uses)
│   │   ├── workflow_refs: [] (workflows this skill uses)
│   │   ├── visibility: public | private
│   │   ├── version: "1.0.0"
│   │   ├── created_at: "2026-02-01T00:00:00Z"
│   │   └── updated_at: "2026-02-01T00:00:00Z"
│   └── Body (Markdown prompt content)
├── scripts/ (optional) - Executable scripts
├── references/ (optional) - Documentation
└── assets/ (optional) - Templates, files
```

### Directory Structure

Skills can be organized in subdirectories for better organization:

```
~/.bamboo/skills/
├── builtin/
│   ├── file-analysis/
│   │   └── SKILL.md
│   ├── code-review/
│   │   └── SKILL.md
│   └── project-setup/
│       └── SKILL.md
├── custom/
│   ├── my-api-helper/
│   │   └── SKILL.md
│   └── my-workflow/
│       └── SKILL.md
└── skill-creator/
    └── SKILL.md
```

The system recursively searches for all `SKILL.md` files in `~/.bamboo/skills/`. Any directory containing a `SKILL.md` file is considered a skill directory. The `id` in the frontmatter must match the directory name (the immediate parent of `SKILL.md`).

### Bundled Resources

**Scripts (`scripts/`)**
- Executable code (Python/Bash/etc.)
- Use when deterministic reliability is needed
- Example: `scripts/rotate_pdf.py` for PDF operations

**References (`references/`)**
- Documentation loaded into context as needed
- Example: Database schemas, API docs, workflow guides
- Reference from SKILL.md with clear "when to read" guidance

**Assets (`assets/`)**
- Files used in output (templates, images, fonts)
- Example: `assets/logo.png`, `assets/template.pptx`

## Skill Creation Process

### Step 1: Understand the Skill

Ask clarifying questions:
- "What functionality should this skill support?"
- "Can you give examples of how this skill would be used?"
- "What would a user say that should trigger this skill?"

### Step 2: Plan Resources

Analyze what reusable resources would help:
- Scripts for repetitive code
- References for complex documentation
- Assets for templates

### Step 3: Initialize the Skill

Use the init script to create the skill:

```bash
python3 ~/.bamboo/skills/skill-creator/scripts/init_skill.py <skill-name> --path ~/.bamboo/skills
```

Options:
- `--resources scripts,references,assets` - Create resource directories
- `--examples` - Add example files

### Step 4: Edit SKILL.md

**Frontmatter fields:**
- `id`: Must match folder name (kebab-case)
- `name`: Display name
- `description`: **Critical** - This determines when the skill triggers. Include specific scenarios and triggers.
- `category`: For grouping in UI
- `tags`: Searchable keywords
- `tool_refs`: List of tools this skill uses
- `workflow_refs`: List of workflows this skill uses

**Body content:**
- Instructions for using the skill
- Reference bundled resources as needed
- Keep under 500 lines; split large content to references/

### Step 5: Validate

Run the validator to check structure:

```bash
python3 ~/.bamboo/skills/skill-creator/scripts/validate_skill.py ~/.bamboo/skills/<skill-name>
```

## Skill Naming

- Use kebab-case: `my-new-skill`
- Maximum 64 characters
- Use lowercase letters, digits, and hyphens only
- Prefer verb-led phrases: `pdf-processor`, `api-helper`
- Folder name must exactly match skill `id`

## Best Practices

1. **Start simple** - Add complexity only when needed
2. **Test scripts** - Run them to ensure they work
3. **Reference strategically** - Link to references from SKILL.md with clear usage guidance
4. **Validate frontmatter** - Ensure id matches folder name, timestamps are valid ISO 8601
5. **Keep descriptions clear** - This is how the system knows when to use your skill
"#;

pub fn create_builtin_skills() -> Vec<SkillDefinition> {
    vec![
        SkillDefinition::new(
            "builtin-file-analysis",
            "File Analysis",
            "Read and analyze file contents, providing summaries and key information",
            "analysis",
            "You are a file analysis expert. Use the read_file tool to read files, then provide a structured analysis including:\n1. File type and purpose\n2. Main content summary\n3. Key code/data snippets\n4. Potential issues or improvements",
        )
        .with_tool_ref("read_file")
        .with_tag("files")
        .with_tag("analysis"),
        SkillDefinition::new(
            "builtin-code-review",
            "Code Review",
            "Review code changes to identify potential issues and improvement opportunities",
            "development",
            "You are a code review expert. When analyzing code changes, focus on:\n1. Code quality and readability\n2. Potential bugs and security issues\n3. Performance impact\n4. Alignment with best practices\n5. Test coverage",
        )
        .with_tool_ref("read_file")
        .with_tag("code")
        .with_tag("review"),
        SkillDefinition::new(
            "builtin-project-setup",
            "Project Setup",
            "Help set up new projects by creating necessary configuration files and directory structures",
            "development",
            "You are a project setup expert. When helping users set up a new project:\n1. Analyze project type and requirements\n2. Create a recommended directory structure\n3. Generate basic configuration files\n4. Provide next-step guidance",
        )
        .with_workflow_ref("create-project")
        .with_tag("project")
        .with_tag("setup"),
        SkillDefinition::new(
            "skill-creator",
            "Skill Creator",
            "Guide for creating effective skills for Bamboo. Use this skill when users want to create a new skill that extends Bamboo's capabilities with specialized knowledge, workflows, or tool integrations.",
            "system",
            SKILL_CREATOR_PROMPT,
        )
        .with_tag("skills")
        .with_tag("development"),
    ]
}

/// Get embedded script content for a builtin skill
/// Returns a map of relative file path -> content
pub fn get_builtin_scripts(skill_id: &str) -> HashMap<String, String> {
    let mut scripts = HashMap::new();

    if skill_id == "skill-creator" {
        scripts.insert(
            "scripts/init_skill.py".to_string(),
            INIT_SKILL_SCRIPT.to_string(),
        );
        scripts.insert(
            "scripts/validate_skill.py".to_string(),
            VALIDATE_SKILL_SCRIPT.to_string(),
        );
    }

    scripts
}
