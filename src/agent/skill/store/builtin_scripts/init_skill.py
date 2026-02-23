#!/usr/bin/env python3
"""
Skill Initializer for Bamboo - Creates a new skill from template

Usage:
    init_skill.py <skill-name> --path <path> [--resources scripts,references,assets] [--examples]

Examples:
    init_skill.py my-new-skill --path ~/.bamboo/skills
    init_skill.py my-new-skill --path ~/.bamboo/skills --resources scripts,references
    init_skill.py my-api-helper --path ~/.bamboo/skills --resources scripts --examples
"""

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

MAX_SKILL_NAME_LENGTH = 64
ALLOWED_RESOURCES = {"scripts", "references", "assets"}

SKILL_TEMPLATE = """---
id: {skill_name}
name: {skill_title}
description: [TODO: Describe what this skill does and WHEN to use it. This is critical for skill triggering. Include specific scenarios, file types, or tasks that should trigger this skill.]
category: [TODO: e.g., development, document, system, etc.]
tags:
  - [TODO: tag1]
  - [TODO: tag2]
tool_refs: []
workflow_refs: []
visibility: public
version: "1.0.0"
created_at: "{timestamp}"
updated_at: "{timestamp}"
---

# {skill_title}

## Overview

[TODO: 1-2 sentences explaining what this skill enables]

## When to Use

[TODO: Specific scenarios that trigger this skill. Examples:
- "Use this skill when working with PDF files"
- "Use this skill for code review tasks"
- "Use this skill when analyzing database schemas"
]

## Workflow

[TODO: Describe the workflow for using this skill]

### Step 1: [First step]

[Instructions and examples]

### Step 2: [Second step]

[Instructions and examples]

## Resources

[TODO: Reference bundled resources if applicable]

### scripts/
- `scripts/example.py` - [Description of what this script does]

### references/
- `references/guide.md` - [Description of reference documentation]

### assets/
- `assets/template.txt` - [Description of asset files]

---

**Delete TODO sections as you complete them**
"""

EXAMPLE_SCRIPT = '''#!/usr/bin/env python3
"""
Example helper script for {skill_name}

This is a placeholder script. Replace with actual implementation or delete.

Usage:
    python3 example.py <input> <output>
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description="Example script for {skill_name}")
    parser.add_argument("input", help="Input file")
    parser.add_argument("output", help="Output file")
    args = parser.parse_args()

    # TODO: Add actual script logic here
    print(f"Processing {{args.input}} -> {{args.output}}")


if __name__ == "__main__":
    main()
'''

EXAMPLE_REFERENCE = """# Reference Guide for {skill_title}

This is placeholder reference documentation. Replace with actual content or delete.

## Purpose

[Describe what information this reference contains]

## When to Read This

[Specify when this reference should be loaded into context]

## Content

[Detailed reference information]

## Examples

[Concrete examples if applicable]
"""

EXAMPLE_ASSET = """# Example Asset

This placeholder represents asset files stored in the assets/ directory.

Asset files are NOT loaded into context but are used in the output.

Examples:
- Templates (pptx, docx)
- Images (png, jpg, svg)
- Fonts (ttf, woff2)
- Boilerplate code directories

Replace this file with actual assets or delete if not needed.
"""


def normalize_skill_name(skill_name):
    """Normalize a skill name to lowercase hyphen-case."""
    normalized = skill_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = normalized.strip("-")
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized


def title_case_skill_name(skill_name):
    """Convert hyphenated skill name to Title Case for display."""
    return " ".join(word.capitalize() for word in skill_name.split("-"))


def parse_resources(raw_resources):
    if not raw_resources:
        return []
    resources = [item.strip() for item in raw_resources.split(",") if item.strip()]
    invalid = sorted({item for item in resources if item not in ALLOWED_RESOURCES})
    if invalid:
        allowed = ", ".join(sorted(ALLOWED_RESOURCES))
        print(f"[ERROR] Unknown resource type(s): {', '.join(invalid)}")
        print(f"   Allowed: {allowed}")
        sys.exit(1)
    deduped = []
    seen = set()
    for resource in resources:
        if resource not in seen:
            deduped.append(resource)
            seen.add(resource)
    return deduped


def create_resource_dirs(skill_dir, skill_name, skill_title, resources, include_examples):
    for resource in resources:
        resource_dir = skill_dir / resource
        resource_dir.mkdir(exist_ok=True)
        if resource == "scripts":
            if include_examples:
                example_script = resource_dir / "example.py"
                example_script.write_text(EXAMPLE_SCRIPT.format(skill_name=skill_name))
                example_script.chmod(0o755)
                print("[OK] Created scripts/example.py")
            else:
                print("[OK] Created scripts/")
        elif resource == "references":
            if include_examples:
                example_ref = resource_dir / "guide.md"
                example_ref.write_text(EXAMPLE_REFERENCE.format(skill_title=skill_title))
                print("[OK] Created references/guide.md")
            else:
                print("[OK] Created references/")
        elif resource == "assets":
            if include_examples:
                example_asset = resource_dir / "example.txt"
                example_asset.write_text(EXAMPLE_ASSET)
                print("[OK] Created assets/example.txt")
            else:
                print("[OK] Created assets/")


def init_skill(skill_name, path, resources, include_examples):
    """Initialize a new skill directory."""
    skill_dir = Path(path).resolve() / skill_name

    if skill_dir.exists():
        print(f"[ERROR] Skill directory already exists: {skill_dir}")
        return None

    try:
        skill_dir.mkdir(parents=True, exist_ok=False)
        print(f"[OK] Created skill directory: {skill_dir}")
    except Exception as e:
        print(f"[ERROR] Error creating directory: {e}")
        return None

    skill_title = title_case_skill_name(skill_name)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    skill_content = SKILL_TEMPLATE.format(
        skill_name=skill_name,
        skill_title=skill_title,
        timestamp=timestamp
    )

    skill_md_path = skill_dir / "SKILL.md"
    try:
        skill_md_path.write_text(skill_content)
        print("[OK] Created SKILL.md")
    except Exception as e:
        print(f"[ERROR] Error creating SKILL.md: {e}")
        return None

    if resources:
        try:
            create_resource_dirs(skill_dir, skill_name, skill_title, resources, include_examples)
        except Exception as e:
            print(f"[ERROR] Error creating resource directories: {e}")
            return None

    print(f"\n[OK] Skill '{skill_name}' initialized successfully at {skill_dir}")
    print("\nNext steps:")
    print("1. Edit SKILL.md to complete the TODO items")
    print("2. Update the description field - this determines when the skill triggers")
    print("3. Add resources to scripts/, references/, assets/ as needed")
    print("4. Run the validator when ready:")
    print(f"   python3 ~/.bamboo/skills/skill-creator/scripts/validate_skill.py {skill_dir}")

    return skill_dir


def main():
    parser = argparse.ArgumentParser(
        description="Create a new Bamboo skill directory with a SKILL.md template.",
    )
    parser.add_argument("skill_name", help="Skill name (normalized to hyphen-case)")
    parser.add_argument("--path", required=True, help="Output directory for the skill")
    parser.add_argument(
        "--resources",
        default="",
        help="Comma-separated list: scripts,references,assets",
    )
    parser.add_argument(
        "--examples",
        action="store_true",
        help="Create example files inside the selected resource directories",
    )
    args = parser.parse_args()

    raw_skill_name = args.skill_name
    skill_name = normalize_skill_name(raw_skill_name)
    if not skill_name:
        print("[ERROR] Skill name must include at least one letter or digit.")
        sys.exit(1)
    if len(skill_name) > MAX_SKILL_NAME_LENGTH:
        print(
            f"[ERROR] Skill name '{skill_name}' is too long ({len(skill_name)} characters). "
            f"Maximum is {MAX_SKILL_NAME_LENGTH} characters."
        )
        sys.exit(1)
    if skill_name != raw_skill_name:
        print(f"Note: Normalized skill name from '{raw_skill_name}' to '{skill_name}'.")

    resources = parse_resources(args.resources)
    if args.examples and not resources:
        print("[ERROR] --examples requires --resources to be set.")
        sys.exit(1)

    path = args.path

    print(f"Initializing skill: {skill_name}")
    print(f"   Location: {path}")
    if resources:
        print(f"   Resources: {', '.join(resources)}")
        if args.examples:
            print("   Examples: enabled")
    else:
        print("   Resources: none (create as needed)")
    print()

    result = init_skill(skill_name, path, resources, args.examples)

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
