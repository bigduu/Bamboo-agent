#!/usr/bin/env python3
"""
Skill Validator for Bamboo - Validates skill structure and content

Usage:
    validate_skill.py <path/to/skill-folder>

Examples:
    validate_skill.py ~/.bamboo/skills/my-skill
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    print("[WARN] PyYAML not installed. Using basic validation only.")
    print("Install with: python3 -m pip install pyyaml")


REQUIRED_FIELDS = ["id", "name", "description", "category", "tags", "tool_refs", "workflow_refs", "visibility", "version", "created_at", "updated_at"]
VALID_VISIBILITY = {"public", "private"}


def is_valid_skill_id(skill_id):
    """Check if skill ID is valid kebab-case."""
    if not skill_id:
        return False
    if not skill_id[0].islower():
        return False
    return all(c.islower() or c.isdigit() or c == '-' for c in skill_id)


def validate_timestamp(timestamp_str):
    """Validate ISO 8601 timestamp format."""
    try:
        datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


def validate_skill(skill_dir):
    """Validate a skill directory."""
    skill_path = Path(skill_dir)
    errors = []
    warnings = []

    # Check directory exists
    if not skill_path.exists():
        print(f"[ERROR] Skill directory does not exist: {skill_path}")
        return False

    if not skill_path.is_dir():
        print(f"[ERROR] Path is not a directory: {skill_path}")
        return False

    skill_name = skill_path.name

    # Check SKILL.md exists
    skill_md = skill_path / "SKILL.md"
    if not skill_md.exists():
        print(f"[ERROR] SKILL.md not found in {skill_path}")
        return False

    # Read and parse SKILL.md
    try:
        content = skill_md.read_text()
    except Exception as e:
        print(f"[ERROR] Cannot read SKILL.md: {e}")
        return False

    # Split frontmatter and body
    if not content.startswith("---"):
        print("[ERROR] SKILL.md must start with YAML frontmatter (---)")
        return False

    parts = content.split("---", 2)
    if len(parts) < 3:
        print("[ERROR] Invalid frontmatter format. Expected: ---\n<yaml>\n---\n<body>")
        return False

    frontmatter_str = parts[1].strip()
    body = parts[2].strip()

    if not HAS_YAML:
        # Basic validation without PyYAML - just check structure
        print(f"[OK] Basic validation passed for: {skill_name}")
        print("[INFO] Install PyYAML for full validation: python3 -m pip install pyyaml")

        # Basic checks
        if not body:
            print("[WARN] SKILL.md body is empty")
        elif len(body) < 100:
            print("[WARN] SKILL.md body is very short (< 100 characters)")

        todo_count = body.count("TODO") + body.count("[TODO")
        if todo_count > 0:
            print(f"[WARN] Found {todo_count} TODO markers in body")

        return True

    # Parse YAML frontmatter
    try:
        frontmatter = yaml.safe_load(frontmatter_str)
    except yaml.YAMLError as e:
        print(f"[ERROR] Invalid YAML frontmatter: {e}")
        return False

    if not isinstance(frontmatter, dict):
        print("[ERROR] Frontmatter must be a YAML object")
        return False

    # Validate required fields
    for field in REQUIRED_FIELDS:
        if field not in frontmatter:
            errors.append(f"Missing required field: {field}")

    if errors:
        print("[ERROR] Validation failed:")
        for error in errors:
            print(f"   - {error}")
        return False

    # Validate id matches folder name
    if frontmatter["id"] != skill_name:
        errors.append(f"id '{frontmatter['id']}' does not match folder name '{skill_name}'")

    # Validate skill ID format
    if not is_valid_skill_id(frontmatter["id"]):
        errors.append(f"Invalid id format '{frontmatter['id']}': must be kebab-case starting with lowercase letter")

    # Validate visibility
    if frontmatter["visibility"] not in VALID_VISIBILITY:
        errors.append(f"Invalid visibility '{frontmatter['visibility']}': must be 'public' or 'private'")

    # Validate timestamps
    for field in ["created_at", "updated_at"]:
        if not validate_timestamp(frontmatter[field]):
            errors.append(f"Invalid timestamp format for {field}: '{frontmatter[field]}'")

    # Validate arrays
    for field in ["tags", "tool_refs", "workflow_refs"]:
        if not isinstance(frontmatter[field], list):
            errors.append(f"{field} must be an array")

    # Check body content
    if not body:
        errors.append("SKILL.md body is empty")
    elif len(body) < 100:
        warnings.append("SKILL.md body is very short (< 100 characters)")

    # Check for TODO markers
    todo_count = body.count("TODO") + body.count("[TODO")
    if todo_count > 0:
        warnings.append(f"Found {todo_count} TODO markers in body - remember to complete them")

    # Check description quality
    description = frontmatter.get("description", "")
    if "TODO" in description or "[TODO" in description:
        warnings.append("Description contains TODO - this is critical for skill triggering")
    if len(description) < 50:
        warnings.append("Description is very short - consider adding more detail for better triggering")

    # Report results (YAML validation only)
    if errors:
        print("[ERROR] Validation failed:")
        for error in errors:
            print(f"   - {error}")
        return False

    print(f"[OK] Skill structure is valid: {skill_name}")

    if warnings:
        print("\n[WARN] Warnings:")
        for warning in warnings:
            print(f"   - {warning}")

    print(f"\n[OK] Frontmatter fields:")
    for field in REQUIRED_FIELDS:
        value = frontmatter[field]
        if isinstance(value, list):
            value = f"[{len(value)} items]"
        elif isinstance(value, str) and len(value) > 50:
            value = value[:50] + "..."
        print(f"   {field}: {value}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Validate a Bamboo skill directory structure.",
    )
    parser.add_argument("skill_path", help="Path to the skill directory")
    args = parser.parse_args()

    success = validate_skill(args.skill_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
