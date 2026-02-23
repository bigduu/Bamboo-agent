# Auto-Publish Workflow Guide

## Overview

The bamboo-agent project now has an automated publishing system that:
- Detects when the version in `Cargo.toml` is changed
- Automatically creates a git tag
- Creates a GitHub Release
- Publishes the crate to crates.io

## How It Works

### 1. Version Detection
When you push changes to `main` branch that modify `Cargo.toml`, the workflow:
- Compares the current version with the previous commit
- If the version changed, it triggers the release process

### 2. Automatic Actions
When a version change is detected:
1. Creates an annotated git tag (e.g., `v0.1.0`)
2. Creates a GitHub Release with auto-generated release notes
3. Publishes to crates.io

## How to Release a New Version

### Step 1: Update Version
Edit `Cargo.toml` and update the version:

```toml
[package]
name = "bamboo-agent"
version = "0.2.0"  # Changed from 0.1.0
```

### Step 2: Commit and Push
```bash
git add Cargo.toml
git commit -m "chore: bump version to 0.2.0"
git push origin main
```

### Step 3: Automatic Release
The workflow will automatically:
- ✅ Create tag `v0.2.0`
- ✅ Create GitHub Release
- ✅ Publish to crates.io

## Required Setup

### CARGO_REGISTRY_TOKEN Secret

You need to set up the `CARGO_REGISTRY_TOKEN` secret in GitHub:

1. **Get API Token from crates.io:**
   - Visit https://crates.io/settings/tokens
   - Click "New Token"
   - Give it a name (e.g., "GitHub Actions")
   - Select scopes: `publish-update`
   - Copy the generated token

2. **Add Secret to GitHub Repository:**
   - Go to https://github.com/bigduu/Bamboo-agent/settings/secrets/actions
   - Click "New repository secret"
   - Name: `CARGO_REGISTRY_TOKEN`
   - Value: Paste your token
   - Click "Add secret"

## Workflow File

The workflow is defined in `.github/workflows/auto-publish.yml`:

```yaml
name: Auto Publish

on:
  push:
    branches: [ main ]
    paths:
      - 'Cargo.toml'
```

## Manual Publishing (Alternative)

If you need to manually publish (e.g., for testing), you can still use the manual workflow:

1. Go to Actions → "Publish to crates.io"
2. Click "Run workflow"
3. Select branch and run

Or use the command line:
```bash
cargo publish --token <your-token>
```

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (X.0.0): Incompatible API changes
- **MINOR** (0.X.0): New features, backwards compatible
- **PATCH** (0.0.X): Bug fixes, backwards compatible

Examples:
- `0.1.0` → `0.1.1`: Bug fix
- `0.1.0` → `0.2.0`: New feature
- `0.1.0` → `1.0.0`: Breaking change

## Monitoring Releases

### GitHub Actions
View workflow runs: https://github.com/bigduu/Bamboo-agent/actions/workflows/auto-publish.yml

### crates.io
View published versions: https://crates.io/crates/bamboo-agent/versions

### docs.rs
Documentation is auto-generated: https://docs.rs/bamboo-agent

## Troubleshooting

### Workflow Not Triggered
- Ensure you're pushing to `main` branch
- Check that `Cargo.toml` was actually modified
- Verify the version number changed

### Publishing Fails
- Check `CARGO_REGISTRY_TOKEN` is set correctly
- Ensure token has `publish-update` scope
- Check crate name isn't already taken

### Need to Republish?
If publishing fails but tag was created:
1. Delete the tag: `git tag -d v0.2.0 && git push origin :refs/tags/v0.2.0`
2. Delete GitHub Release (if created)
3. Fix the issue
4. Bump version again and push

## Example Release Process

```bash
# 1. Update Cargo.toml version
vim Cargo.toml  # Change version from 0.1.0 to 0.2.0

# 2. Update CHANGELOG.md (optional but recommended)
vim CHANGELOG.md  # Add release notes

# 3. Commit
git add Cargo.toml CHANGELOG.md
git commit -m "chore: release version 0.2.0

- Add new feature X
- Fix bug Y
- Improve documentation"

# 4. Push and let automation handle the rest
git push origin main

# 5. Verify
# - Check GitHub Actions: https://github.com/bigduu/Bamboo-agent/actions
# - Check GitHub Releases: https://github.com/bigduu/Bamboo-agent/releases
# - Check crates.io: https://crates.io/crates/bamboo-agent
```

---

**Note**: The first time you set up `CARGO_REGISTRY_TOKEN`, you may want to test with a pre-release version (e.g., `0.1.0-alpha.1`) before releasing a stable version.
