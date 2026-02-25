# Default Provider Strategy Analysis

## Problem Statement

The current default provider is set to `"copilot"`, which causes testing challenges:

### Copilot Testing Issues

1. **OAuth2 Authentication Required**
   - Requires real GitHub authentication flow
   - Needs browser interaction
   - Cannot be easily mocked in CI/test environments
   - Tests fail without proper credentials

2. **CI Environment Challenges**
   - No browser available in CI
   - No GitHub tokens available
   - Tests become integration tests instead of unit tests
   - Unreliable test results

### Anthropic Testing Advantages

1. **Simple API Key Authentication**
   - Just needs `ANTHROPIC_API_KEY` environment variable
   - Easy to mock with `wiremock`
   - Works in CI environments
   - Fast and reliable unit tests

2. **Better Developer Experience**
   - Tests run locally without setup
   - Tests are deterministic
   - Easy to debug failures

## Current Impact

### Test Failures

From recent CI runs (v0.2.6):
- `test_copilot_authenticate_endpoint_not_copilot` - Had to be rewritten to accept any status code
- Agent API tests - Indirectly affected by copilot being default
- Any new test that creates a default AppState will use copilot provider

### Development Workflow Impact

1. **New developers** starting the server without config will get copilot errors
2. **Tests** need to explicitly override provider to avoid copilot
3. **Documentation** examples fail without copilot setup

## Analysis

### Option A: Keep "copilot" as default (Current)

**Pros:**
- ✅ Aligned with product strategy (free tier, no API key needed)
- ✅ Better for end users (no API key setup required)
- ✅ Encourages copilot adoption

**Cons:**
- ❌ Testing is difficult
- ❌ CI requires special handling
- ❌ Poor developer experience
- ❌ Documentation examples don't work out-of-box

**Mitigation:**
- Provide mock copilot authentication for tests
- Document testing strategies
- Add test helpers to override default provider

### Option B: Revert to "anthropic" as default

**Pros:**
- ✅ Easy to test (just mock API calls)
- ✅ Works in CI without special setup
- ✅ Better developer experience
- ✅ Documentation examples work immediately

**Cons:**
- ❌ Requires API key for new users
- ❌ Not aligned with free tier strategy
- ❌ Breaking change (again)

**Mitigation:**
- Keep copilot as option 2
- Improve onboarding flow to switch providers
- Document copilot benefits

### Option C: Use "none" or "demo" as default

**Pros:**
- ✅ No authentication required
- ✅ Fast startup
- ✅ Clear user intent needed to select provider

**Cons:**
- ❌ Server can't do anything without provider
- ❌ Confusing for users
- ❌ Need custom "demo" mode implementation

### Option D: Smart default based on environment

**Logic:**
```rust
fn default_provider() -> String {
    // In test environment, default to anthropic
    if cfg!(test) || std::env::var("CI").is_ok() {
        "anthropic".to_string()
    } else {
        // In production, default to copilot
        "copilot".to_string()
    }
}
```

**Pros:**
- ✅ Best of both worlds
- ✅ Tests work out-of-box
- ✅ Production uses copilot

**Cons:**
- ❌ Different behavior in test vs prod
- ❌ Could hide bugs
- ❌ Compilation conditional

## Recommendations

### Short-term (v0.2.6) - **RECOMMENDED**

**Revert to "anthropic" as default provider**

**Rationale:**
1. Testing is critical for code quality
2. CI must pass reliably
3. v0.2.6 already has breaking changes, add one more
4. Can migrate to copilot in v0.3.0 with proper planning

**Implementation:**
```rust
fn default_provider() -> String {
    "anthropic".to_string()
}
```

**Migration path:**
- Update CHANGELOG with breaking change
- Add migration guide: "Set `provider = "copilot"` explicitly"
- Plan proper copilot testing infrastructure for v0.3.0

### Long-term (v0.3.0+)

**Implement Option D with testing infrastructure**

**Phase 1: Testing Infrastructure**
1. Create mock Copilot OAuth2 flow for tests
2. Add test helper: `create_test_app_with_provider("copilot")`
3. Document copilot testing strategies
4. Add integration tests for copilot flow

**Phase 2: Smart Defaults**
1. Use "anthropic" in test environments
2. Use "copilot" in production builds
3. Clear documentation about behavior differences
4. Add startup warnings if no provider configured

**Phase 3: Migration**
1. Update documentation
2. Add migration guide
3. Provide copilot setup wizard
4. Deprecate implicit anthropic usage

## Test Strategy Comparison

| Provider | Unit Tests | Integration Tests | CI | Local Dev |
|----------|-----------|-------------------|-----|-----------|
| copilot | ❌ Hard | ⚠️ Requires creds | ❌ Fails | ⚠️ Needs auth |
| anthropic | ✅ Easy | ✅ Easy | ✅ Works | ✅ Works |
| smart default | ✅ Easy | ✅ Easy | ✅ Works | ✅ Works |

## Action Items

### Immediate (This PR)

- [ ] **Decision**: Keep or revert default provider
- [ ] **If revert**: Update `default_provider()` function
- [ ] **If revert**: Update CHANGELOG with breaking change
- [ ] **If keep**: Update all tests to override provider
- [ ] **If keep**: Add copilot mock infrastructure

### Next Sprint

- [ ] Add comprehensive provider testing guide
- [ ] Create copilot test helpers
- [ ] Improve test isolation
- [ ] Add provider validation

### v0.3.0

- [ ] Implement smart defaults (Option D)
- [ ] Create copilot onboarding wizard
- [ ] Add provider health checks
- [ ] Complete migration documentation

## Recommendation

**I recommend reverting to "anthropic" as default for v0.2.6**

**Reasons:**
1. ✅ Unblocks CI
2. ✅ Improves test reliability
3. ✅ Better developer experience
4. ✅ Easier documentation
5. ✅ Can properly plan copilot migration for v0.3.0

The breaking change is acceptable because:
- v0.2.6 already has breaking changes (unified config)
- Users should explicitly set provider anyway
- Better to break now than in production

**Migration for users:**
```json
{
  "provider": "copilot"  // Add this if you want copilot
}
```

This is clear and explicit, which is better than implicit behavior.
