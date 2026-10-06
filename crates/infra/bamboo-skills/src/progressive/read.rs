//! Independently owned selected content. Bytes are historical data, never a grant.
use crate::{SkillError, SkillResult};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SelectedLimits {
    pub bytes: usize,
    pub owners: usize,
    pub inflight: usize,
}
impl Default for SelectedLimits {
    fn default() -> Self {
        Self {
            bytes: 32 * 1024 * 1024,
            owners: 4,
            inflight: 1,
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct SelectedUsage {
    pub bytes: usize,
    pub owners: usize,
    pub inflight: usize,
}
#[derive(Debug)]
pub(crate) struct SelectedBudget {
    state: Arc<Mutex<SelectedUsage>>,
    limits: SelectedLimits,
}
impl Default for SelectedBudget {
    fn default() -> Self {
        Self::new(SelectedLimits::default())
    }
}
impl SelectedBudget {
    pub(crate) fn new(limits: SelectedLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(SelectedUsage::default())),
            limits,
        }
    }
    fn reserve(&self, bytes: usize, owner: bool, inflight: bool) -> SkillResult<SelectedCharge> {
        let mut state = self.state.lock().expect("selected Skill budget");
        if bytes > self.limits.bytes.saturating_sub(state.bytes)
            || usize::from(owner) > self.limits.owners.saturating_sub(state.owners)
            || usize::from(inflight) > self.limits.inflight.saturating_sub(state.inflight)
        {
            return Err(SkillError::Validation(
                "selected Skill capacity exhausted".into(),
            ));
        }
        state.bytes += bytes;
        state.owners += usize::from(owner);
        state.inflight += usize::from(inflight);
        Ok(SelectedCharge {
            state: self.state.clone(),
            bytes,
            owner,
            inflight,
        })
    }
    pub(crate) fn operation(&self) -> SkillResult<SelectedCharge> {
        self.reserve(0, false, true)
    }
    pub(crate) fn buffer(&self, bytes: usize, owner: bool) -> SkillResult<SelectedBuffer> {
        let mut charge = self.reserve(bytes, owner, false)?;
        let mut contents = Vec::new();
        contents.try_reserve_exact(bytes).map_err(|error| {
            SkillError::Validation(format!("selected allocation failed: {error}"))
        })?;
        // The actual capacity, including allocator rounding, is charged before IO/copy.
        if contents.capacity() > bytes {
            let extra = contents.capacity() - bytes;
            let mut state = self.state.lock().expect("selected Skill budget");
            if extra > self.limits.bytes.saturating_sub(state.bytes) {
                return Err(SkillError::Validation(
                    "selected allocation exceeds capacity".into(),
                ));
            }
            state.bytes += extra;
            charge.bytes += extra;
        }
        contents.resize(bytes, 0);
        Ok(SelectedBuffer { contents, charge })
    }
    #[cfg(test)]
    pub(crate) fn usage(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (state.bytes, state.owners, state.inflight)
    }
    #[cfg(test)]
    pub(crate) fn weak_state(&self) -> std::sync::Weak<Mutex<SelectedUsage>> {
        Arc::downgrade(&self.state)
    }
}

// Own only the ledger state. No manager/store/cache back-reference or Arc cycle.
#[derive(Debug)]
pub(crate) struct SelectedCharge {
    state: Arc<Mutex<SelectedUsage>>,
    bytes: usize,
    owner: bool,
    inflight: bool,
}
impl Drop for SelectedCharge {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("selected Skill budget");
        state.bytes -= self.bytes;
        state.owners -= usize::from(self.owner);
        state.inflight -= usize::from(self.inflight);
    }
}
#[derive(Debug)]
pub(crate) struct SelectedBuffer {
    pub(crate) contents: Vec<u8>,
    charge: SelectedCharge,
}
impl SelectedBuffer {
    pub(crate) fn snapshot(
        self,
        package: String,
        resource: String,
        identity: String,
    ) -> SkillResult<SelectedSkillSnapshot> {
        let contents = String::from_utf8(self.contents).map_err(|_| {
            SkillError::Validation("selected Skill file is not complete UTF-8".into())
        })?;
        Ok(SelectedSkillSnapshot {
            package,
            resource,
            identity,
            contents,
            _charge: self.charge,
        })
    }
}

/// Complete raw UTF-8 chosen file; clones of its Arc retain the byte/entry charge.
/// Contains owned locators and bytes only: no source handle or execution authority.
#[derive(Debug)]
pub struct SelectedSkillSnapshot {
    pub package: String,
    pub resource: String,
    pub identity: String,
    contents: String,
    _charge: SelectedCharge,
}
impl SelectedSkillSnapshot {
    pub fn contents(&self) -> &str {
        &self.contents
    }
    pub fn capacity(&self) -> usize {
        self.contents.capacity()
    }
}
