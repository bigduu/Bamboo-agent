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
            limits: self.limits,
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
    limits: SelectedLimits,
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

// Copyright 2025 OpenAI. Licensed under Apache-2.0.
// UTF-8 paging adapted from Codex ext/skills/src/tools/read.rs at
// 7f892275e31002f0422477c6219189284560e689. Cache/charges are Bamboo-authored.

/// One ephemeral admission. Historical data alone never authorizes a read.
#[derive(Debug)]
pub struct CachedSkillRead {
    pub snapshot: Arc<SelectedSkillSnapshot>,
    pub identity: String,
    instance: u64,
    generation: u64,
}
impl CachedSkillRead {
    pub fn cursor(&self, offset: usize) -> String {
        format!("{}:{}:{offset}", self.instance, self.generation)
    }
}

/// One resident chosen file; active borrowers retain the original shared charge.
/// No source handles, manager references, weak key index or persistent state.
#[derive(Debug)]
pub struct SelectedSkillReadCache {
    instance: u64,
    state: Mutex<(u64, Option<Arc<CachedSkillRead>>)>,
}
impl Default for SelectedSkillReadCache {
    fn default() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            instance: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            state: Mutex::new((0, None)),
        }
    }
}
impl SelectedSkillReadCache {
    pub fn clear(&self) {
        let removed = self.state.lock().expect("chosen cache").1.take();
        drop(removed); // Drop charges outside the cache mutex.
    }
    pub fn admit(
        &self,
        snapshot: Arc<SelectedSkillSnapshot>,
        identity: String,
    ) -> Arc<CachedSkillRead> {
        let (entry, removed) = {
            let mut state = self.state.lock().expect("chosen cache");
            state.0 = state
                .0
                .checked_add(1)
                .expect("chosen cache generation exhausted");
            let entry = Arc::new(CachedSkillRead {
                snapshot,
                identity,
                instance: self.instance,
                generation: state.0,
            });
            let removed = state.1.replace(entry.clone());
            (entry, removed)
        };
        drop(removed);
        entry
    }
    pub fn lookup(&self, cursor: &str) -> SkillResult<(Arc<CachedSkillRead>, usize)> {
        let invalid = || SkillError::Validation("skills_read cursor is stale or invalid".into());
        if cursor.len() > 128 {
            return Err(invalid());
        }
        if cursor
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && byte != b':')
        {
            return Err(invalid());
        }
        let mut parts = cursor.split(':');
        let instance = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        let generation = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        let offset = parts
            .next()
            .ok_or_else(invalid)?
            .parse::<usize>()
            .map_err(|_| invalid())?;
        if parts.next().is_some() {
            return Err(invalid());
        }
        let state = self.state.lock().expect("chosen cache");
        let entry = state.1.as_ref().ok_or_else(invalid)?;
        if instance != self.instance
            || generation != entry.generation
            || offset > entry.snapshot.contents.len()
            || !entry.snapshot.contents.is_char_boundary(offset)
        {
            return Err(invalid());
        }
        Ok((entry.clone(), offset))
    }
    /// Final synchronous membership is the current read's linearization point.
    /// Eviction after it invalidates the next continuation, not the completed page.
    pub fn contains(&self, entry: &Arc<CachedSkillRead>) -> bool {
        self.state
            .lock()
            .expect("chosen cache")
            .1
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, entry))
    }
}

/// Encoded final page. Its allocation stays charged through host final checks.
#[derive(Debug)]
pub struct SelectedSkillPage {
    text: String,
    _charge: SelectedCharge,
}
impl SelectedSkillPage {
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn into_text(self) -> String {
        self.text
    }
}
impl SelectedSkillSnapshot {
    /// Borrow candidates; copy only the final encoded page into precharged space.
    /// `measure` must include the complete host/provider envelope, `encode` the
    /// inner response. The caller's trusted cap bounds every probe's raw span.
    pub fn page_response<M, E>(
        &self,
        start: usize,
        budget: usize,
        measure: M,
        encode: E,
    ) -> SkillResult<SelectedSkillPage>
    where
        M: Fn(&str, Option<usize>) -> SkillResult<usize>,
        E: Fn(&str, Option<usize>, &mut dyn std::io::Write) -> SkillResult<()>,
    {
        let budget = budget.min(512 * 1024);
        let ledger = SelectedBudget {
            state: self._charge.state.clone(),
            limits: self._charge.limits,
        };
        let _operation = ledger.operation()?;
        let _scratch = ledger.buffer(256, false)?; // bounded cursor/serializer scratch before probing
        let invalid = || {
            SkillError::Validation(
                "skills_read response budget cannot fit contents/envelope".into(),
            )
        };
        if start > self.contents.len() || !self.contents.is_char_boundary(start) {
            return Err(invalid());
        }
        let len = self.contents.len();
        let end = if len - start <= budget && measure(&self.contents[start..], None)? <= budget {
            len
        } else {
            let mut lower = start;
            let mut upper = start.saturating_add(budget).min(len);
            while !self.contents.is_char_boundary(upper) {
                upper -= 1;
            }
            let mut best = None;
            while lower < upper {
                let mut end = lower + (upper - lower) / 2 + 1;
                while !self.contents.is_char_boundary(end) {
                    end += 1;
                }
                let next = (end < len).then_some(end);
                if measure(&self.contents[start..end], next)? <= budget {
                    lower = end;
                    best = Some(end);
                } else {
                    upper = end - 1;
                    while !self.contents.is_char_boundary(upper) {
                        upper -= 1;
                    }
                }
            }
            best.ok_or_else(invalid)?
        };
        let mut buffer = ledger.buffer(budget, false)?;
        let mut writer = std::io::Cursor::new(buffer.contents.as_mut_slice());
        encode(
            &self.contents[start..end],
            (end < len).then_some(end),
            &mut writer,
        )?;
        let written = usize::try_from(writer.position()).map_err(|_| invalid())?;
        buffer.contents.truncate(written);
        let text = String::from_utf8(buffer.contents).map_err(|_| invalid())?;
        Ok(SelectedSkillPage {
            text,
            _charge: buffer.charge,
        })
    }
}
