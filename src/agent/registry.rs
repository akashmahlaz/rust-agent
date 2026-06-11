//! In-memory registry of currently-running runs. Live runs broadcast events;
//! reconnecting clients can replay from Postgres and tail from here.

use dashmap::DashMap;
use std::sync::Arc;

use super::{runner::RunHandle, types::RunId};

#[derive(Clone, Default)]
pub struct AgentRegistry {
    inner: Arc<DashMap<RunId, RunHandle>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, handle: RunHandle) {
        self.inner.insert(handle.run_id, handle);
    }

    pub fn get(&self, run_id: &RunId) -> Option<RunHandle> {
        self.inner.get(run_id).map(|entry| entry.clone())
    }

    pub fn remove(&self, run_id: &RunId) {
        self.inner.remove(run_id);
    }
}
