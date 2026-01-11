//! Storage abstractions for Raft.
//!
//! This module defines the `Storage` trait which extends the base Raft storage
//! with asynchronous initialization and update methods.

use raft::prelude::{ConfState, Entry, HardState};
use raft::storage::MemStorage;

/// An extension trait for [`raft::storage::Storage`].
#[tonic::async_trait]
pub trait Storage: raft::prelude::Storage {
    /// Initializes a new storage instance.
    async fn init(conf_state: ConfState) -> Self;

    /// Appends new entries to the log.
    async fn append(&self, entries: &[Entry]) -> raft::Result<()>;

    /// Sets the hard state.
    async fn set_hard_state(&self, hard_state: HardState);

    /// Sets the commit index.
    async fn set_commit_index(&self, commit_index: u64);
}

#[tonic::async_trait]
impl Storage for MemStorage {
    async fn init(conf_state: ConfState) -> Self {
        let storage = MemStorage::new();
        storage.initialize_with_conf_state(conf_state);
        storage
    }

    async fn append(&self, entries: &[Entry]) -> raft::Result<()> {
        self.wl().append(entries)
    }

    async fn set_hard_state(&self, hard_state: HardState) {
        self.wl().set_hardstate(hard_state);
    }

    async fn set_commit_index(&self, commit_index: u64) {
        self.wl().mut_hard_state().set_commit(commit_index);
    }
}
