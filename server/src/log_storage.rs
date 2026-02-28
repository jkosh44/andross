//! Log storage abstractions for Raft.
//!
//! This module defines the [`LogStorage`] trait which extends the base Raft storage
//! with initialization and update methods.

mod file_storage;

pub use file_storage::FileStorage;
use raft::prelude::{ConfState, Entry, HardState};
use raft::storage::MemStorage;
use tokio::io;

/// An extension trait for [`raft::storage::Storage`].
pub trait LogStorage: raft::prelude::Storage {
    // TODO: Currently `ConfState` is static and can never change. When we support dynamic
    // membership, we'll need to add a way to update the `ConfState` and read it back from disk.
    /// Sets the [`ConfState`] of this log storage.
    fn set_conf_state(&mut self, conf_state: ConfState);

    /// Appends new entries to the log.
    ///
    /// # Errors
    ///
    /// Returns an error if the entries could not be appended.
    fn append(&mut self, entries: &[Entry]) -> raft::Result<()>;

    /// Sets the hard state.
    ///
    /// # Errors
    ///
    /// Returns an error if the hard state could not be updated.
    fn set_hard_state(&mut self, hard_state: HardState) -> io::Result<()>;

    /// Sets the commit index.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit index could not be updated.
    fn set_commit_index(&mut self, commit_index: u64) -> io::Result<()>;
}

impl LogStorage for MemStorage {
    fn set_conf_state(&mut self, conf_state: ConfState) {
        self.initialize_with_conf_state(conf_state);
    }

    fn append(&mut self, entries: &[Entry]) -> raft::Result<()> {
        self.wl().append(entries)
    }

    fn set_hard_state(&mut self, hard_state: HardState) -> io::Result<()> {
        self.wl().set_hardstate(hard_state);
        Ok(())
    }

    fn set_commit_index(&mut self, commit_index: u64) -> io::Result<()> {
        self.wl().mut_hard_state().set_commit(commit_index);
        Ok(())
    }
}
