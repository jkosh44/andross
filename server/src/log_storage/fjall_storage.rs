//! [`fjall`] based storage for Raft logs and state.
//!
//! This module provides an implementation of the `LogStorage` trait that persists
//! Raft entries and hard state to a [`fjall`] database.

use crate::Result;
use crate::log_storage::{Entry, HardState, LogStorage};
use crate::util::usize_to_u64;
use fjall::{PersistMode, UserValue};
use raft::eraftpb::{ConfState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};

const COMMIT_KEY: &[u8] = b"c";
const TERM_KEY: &[u8] = b"t";
const VOTE_KEY: &[u8] = b"v";

/// Storage implementation that persists logs to Fjall.
pub struct FjallStorage {
    /// Cached Raft state.
    raft_state: RaftState,
    /// The index of the first entry in the storage.
    first_log_index: u64,
    /// The index of the last entry in the storage.
    last_log_index: u64,
    /// [`fjall`] database.
    db: fjall::Database,
    /// [`fjall`] keyspace containing log entries.
    log_keyspace: fjall::Keyspace,
    /// [`fjall`] keyspace containing hard state.
    hard_state_keyspace: fjall::Keyspace,
}

impl FjallStorage {
    /// Creates a new [`FjallStorage`] using the provided database and keyspaces.
    ///
    /// # Errors
    ///
    /// Returns an error if [`fjall`] returns an error when reading from the database.
    pub fn new(
        db: fjall::Database,
        log_keyspace: fjall::Keyspace,
        hard_state_keyspace: fjall::Keyspace,
    ) -> Result<Self> {
        let first_log_index = match log_keyspace.first_key_value() {
            Some(key_value) => {
                let entry: Entry = key_value.value()?.try_into()?;
                entry.index
            }
            None => 1,
        };
        let last_log_index = match log_keyspace.last_key_value() {
            Some(key_value) => {
                let entry: Entry = key_value.value()?.try_into()?;
                entry.index
            }
            None => 0,
        };

        Ok(Self {
            raft_state: RaftState::default(),
            first_log_index,
            last_log_index,
            db,
            log_keyspace,
            hard_state_keyspace,
        })
    }

    fn entry(&self, idx: u64) -> raft::Result<Option<raft::prelude::Entry>> {
        if idx > self.last_index()? {
            return Ok(None);
        }
        let entries = self.entries(idx, idx + 1, None, GetEntriesContext::empty(false))?;
        assert!(
            entries.len() <= 1,
            "unexpected number of entries: {entries:?}"
        );
        let entry = entries.into_iter().next();
        Ok(entry)
    }
}

impl Storage for FjallStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.raft_state.clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        assert!(low <= high, "low {low} is larger than high {high}");
        assert!(high <= self.last_index()? + 1);

        let size = std::cmp::min(high - low, max_size.into().unwrap_or(u64::MAX));
        let high = low + size;
        let low = low.to_le_bytes();
        let high = high.to_le_bytes();

        let entries = self
            .log_keyspace
            .range(low..high)
            .map(|entry| {
                let value = entry.value().map_err(fjall_error_to_raft_error)?;
                let entry: Entry = value
                    .try_into()
                    .map_err(|e| raft::Error::Store(StorageError::Other(e)))?;
                let entry: raft::prelude::Entry = entry
                    .try_into()
                    .map_err(|e: String| raft::Error::Store(StorageError::Other(e.into())))?;
                Ok(entry)
            })
            .collect::<raft::Result<_>>()?;
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            return Ok(0);
        }

        let entry = self.entry(idx)?.expect("invalid index");
        Ok(entry.term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_log_index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_log_index)
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        unimplemented!("snapshots are not yet supported");
    }
}

impl LogStorage for FjallStorage {
    fn set_conf_state(&mut self, conf_state: ConfState) {
        self.raft_state.conf_state = conf_state;
    }

    fn append(&mut self, entries: &[raft::eraftpb::Entry]) -> raft::Result<()> {
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        for entry in entries {
            let entry: Entry = entry.into();
            batch.insert(&self.log_keyspace, entry.index.to_le_bytes(), entry);
        }
        batch.commit().map_err(fjall_error_to_raft_error)?;

        self.last_log_index += usize_to_u64(entries.len());
        Ok(())
    }

    fn set_hard_state(&mut self, hard_state: raft::prelude::HardState) -> raft::Result<()> {
        self.raft_state.hard_state = hard_state;
        let HardState { commit, term, vote } = (&self.raft_state.hard_state).into();
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&self.hard_state_keyspace, COMMIT_KEY, commit.to_le_bytes());
        batch.insert(&self.hard_state_keyspace, TERM_KEY, term.to_le_bytes());
        batch.insert(&self.hard_state_keyspace, VOTE_KEY, vote.to_le_bytes());
        batch.commit().map_err(fjall_error_to_raft_error)?;
        Ok(())
    }

    fn set_commit_index(&mut self, commit_index: u64) -> raft::Result<()> {
        self.raft_state.hard_state.commit = commit_index;
        self.hard_state_keyspace
            .insert(COMMIT_KEY, commit_index.to_le_bytes())
            .map_err(fjall_error_to_raft_error)?;
        Ok(())
    }
}

impl TryFrom<UserValue> for Entry {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: UserValue) -> std::result::Result<Self, Self::Error> {
        let entry = Entry::from_bytes(value.as_ref())?;
        Ok(entry)
    }
}

impl From<Entry> for UserValue {
    fn from(entry: Entry) -> Self {
        UserValue::new(&entry.to_bytes())
    }
}
fn fjall_error_to_raft_error(e: fjall::Error) -> raft::Error {
    match e {
        fjall::Error::Io(e) => raft::Error::Io(e),
        e => raft::Error::Store(StorageError::Other(e.into())),
    }
}
