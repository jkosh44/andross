//! [`fjall`] based storage for Raft logs and state.
//!
//! This module provides an implementation of the `LogStorage` trait that persists
//! Raft entries and hard state to a [`fjall`] database.

use crate::Result;
use crate::log_storage::{Entry, HardState, LogStorage};
use fjall::UserValue;
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

        // Load hard state from disk if it exists
        let mut raft_state = RaftState::default();
        if let (Some(commit), Some(term), Some(vote)) = (
            hard_state_keyspace.get(COMMIT_KEY)?,
            hard_state_keyspace.get(TERM_KEY)?,
            hard_state_keyspace.get(VOTE_KEY)?,
        ) {
            let commit_bytes = commit
                .as_ref()
                .try_into()
                .map_err(|_| "invalid commit value in hard state")?;
            let term_bytes = term
                .as_ref()
                .try_into()
                .map_err(|_| "invalid term value in hard state")?;
            let vote_bytes = vote
                .as_ref()
                .try_into()
                .map_err(|_| "invalid vote value in hard state")?;

            raft_state.hard_state.commit = u64::from_le_bytes(commit_bytes);
            raft_state.hard_state.term = u64::from_le_bytes(term_bytes);
            raft_state.hard_state.vote = u64::from_le_bytes(vote_bytes);
        }

        Ok(Self {
            raft_state,
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
        if low < self.first_log_index {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if high > self.last_log_index + 1 {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }

        let max_size = max_size.into();

        // eprintln!(
        //     "ENTRIES low: {}, high: {}, max_size: {:?}, self.last_index(): {}",
        //     low,
        //     high,
        //     max_size,
        //     self.last_index()?
        // );

        let size = std::cmp::min(high - low, max_size.unwrap_or(u64::MAX));
        let high = low + size;
        let low = raft_index_to_key(low);
        let high = raft_index_to_key(high);

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

        // eprintln!("ENTRIES: {entries:?}");

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            return Ok(0);
        }
        if idx < self.first_log_index {
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        let entry = self
            .entry(idx)?
            .ok_or(raft::Error::Store(StorageError::Unavailable))?;
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
        if entries.is_empty() {
            return Ok(());
        }

        assert!(
            self.first_log_index <= entries[0].index,
            "overwrite compacted raft logs, compacted: {}, append: {}",
            self.first_log_index - 1,
            entries[0].index,
        );
        assert!(
            self.last_log_index + 1 >= entries[0].index,
            "raft logs should be continuous, last index: {}, new appended: {}",
            self.last_log_index,
            entries[0].index,
        );

        let mut batch = self.db.batch();
        // Remove all entries overwritten by the new entries.
        for index in entries[0].index..=self.last_log_index {
            batch.remove(&self.log_keyspace, raft_index_to_key(index));
        }
        for entry in entries {
            let entry: Entry = entry.into();
            batch.insert(&self.log_keyspace, raft_index_to_key(entry.index), entry);
        }
        batch.commit().map_err(fjall_error_to_raft_error)?;

        self.last_log_index = entries.last().expect("empty entries return early").index;

        assert!(
            self.raft_state.hard_state.commit <= self.last_log_index,
            "commit index is invalid, commit: {}, last_log_index: {}",
            self.raft_state.hard_state.commit,
            self.last_log_index
        );

        Ok(())
    }

    fn set_hard_state(&mut self, hard_state: raft::prelude::HardState) -> raft::Result<()> {
        self.raft_state.hard_state = hard_state;
        let HardState { commit, term, vote } = (&self.raft_state.hard_state).into();
        let mut batch = self.db.batch();
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

fn raft_index_to_key(index: u64) -> [u8; size_of::<u64>()] {
    // Use big endian so that the lexicographical ordering of keys is consistent with the numeric
    // ordering of indices.
    index.to_be_bytes()
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use protobuf::{CachedSize, UnknownFields};
    use raft::Storage;
    use raft::prelude::EntryType;
    use tempfile::TempDir;

    #[test]
    fn test_fjall_storage_orders_entries_by_numeric_index() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path()).open().unwrap();
        let log_keyspace = db
            .keyspace("log", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let hard_state_keyspace = db
            .keyspace("hard_state", fjall::KeyspaceCreateOptions::default)
            .unwrap();

        let mut storage = FjallStorage::new(db.clone(), log_keyspace, hard_state_keyspace).unwrap();

        let make_entry = |index: u64, term: u64| raft::prelude::Entry {
            entry_type: EntryType::EntryNormal,
            term,
            index,
            data: Bytes::from_static(b"payload"),
            context: Bytes::new(),
            sync_log: false,
            unknown_fields: UnknownFields::default(),
            cached_size: CachedSize::default(),
        };

        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];

        storage.append(&entries).unwrap();

        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 3);

        let fetched = storage
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();

        let fetched_indexes: Vec<_> = fetched.into_iter().map(|e| e.index).collect();
        assert_eq!(fetched_indexes, vec![1, 2, 3]);

        drop(storage);

        let log_keyspace = db
            .keyspace("log", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let hard_state_keyspace = db
            .keyspace("hard_state", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let storage = FjallStorage::new(db, log_keyspace, hard_state_keyspace).unwrap();

        let fetched = storage
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();

        let fetched_indexes: Vec<_> = fetched.into_iter().map(|e| e.index).collect();
        assert_eq!(fetched_indexes, vec![1, 2, 3]);
    }

    #[test]
    fn test_fjall_storage_append_truncates_log_at_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path()).open().unwrap();
        let log_keyspace = db
            .keyspace("log", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let hard_state_keyspace = db
            .keyspace("hard_state", fjall::KeyspaceCreateOptions::default)
            .unwrap();

        let mut storage = FjallStorage::new(db, log_keyspace, hard_state_keyspace).unwrap();

        let make_entry = |index: u64, term: u64| raft::prelude::Entry {
            entry_type: EntryType::EntryNormal,
            term,
            index,
            data: Bytes::from_static(b"payload"),
            context: Bytes::new(),
            sync_log: false,
            unknown_fields: UnknownFields::default(),
            cached_size: CachedSize::default(),
        };

        // Append 1, 2, 3
        storage
            .append(&[make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)])
            .unwrap();
        assert_eq!(storage.last_index().unwrap(), 3);

        // Overwrite at index 2 with new entry at term 2
        storage.append(&[make_entry(2, 2)]).unwrap();
        assert_eq!(storage.last_index().unwrap(), 2);

        let entries = storage
            .entries(1, 3, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[0].term, 1);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[1].term, 2);

        // Ensure 3 is gone
        assert!(storage.entry(3).unwrap().is_none());
    }

    #[test]
    fn test_fjall_storage_append_truncates_log_at_overwrite_with_offset() {
        let temp_dir = TempDir::new().unwrap();
        let db = fjall::Database::builder(temp_dir.path()).open().unwrap();
        let log_keyspace = db
            .keyspace("log", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let hard_state_keyspace = db
            .keyspace("hard_state", fjall::KeyspaceCreateOptions::default)
            .unwrap();

        let mut storage = FjallStorage::new(db, log_keyspace, hard_state_keyspace).unwrap();
        // Force first_log_index to 10
        storage.first_log_index = 10;
        storage.last_log_index = 9;

        let make_entry = |index: u64, term: u64| raft::prelude::Entry {
            entry_type: EntryType::EntryNormal,
            term,
            index,
            data: Bytes::from_static(b"payload"),
            context: Bytes::new(),
            sync_log: false,
            unknown_fields: UnknownFields::default(),
            cached_size: CachedSize::default(),
        };

        // Pretend we have entries 10, 11, 12
        // We need to set first_log_index manually or by compacting,
        // but new() initializes first_log_index from the first key in log_keyspace.

        storage
            .append(&[make_entry(10, 1), make_entry(11, 1), make_entry(12, 1)])
            .unwrap();
        assert_eq!(storage.first_index().unwrap(), 10);
        assert_eq!(storage.last_index().unwrap(), 12);

        // Overwrite at index 11
        storage.append(&[make_entry(11, 2)]).unwrap();
        assert_eq!(storage.last_index().unwrap(), 11);

        let entries = storage
            .entries(10, 12, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 10);
        assert_eq!(entries[1].index, 11);
        assert_eq!(entries[1].term, 2);

        assert!(storage.entry(12).unwrap().is_none());
    }
}
