//! Log storage abstractions for Raft.
//!
//! This module defines the [`LogStorage`] trait which extends the base Raft storage
//! with initialization and update methods.

mod file_storage;

use crate::Result;
#[cfg(test)]
use crate::util::u64_to_usize;
use crate::util::usize_to_u64;
use bytes::Bytes;
pub use file_storage::FileStorage;
#[cfg(test)]
use proptest::prelude::{Strategy, any};
#[cfg(test)]
use proptest_derive::Arbitrary;
use protobuf::{CachedSize, ProtobufEnum, UnknownFields};
use raft::eraftpb::EntryType;
use raft::prelude::ConfState;
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
    fn append(&mut self, entries: &[raft::prelude::Entry]) -> raft::Result<()>;

    /// Sets the hard state.
    ///
    /// # Errors
    ///
    /// Returns an error if the hard state could not be updated.
    fn set_hard_state(&mut self, hard_state: raft::prelude::HardState) -> io::Result<()>;

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

    fn append(&mut self, entries: &[raft::prelude::Entry]) -> raft::Result<()> {
        self.wl().append(entries)
    }

    fn set_hard_state(&mut self, hard_state: raft::prelude::HardState) -> io::Result<()> {
        self.wl().set_hardstate(hard_state);
        Ok(())
    }

    fn set_commit_index(&mut self, commit_index: u64) -> io::Result<()> {
        self.wl().mut_hard_state().set_commit(commit_index);
        Ok(())
    }
}

// Serialized versions of Raft structs.

/// Size in bytes of the hard state.
const HARD_STATE_SIZE: usize = size_of::<u64>() * 3;

/// Represents the Raft hard state to be persisted.
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct HardState {
    commit: u64,
    term: u64,
    vote: u64,
}

impl HardState {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HARD_STATE_SIZE);
        bytes.extend_from_slice(&self.commit.to_le_bytes());
        bytes.extend_from_slice(&self.term.to_le_bytes());
        bytes.extend_from_slice(&self.vote.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (commit_bytes, rest_bytes) = bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid hard state: {bytes:?}"))?;
        let (term_bytes, vote_bytes) = rest_bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid hard state: {rest_bytes:?}"))?;
        let vote_bytes: [u8; size_of::<u64>()] = vote_bytes
            .try_into()
            .map_err(|_| format!("invalid hard state: {rest_bytes:?}"))?;

        let commit = u64::from_le_bytes(*commit_bytes);
        let term = u64::from_le_bytes(*term_bytes);
        let vote = u64::from_le_bytes(vote_bytes);

        Ok(Self { commit, term, vote })
    }
}

impl From<raft::prelude::HardState> for HardState {
    fn from(hard_state: raft::eraftpb::HardState) -> Self {
        Self {
            commit: hard_state.commit,
            term: hard_state.term,
            vote: hard_state.vote,
        }
    }
}

impl From<&raft::prelude::HardState> for HardState {
    fn from(hard_state: &raft::eraftpb::HardState) -> Self {
        Self {
            commit: hard_state.commit,
            term: hard_state.term,
            vote: hard_state.vote,
        }
    }
}

impl From<HardState> for raft::prelude::HardState {
    fn from(HardState { commit, term, vote }: HardState) -> Self {
        let mut hard_state = raft::prelude::HardState::new();
        hard_state.commit = commit;
        hard_state.term = term;
        hard_state.vote = vote;
        hard_state
    }
}

/// Represents a single Raft log entry.
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    #[expect(clippy::struct_field_names)]
    entry_type: i32,
    term: u64,
    index: u64,
    #[cfg_attr(
        test,
        proptest(strategy = "any::<Vec<u8>>().prop_map(bytes::Bytes::from)")
    )]
    data: Bytes,
    #[cfg_attr(
        test,
        proptest(strategy = "any::<Vec<u8>>().prop_map(bytes::Bytes::from)")
    )]
    context: Bytes,
}

impl Entry {
    fn header(&self) -> EntryHeader {
        EntryHeader {
            entry_type: self.entry_type,
            term: self.term,
            index: self.index,
            data_len: usize_to_u64(self.data.len()),
            context_len: usize_to_u64(self.context.len()),
        }
    }

    #[cfg(test)]
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(ENTRY_HEADER_SIZE + self.data.len() + self.context.len());
        let header = self.header();
        header.bytes_to(&mut bytes);
        bytes.extend_from_slice(&self.data);
        bytes.extend_from_slice(&self.context);
        bytes
    }

    #[cfg(test)]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (header_bytes, rest_bytes) = (&bytes[..ENTRY_HEADER_SIZE], &bytes[ENTRY_HEADER_SIZE..]);
        let EntryHeader {
            entry_type,
            term,
            index,
            data_len,
            context_len,
        } = EntryHeader::from_bytes(header_bytes)?;
        let data_len = u64_to_usize(data_len);
        let context_len = u64_to_usize(context_len);
        if rest_bytes.len() < data_len + context_len {
            return Err(format!(
                "entry len is too short: {} < {} + {}",
                rest_bytes.len(),
                data_len,
                context_len
            )
            .into());
        }
        let data = Bytes::copy_from_slice(&rest_bytes[..data_len]);
        let rest_bytes = &rest_bytes[data_len..];
        let context = Bytes::copy_from_slice(&rest_bytes[..context_len]);

        Ok(Self {
            entry_type,
            term,
            index,
            data,
            context,
        })
    }
}

impl From<raft::prelude::Entry> for Entry {
    fn from(entry: raft::eraftpb::Entry) -> Self {
        (&entry).into()
    }
}

impl From<&raft::prelude::Entry> for Entry {
    fn from(entry: &raft::eraftpb::Entry) -> Self {
        Self {
            entry_type: entry.entry_type.value(),
            term: entry.term,
            index: entry.index,
            data: entry.data.clone(),
            context: entry.context.clone(),
        }
    }
}

impl TryFrom<Entry> for raft::prelude::Entry {
    type Error = String;

    fn try_from(entry: Entry) -> std::result::Result<Self, Self::Error> {
        (&entry).try_into()
    }
}

impl TryFrom<&Entry> for raft::prelude::Entry {
    type Error = String;

    fn try_from(entry: &Entry) -> std::result::Result<Self, Self::Error> {
        Ok(raft::prelude::Entry {
            entry_type: EntryType::from_i32(entry.entry_type)
                .ok_or_else(|| format!("invalid entry type: {}", entry.entry_type))?,
            term: entry.term,
            index: entry.index,
            data: entry.data.clone(),
            context: entry.context.clone(),
            sync_log: false,
            unknown_fields: UnknownFields::default(),
            cached_size: CachedSize::default(),
        })
    }
}

/// Size in bytes of the header of a log entry.
const ENTRY_HEADER_SIZE: usize = size_of::<i32>() + size_of::<u64>() * 4;

/// Header for a log entry stored on disk.
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryHeader {
    entry_type: i32,
    term: u64,
    index: u64,
    data_len: u64,
    context_len: u64,
}

impl EntryHeader {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ENTRY_HEADER_SIZE);
        self.bytes_to(&mut bytes);
        bytes
    }

    fn bytes_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.entry_type.to_le_bytes());
        bytes.extend_from_slice(&self.term.to_le_bytes());
        bytes.extend_from_slice(&self.index.to_le_bytes());
        bytes.extend_from_slice(&self.data_len.to_le_bytes());
        bytes.extend_from_slice(&self.context_len.to_le_bytes());
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (entry_type_bytes, rest_bytes) = bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid log entry header: {bytes:?}"))?;
        let (term_bytes, rest_bytes) = rest_bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid log entry header: {bytes:?}"))?;
        let (index_bytes, rest_bytes) = rest_bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid log entry header: {bytes:?}"))?;
        let (data_len_bytes, context_len_bytes) = rest_bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid log entry header: {bytes:?}"))?;
        let context_len_bytes: [u8; size_of::<u64>()] = context_len_bytes
            .try_into()
            .map_err(|_| format!("invalid log file entry header: {bytes:?}"))?;

        let entry_type = i32::from_le_bytes(*entry_type_bytes);
        let term = u64::from_le_bytes(*term_bytes);
        let index = u64::from_le_bytes(*index_bytes);
        let data_len = u64::from_le_bytes(*data_len_bytes);
        let context_len = u64::from_le_bytes(context_len_bytes);

        Ok(Self {
            entry_type,
            term,
            index,
            data_len,
            context_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_hard_state_roundtrip(hs: HardState) {
            let bytes = hs.to_bytes();
            let hs2 = HardState::from_bytes(&bytes).unwrap();
            assert_eq!(hs, hs2);
        }

        #[test]
        fn test_entry_roundtrip(entry: Entry) {
            let bytes = entry.to_bytes();
            let entry2 = Entry::from_bytes(&bytes).unwrap();
            assert_eq!(entry, entry2);
        }

        #[test]
        fn test_entry_header_roundtrip(header: EntryHeader) {
            let mut bytes = Vec::new();
            header.bytes_to(&mut bytes);
            let bytes2 = header.to_bytes();
            assert_eq!(bytes, bytes2);
            let header2 = EntryHeader::from_bytes(&bytes).unwrap();
            assert_eq!(header, header2);
        }
    }
}
