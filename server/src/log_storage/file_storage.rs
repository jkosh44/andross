//! File-based storage for Raft logs and state.
//!
//! This module provides an implementation of the `LogStorage` trait that persists
//! Raft entries and hard state to the local file system.
//!
//! # Storage Layout
//!
//! The storage is organized into a root directory containing the following:
//!
//! - `hardstate`: A file containing the Raft hard state (commit index, term, and vote).
//! - `directory`: A file acting as an index for the log segments. It maps log file IDs to
//!   their starting Raft indices.
//! - `logs/`: A subdirectory containing the actual log segments.
//!   - `0`, `1`, `2`, ...: Log segment files named by their incremental ID.
//!
//! # File Formats
//!
//! ### Hard State (`hardstate`)
//! Fixed-size file (24 bytes) containing three little-endian `u64` values:
//! | Offset | Length | Description |
//! |--------|--------|-------------|
//! | 0      | 8      | Commit Index|
//! | 8      | 8      | Current Term|
//! | 16     | 8      | Vote        |
//!
//! ### Directory (`directory`)
//! A sequence of [`LogFileEntry`] records, each 16 bytes:
//! | Offset | Length | Description |
//! |--------|--------|-------------|
//! | 0      | 8      | Log File ID |
//! | 8      | 8      | Start Index |
//!
//! ### Log Segments (`logs/ID`)
//! A sequence of log entries. Each entry consists of a fixed-size header followed by raw data:
//!
//! **Header (28 bytes):**
//! | Offset | Length | Description  |
//! |--------|--------|--------------|
//! | 0      | 4      | Entry Type   |
//! | 4      | 8      | Term         |
//! | 12     | 8      | Index        |
//! | 20     | 8      | Data Length  |
//!
//! **Data:**
//! - Variable length payload as specified by `Data Length` in the header.

use crate::Result;
use crate::log_storage::LogStorage;
use crate::util::u64_to_usize;
use bytes::Bytes;
use protobuf::{CachedSize, ProtobufEnum, UnknownFields};
use raft::eraftpb::{ConfState, Snapshot};
use raft::prelude::EntryType;
use raft::{GetEntriesContext, RaftState};
use std::cmp::Ordering;
use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, io};

/// Filename containing Raft hard state.
const HARD_STATE_FILE_NAME: &str = "hardstate";
/// Filename containing log directory.
const DIRECTORY_FILE_NAME: &str = "directory";
/// Directory containing log files.
const LOG_PATH: &str = "logs";

/// Storage implementation that persists logs to files.
pub struct FileStorage {
    /// Cached Raft state.
    raft_state: RaftState,
    /// Directory of log files and their starting indices.
    /// This file is the source of truth, if a log file doesn't exist here, it should be deleted.
    directory: Directory,
    /// The index of the first entry in the storage.
    first_log_index: u64,
    /// The index of the last entry in the storage.
    last_log_index: u64,
    /// The ID of the current log file being written to.
    current_log_id: u64,
    /// Mapping of log index ranges to their respective terms.
    log_terms: Vec<(Range<u64>, u64)>,
    /// Path to the directory where log files are kept.
    path: PathBuf,
    /// Maximum size of a single log file in bytes before rolling over.
    max_log_file_size_bytes: usize,
    /// Cached log entries.
    // TODO: Page these out to disk.
    entries: Vec<Entry>,
}

impl FileStorage {
    /// Creates a new [`FileStorage`] at the specified path.
    ///
    /// If the path does not exist, it will be created.
    ///
    /// # Errors
    ///
    /// Returns an error if the file system returns an error or if there is a torn write in the log.
    pub async fn new(path: PathBuf, max_log_file_size_bytes: usize) -> Result<Self> {
        fs::create_dir_all(&path).await?;

        let mut storage = Self {
            raft_state: RaftState::default(),
            directory: Directory::new(),
            first_log_index: 1,
            last_log_index: 0,
            current_log_id: 0,
            log_terms: vec![(0..0, 0)],
            path,
            max_log_file_size_bytes,
            entries: Vec::new(),
        };

        fs::create_dir_all(&storage.logs_path()).await?;

        // Read hard state in from disk if it exists.
        match storage.read_hard_state().await? {
            Some(hard_state) => storage.raft_state.hard_state = hard_state.into(),
            None => {
                storage.persist_hard_state().await?;
            }
        }

        // Read the directory in from disk if it exists.
        let mut directory = storage.read_directory().await?;
        // If the directory is empty, add an entry for the first log file.
        if directory.entries.is_empty() {
            let mut directory_file = File::options()
                .create(true)
                .append(true)
                .open(storage.directory_path())
                .await?;
            let log_file_entry = LogFileEntry {
                id: 0,
                start_index: 1,
            };
            let log_file_entry_bytes = log_file_entry.to_bytes();
            directory.entries.push(log_file_entry);
            directory_file.write_all(&log_file_entry_bytes).await?;
            directory_file.flush().await?;
            directory_file.sync_all().await?;
        }
        storage.directory = directory;

        // Read in all log file metadata.
        let mut read_dir = fs::read_dir(&storage.logs_path()).await?;
        let mut log_files = Vec::new();
        while let Some(entry) = read_dir.next_entry().await? {
            let file_type = entry.file_type().await?;
            assert!(file_type.is_file(), "{file_type:?} is not a file");
            let log_id = entry
                .file_name()
                .to_str()
                .ok_or("Invalid log file name")?
                .parse::<u64>()
                .map_err(|e| format!("Invalid log file name: {e}"))?;
            log_files.push((log_id, entry));
        }
        log_files.sort_by_key(|(id, _entry)| *id);

        let mut directory_idx = 0;
        let mut log_file_idx = 0;

        // Delete orphaned log files. The directory entry was deleted, but the file was never
        // deleted.
        while let Some(directory_entry) = storage.directory.entries.get(directory_idx)
            && let Some((log_id, log_file)) = log_files.get(log_file_idx)
            && directory_entry.id > *log_id
        {
            fs::remove_file(log_file.path()).await?;
            log_file_idx += 1;
        }

        // Iterate through all log files that have directory entries.
        let mut first_index_set = false;
        while let Some(directory_entry) = storage.directory.entries.get(directory_idx)
            && let Some((log_id, _log_file)) = log_files.get(log_file_idx)
            && directory_entry.id == *log_id
        {
            // TODO: Handle torn write.
            let entries = storage.read_log_entries(*log_id).await.expect("torn write");
            for entry in entries {
                if !first_index_set {
                    storage.first_log_index = entry.index;
                    first_index_set = true;
                }
                storage.last_log_index = entry.index;
                storage.update_terms(&entry);
                storage.entries.push(entry);
            }
            storage.current_log_id = *log_id;
            log_file_idx += 1;
            directory_idx += 1;
        }
        assert_eq!(
            log_file_idx,
            log_files.len(),
            "directory entries are always created before log files"
        );

        // Create log files for any directory entries that don't have a corresponding log file.
        while let Some(directory_entry) = storage.directory.entries.get(directory_idx) {
            let path = storage.log_path(directory_entry.id);
            File::create(path).await?;
            storage.current_log_id = directory_entry.id;
            directory_idx += 1;
        }

        Ok(storage)
    }

    fn update_terms(&mut self, entry: &Entry) {
        let last_term = self
            .log_terms
            .last_mut()
            .expect("initialized with one entry");
        if last_term.1 == entry.term {
            assert!(
                entry.index >= last_term.0.end,
                "index is monotonically increasing; entry: {entry:?}, last_term: {last_term:?}",
            );
            last_term.0.end = entry.index + 1;
        } else {
            assert!(
                entry.term > last_term.1,
                "term is monotonically increasing; entry: {entry:?}, last_term: {last_term:?}",
            );
            self.log_terms
                .push((entry.index..entry.index + 1, entry.term));
        }
    }

    fn hard_state_path(&self) -> PathBuf {
        self.path.join(HARD_STATE_FILE_NAME)
    }

    fn directory_path(&self) -> PathBuf {
        self.path.join(DIRECTORY_FILE_NAME)
    }

    fn logs_path(&self) -> PathBuf {
        self.path.join(LOG_PATH)
    }

    fn log_path(&self, log_id: u64) -> PathBuf {
        self.logs_path().join(log_id.to_string())
    }

    async fn read_hard_state(&self) -> io::Result<Option<HardState>> {
        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.hard_state_path())
            .await?;

        // The process must have crashed when first writing to the hard state file,
        // before all the information was written. Therefore, we delete the file and
        // start over.
        let file_len = file.metadata().await?.len();
        if file_len != HARD_STATE_FILE_SIZE as u64 {
            assert_eq!(file_len, 0);
            return Ok(None);
        }

        let mut bytes = Vec::with_capacity(HARD_STATE_FILE_SIZE);
        file.read_to_end(&mut bytes).await?;

        let hard_state = HardState::from_bytes(&bytes).expect("file size checked");
        Ok(Some(hard_state))
    }

    async fn read_directory(&self) -> io::Result<Directory> {
        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory_path())
            .await?;

        // Truncate any torn writes.
        let mut file_len = file.metadata().await?.len();
        let torn_write_len = file_len % LOG_FILE_ENTRY_SIZE as u64;
        if torn_write_len > 0 {
            file_len -= torn_write_len;
            file.set_len(file_len).await?;
        }

        let mut bytes = Vec::with_capacity(u64_to_usize(file_len));
        file.read_to_end(&mut bytes).await?;

        let directory = Directory::from_bytes(&bytes).expect("file size checked");
        Ok(directory)
    }

    async fn read_log_entries(&self, log_id: u64) -> io::Result<Vec<Entry>> {
        let mut file = File::options()
            .read(true)
            .open(self.log_path(log_id))
            .await?;
        let mut entries = Vec::new();
        let mut bytes = [0; ENTRY_HEADER_SIZE];

        loop {
            let read = match file.read_exact(&mut bytes).await {
                Ok(read) => read,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            assert_eq!(read, ENTRY_HEADER_SIZE);
            // TODO: handle torn writes.
            let entry_header = EntryHeader::from_bytes(&bytes).expect("torn write");
            let mut data = vec![0; u64_to_usize(entry_header.data_len)];
            file.read_exact(&mut data).await?;
            let mut context = vec![0; u64_to_usize(entry_header.context_len)];
            file.read_exact(&mut context).await?;

            let entry = Entry {
                entry_type: entry_header.entry_type,
                term: entry_header.term,
                index: entry_header.index,
                data: data.into(),
                context: context.into(),
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn persist_hard_state(&self) -> io::Result<()> {
        let hard_state: HardState = (&self.raft_state.hard_state).into();
        let bytes = hard_state.to_bytes();

        // Write contents to a temporary file.
        let temp_file = tempfile::Builder::new().tempfile()?;
        let path = temp_file.path().to_path_buf();
        {
            let mut temp_file = File::options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&path)
                .await?;
            temp_file.write_all(&bytes).await?;
            temp_file.flush().await?;
            temp_file.sync_all().await?;
        }

        // Atomically rename the temporary file to the hard state file.
        let hard_state_path = self.hard_state_path();
        fs::rename(path, &hard_state_path).await?;

        // We need to fsync the parent after a rename.
        let parent_path = hard_state_path
            .parent()
            .expect("hard state file must have a parent");
        let parent_file = File::open(parent_path).await?;
        parent_file.sync_all().await?;
        Ok(())
    }
}

impl raft::prelude::Storage for FileStorage {
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
        let size = u64_to_usize(size);
        // TODO: Start reading from disk.
        let start_idx = self
            .entries
            .binary_search_by_key(&low, |entry| entry.index)
            .map_err(|_| raft::Error::Store(raft::StorageError::Compacted))?;
        Ok(self.entries[start_idx..(start_idx + size)]
            .iter()
            .map(|entry| entry.try_into().expect("invalid entry"))
            .collect())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            return Ok(0);
        }

        match self.log_terms.binary_search_by(|(idx_range, _)| {
            if idx_range.end <= idx {
                Ordering::Less
            } else if idx_range.start > idx {
                Ordering::Greater
            } else {
                assert!(idx_range.contains(&idx));
                Ordering::Equal
            }
        }) {
            Ok(term_idx) => Ok(self.log_terms[term_idx].1),
            Err(term_idx) => {
                if term_idx == 0 {
                    Err(raft::Error::Store(raft::StorageError::Compacted))
                } else {
                    Err(raft::Error::Store(raft::StorageError::Unavailable))
                }
            }
        }
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

#[tonic::async_trait]
impl LogStorage for FileStorage {
    async fn set_conf_state(&mut self, conf_state: ConfState) {
        self.raft_state.conf_state = conf_state;
    }

    async fn append(&mut self, entries: &[raft::prelude::Entry]) -> raft::Result<()> {
        let current_file_path = self.log_path(self.current_log_id);
        let mut file = File::options().append(true).open(current_file_path).await?;
        let mut directory_file = File::options()
            .create(true)
            .append(true)
            .open(self.directory_path())
            .await?;

        for entry in entries {
            assert_eq!(entry.index, self.last_log_index + 1);

            let entry: Entry = entry.into();
            let header_bytes = entry.header().to_bytes();
            let entry_len = header_bytes.len() + entry.data.len() + entry.context.len();
            let metadata = file.metadata().await?;

            // The current log file has grown to large so create a new one.
            if metadata.len() > 0
                && u64_to_usize(metadata.len()) + entry_len > self.max_log_file_size_bytes
            {
                self.current_log_id += 1;

                let log_file_entry = LogFileEntry {
                    id: self.current_log_id,
                    start_index: entry.index,
                };
                let log_file_entry_bytes = log_file_entry.to_bytes();
                self.directory.entries.push(log_file_entry);
                directory_file.write_all(&log_file_entry_bytes).await?;
                directory_file.flush().await?;
                directory_file.sync_all().await?;

                file.flush().await?;
                file.sync_all().await?;

                let current_file_path = self.log_path(self.current_log_id);
                file = File::options()
                    .append(true)
                    .create(true)
                    .truncate(false)
                    .open(current_file_path)
                    .await?;
            }

            file.write_all(&header_bytes).await?;
            file.write_all(&entry.data).await?;
            file.write_all(&entry.context).await?;
            self.last_log_index = entry.index;
            self.update_terms(&entry);
            self.entries.push(entry);
        }

        file.flush().await?;
        file.sync_all().await?;

        Ok(())
    }

    async fn set_hard_state(&mut self, hard_state: raft::prelude::HardState) -> io::Result<()> {
        self.raft_state.hard_state = hard_state;
        self.persist_hard_state().await
    }

    async fn set_commit_index(&mut self, commit_index: u64) -> io::Result<()> {
        self.raft_state.hard_state.commit = commit_index;

        let mut file = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory_path())
            .await?;
        // Probably unnecessary, but seek to the start of the file.
        // The commit index is the first 64 bits, so overwrite the old index with the new one. We're
        // assuming that the disk can write 64 bits atomically, otherwise we need to worry about
        // torn writes.
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&commit_index.to_le_bytes()).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }
}

/// Size in bytes of the hard state file.
const HARD_STATE_FILE_SIZE: usize = size_of::<u64>() * 3;

/// Represents the Raft hard state to be persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HardState {
    commit: u64,
    term: u64,
    vote: u64,
}

impl HardState {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HARD_STATE_FILE_SIZE);
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

/// Directory of log files.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Directory {
    entries: Vec<LogFileEntry>,
}

impl Directory {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[cfg(test)]
    fn to_bytes(&self) -> Vec<u8> {
        self.entries
            .iter()
            .flat_map(LogFileEntry::to_bytes)
            .collect()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let entries = bytes
            .chunks_exact(LOG_FILE_ENTRY_SIZE)
            .map(LogFileEntry::from_bytes)
            .collect::<Result<_>>()?;
        Ok(Self { entries })
    }
}

/// Size in bytes of a log file entry in the directory file.
const LOG_FILE_ENTRY_SIZE: usize = size_of::<u64>() * 2;

/// Entry in the log directory.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LogFileEntry {
    /// ID of the log file.
    id: u64,
    /// The index of the first entry in this log file.
    start_index: u64,
}

impl LogFileEntry {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LOG_FILE_ENTRY_SIZE);
        bytes.extend_from_slice(&self.id.to_le_bytes());
        bytes.extend_from_slice(&self.start_index.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (id_bytes, start_index_bytes) = bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid log file entry: {bytes:?}"))?;
        let start_index_bytes: [u8; size_of::<u64>()] = start_index_bytes
            .try_into()
            .map_err(|_| format!("invalid log file entry: {bytes:?}"))?;

        let id = u64::from_le_bytes(*id_bytes);
        let start_index = u64::from_le_bytes(start_index_bytes);

        Ok(Self { id, start_index })
    }
}

/// Represents a single Raft log entry.
#[derive(Debug)]
struct Entry {
    #[expect(clippy::struct_field_names)]
    entry_type: i32,
    term: u64,
    index: u64,
    data: Bytes,
    context: Bytes,
}

impl Entry {
    fn header(&self) -> EntryHeader {
        EntryHeader {
            entry_type: self.entry_type,
            term: self.term,
            index: self.index,
            data_len: self.data.len() as u64,
            context_len: self.context.len() as u64,
        }
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
        bytes.extend_from_slice(&self.entry_type.to_le_bytes());
        bytes.extend_from_slice(&self.term.to_le_bytes());
        bytes.extend_from_slice(&self.index.to_le_bytes());
        bytes.extend_from_slice(&self.data_len.to_le_bytes());
        bytes.extend_from_slice(&self.context_len.to_le_bytes());
        bytes
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
    use crate::util::usize_to_u64;
    use proptest::prelude::*;
    use raft::Storage;
    use tempfile::TempDir;

    proptest! {
        #[test]
        fn test_hard_state_roundtrip(commit in any::<u64>(), term in any::<u64>(), vote in any::<u64>()) {
            let hs = HardState { commit, term, vote };
            let bytes = hs.to_bytes();
            let hs2 = HardState::from_bytes(&bytes).unwrap();
            assert_eq!(hs, hs2);
        }

        #[test]
        fn test_log_file_entry_roundtrip(id in any::<u64>(), start_index in any::<u64>()) {
            let lfe = LogFileEntry { id, start_index };
            let bytes = lfe.to_bytes();
            let lfe2 = LogFileEntry::from_bytes(&bytes).unwrap();
            assert_eq!(lfe, lfe2);
        }

        #[test]
        fn test_directory_roundtrip(entries in prop::collection::vec((any::<u64>(), any::<u64>()), 0..10)) {
            let directory = Directory {
                entries: entries.into_iter().map(|(id, start_index)| LogFileEntry { id, start_index }).collect()
            };
            let bytes = directory.to_bytes();
            let directory2 = Directory::from_bytes(&bytes).unwrap();
            assert_eq!(directory, directory2);
        }

        #[test]
        fn test_entry_header_roundtrip(entry_type in any::<i32>(), term in any::<u64>(), index in any::<u64>(), data_len in any::<u64>()) {
            let header = EntryHeader {
                entry_type,
                term,
                index,
                data_len,
                context_len: 0,
            };
            let bytes = header.to_bytes();
            let header2 = EntryHeader::from_bytes(&bytes).unwrap();
            assert_eq!(header, header2);
        }
    }

    #[tokio::test]
    async fn test_file_storage_basic() {
        const MAX_LOG_SIZE: usize = 256;
        const NUM_ENTRIES: usize = 128;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let entries: Vec<raft::prelude::Entry> = (0..NUM_ENTRIES)
            .map(|idx| {
                let entry_type = EntryType::EntryNormal.value();
                let term = usize_to_u64(idx) / 3 + 1;
                let index = usize_to_u64(idx) + 1;
                let data = format!("write {idx}").into_bytes().into();
                let entry = Entry {
                    entry_type,
                    term,
                    index,
                    data,
                    context: Bytes::new(),
                };
                entry.try_into().unwrap()
            })
            .collect();

        // Create new storage
        let mut storage = FileStorage::new(path.clone(), MAX_LOG_SIZE).await.unwrap();

        // Initial state
        let state = storage.initial_state().unwrap();
        assert_eq!(state.hard_state.commit, 0);
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 0);

        // Append entries
        storage.append(&entries[..NUM_ENTRIES / 2]).await.unwrap();
        storage.append(&entries[NUM_ENTRIES / 2..]).await.unwrap();

        // Persist hard state
        let commit = usize_to_u64(entries.len()) / 2;
        let term = entries.last().unwrap().term;
        let hard_state = HardState {
            commit,
            term,
            vote: 2,
        };
        storage
            .set_hard_state(hard_state.clone().into())
            .await
            .unwrap();

        for _ in 0..2 {
            assert_eq!(storage.last_index().unwrap(), usize_to_u64(NUM_ENTRIES));
            for idx in 1..=NUM_ENTRIES {
                let term = (usize_to_u64(idx) - 1) / 3 + 1;
                assert_eq!(storage.term(usize_to_u64(idx)).unwrap(), term);
            }

            let fetched_entries = storage
                .entries(
                    1,
                    usize_to_u64(NUM_ENTRIES) + 1,
                    None,
                    GetEntriesContext::empty(false),
                )
                .unwrap();
            assert_eq!(fetched_entries, entries);

            let fetched_hard_state = storage.read_hard_state().await.unwrap().unwrap();
            assert_eq!(fetched_hard_state, hard_state);

            // Reopen storage
            drop(storage);
            storage = FileStorage::new(path.clone(), MAX_LOG_SIZE).await.unwrap();

            let state = storage.initial_state().unwrap();
            assert_eq!(state.hard_state, hard_state.clone().into());
        }
    }
}
