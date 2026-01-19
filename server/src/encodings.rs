use crate::util::u64_to_usize;
use bytes::Bytes;
#[cfg(test)]
use proptest_derive::Arbitrary;

/// Represents a set of commands that Raft can execute.
///
/// The first byte is a tag for the command kind and the rest of the bytes are the command.
#[derive(Clone, Debug)]
pub struct Command {
    bytes: Bytes,
    pub(crate) command_inner: CommandInner,
}

impl Command {
    /// Creates a new [`Command`] that corresponds to an insert operation.
    #[must_use]
    pub fn insert(key: &[u8], value: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(1 + size_of::<u64>() + key.len() + value.len());
        bytes.push(CommandKind::Insert as u8);
        let key_len = key.len() as u64;
        bytes.extend_from_slice(&key_len.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        let bytes = Bytes::from(bytes);

        let tuple = bytes.slice(1..);
        let tuple = Tuple::new(tuple).expect("tuple bytes are valid");

        Self {
            bytes,
            command_inner: CommandInner::Insert { tuple },
        }
    }

    /// Creates a new [`Command`] that corresponds to a read operation.
    #[must_use]
    pub fn read(key: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(1 + key.len());
        bytes.push(CommandKind::Read as u8);
        bytes.extend_from_slice(key);
        let bytes = Bytes::from(bytes);

        let key = bytes.slice(1..);

        Self {
            bytes,
            command_inner: CommandInner::Read { key },
        }
    }

    /// Creates a new [`Command`] from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns error if the bytes are malformed.
    pub fn new(bytes: Bytes) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("Command bytes are empty".to_string());
        }
        let command_kind: CommandKind = bytes[0]
            .try_into()
            .map_err(|_| format!("Command bytes malformed: {bytes:?}"))?;
        match command_kind {
            CommandKind::Insert => {
                let tuple = bytes.slice(1..);
                let tuple = Tuple::new(tuple)
                    .map_err(|_| format!("Insert command bytes malformed: {bytes:?}"))?;
                Ok(Self {
                    bytes,
                    command_inner: CommandInner::Insert { tuple },
                })
            }
            CommandKind::Read => {
                let key = bytes.slice(1..);
                Ok(Self {
                    bytes,
                    command_inner: CommandInner::Read { key },
                })
            }
        }
    }

    /// Returns the [`CommandKind`] variant corresponding to the current instance of the command.
    pub fn kind(&self) -> CommandKind {
        match self.command_inner {
            CommandInner::Insert { .. } => CommandKind::Insert,
            CommandInner::Read { .. } => CommandKind::Read,
        }
    }

    /// Converts a [`Command`] into the inner bytes.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CommandInner {
    Insert { tuple: Tuple },
    Read { key: Bytes },
}

/// Represents the type of command in a system, as an u8 enumeration.
#[repr(u8)]
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum CommandKind {
    Insert = 0,
    Read = 1,
}

impl From<CommandKind> for u8 {
    fn from(command_kind: CommandKind) -> Self {
        command_kind as u8
    }
}

impl TryFrom<u8> for CommandKind {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CommandKind::Insert),
            1 => Ok(CommandKind::Read),
            _ => Err(format!("Invalid command kind: {value}")),
        }
    }
}

/// Represents a single key-value pair in the database.
///
/// A [`Tuple`] has the following layout:
/// | Offset             | Length     | Description |
/// |--------------------|------------|-------------|
/// | 0                  | 8          | Key Length  |
/// | 8                  | Key Length | Key         |
/// | 8 + Key Length     | To end     | Value       |
#[derive(Clone, Debug)]
pub struct Tuple {
    bytes: Bytes,
}

impl Tuple {
    /// Creates a new [`Tuple`] from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns error if the bytes are malformed.
    pub fn new(bytes: Bytes) -> Result<Self, String> {
        if bytes.len() < size_of::<u64>() {
            return Err(format!("Tuple bytes too short: {bytes:?}"));
        }
        let key_len_bytes = &bytes[..size_of::<u64>()];
        let key_len = u64::from_be_bytes(key_len_bytes.try_into().expect("size checked above"));
        if bytes.len() < size_of::<u64>() + u64_to_usize(key_len) {
            return Err(format!("Tuple bytes malformed: {bytes:?}"));
        }
        Ok(Self { bytes })
    }

    /// Returns the key and value bytes of the [`Tuple`].
    pub fn to_key_value(&self) -> (Bytes, Bytes) {
        let key_len_bytes = &self.bytes[..size_of::<u64>()];
        let key_len = u64::from_be_bytes(key_len_bytes.try_into().expect("size checked above"));
        let key_len = u64_to_usize(key_len);
        let key = self
            .bytes
            .slice(size_of::<u64>()..size_of::<u64>() + key_len);
        let value = self.bytes.slice(size_of::<u64>() + key_len..);
        (key, value)
    }

    /// Converts a [`Tuple`] into the inner bytes.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}
