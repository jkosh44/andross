use crate::util::{u64_to_usize, usize_to_u64};
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
        let key_len = usize_to_u64(key.len());
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

    /// Creates a new [`Command`] that corresponds to a cas operation.
    #[must_use]
    pub fn cas(key: &[u8], expected_value: &[u8], desired_value: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(
            1 + size_of::<u64>()
                + key.len()
                + size_of::<u64>()
                + expected_value.len()
                + desired_value.len(),
        );
        bytes.push(CommandKind::Cas as u8);

        let key_len = usize_to_u64(key.len());
        bytes.extend_from_slice(&key_len.to_le_bytes());
        bytes.extend_from_slice(key);

        let expected_value_len = usize_to_u64(expected_value.len());
        bytes.extend_from_slice(&expected_value_len.to_le_bytes());
        bytes.extend_from_slice(expected_value);

        bytes.extend_from_slice(desired_value);

        let bytes = Bytes::from(bytes);

        let cas_tuple = bytes.slice(1..);
        let cas_tuple = CasTuple::new(cas_tuple).expect("cas tuple bytes are valid");

        Self {
            bytes,
            command_inner: CommandInner::Cas { cas_tuple },
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
            CommandKind::Cas => {
                let cas_tuple = bytes.slice(1..);
                let cas_tuple = CasTuple::new(cas_tuple)
                    .map_err(|_| format!("CAS command bytes malformed: {bytes:?}"))?;
                Ok(Self {
                    bytes,
                    command_inner: CommandInner::Cas { cas_tuple },
                })
            }
        }
    }

    /// Returns the [`CommandKind`] variant corresponding to the current instance of the command.
    pub fn kind(&self) -> CommandKind {
        match self.command_inner {
            CommandInner::Insert { .. } => CommandKind::Insert,
            CommandInner::Read { .. } => CommandKind::Read,
            CommandInner::Cas { .. } => CommandKind::Cas,
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
    Cas { cas_tuple: CasTuple },
}

/// Represents the type of command in a system, as an u8 enumeration.
#[repr(u8)]
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum CommandKind {
    Insert = 0,
    Read = 1,
    Cas = 2,
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
            2 => Ok(CommandKind::Cas),
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
}

/// Represents a key, expected value, and desired value.
///
/// A [`CasTuple`] has the following layout:
/// | Offset                                     | Length              | Description           |
/// |--------------------------------------------|---------------------|-----------------------|
/// | 0                                          | 8                   | Key Length            |
/// | 8                                          | Key Length          | Key                   |
/// | 8 + Key Length                             | 8                   | Expected Value Length |
/// | 8 + Key Length + 8                         | Expect Value Length | Expected Value        |
/// | 8 + Key Length + 8 + Expected Value Length | To end              | Desired Value         |
#[derive(Clone, Debug)]
pub struct CasTuple {
    bytes: Bytes,
}

impl CasTuple {
    /// Creates a new [`CasTuple`] from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns error if the bytes are malformed.
    pub fn new(bytes: Bytes) -> Result<Self, String> {
        if bytes.len() < size_of::<u64>() {
            return Err(format!("Tuple bytes too short: {bytes:?}"));
        }
        let key_len_bytes = &bytes[..size_of::<u64>()];
        let rest = &bytes[size_of::<u64>()..];
        let key_len = u64::from_le_bytes(key_len_bytes.try_into().expect("size checked above"));
        let key_len = u64_to_usize(key_len);

        if rest.len() < key_len {
            return Err(format!("Tuple bytes malformed: {bytes:?}"));
        }
        let rest = &rest[key_len..];

        if rest.len() < size_of::<u64>() {
            return Err(format!("Tuple bytes too short: {bytes:?}"));
        }
        let expected_value_len_bytes = &rest[..size_of::<u64>()];
        let rest = &rest[size_of::<u64>()..];
        let expected_value_len = u64::from_le_bytes(
            expected_value_len_bytes
                .try_into()
                .expect("size checked above"),
        );
        let expected_value_len = u64_to_usize(expected_value_len);

        if rest.len() < expected_value_len {
            return Err(format!("Tuple bytes malformed: {bytes:?}"));
        }

        Ok(Self { bytes })
    }

    /// Returns the (key, expected value, desired value) bytes of the [`CasTuple`].
    pub fn to_key_and_values(&self) -> (Bytes, Bytes, Bytes) {
        let key_len_bytes = &self.bytes[..size_of::<u64>()];
        let rest = self.bytes.slice(size_of::<u64>()..);
        let key_len = u64::from_le_bytes(
            key_len_bytes
                .try_into()
                .expect("size checked in constructor"),
        );
        let key_len = u64_to_usize(key_len);
        let key = rest.slice(..key_len);
        let rest = rest.slice(key_len..);

        let expected_value_len_bytes = &rest[..size_of::<u64>()];
        let rest = rest.slice(size_of::<u64>()..);
        let expected_value_len = u64::from_le_bytes(
            expected_value_len_bytes
                .try_into()
                .expect("size checked in constructor"),
        );
        let expected_value_len = u64_to_usize(expected_value_len);
        let expected_value = rest.slice(..expected_value_len);
        let desired_value = rest.slice(expected_value_len..);

        (key, expected_value, desired_value)
    }
}
