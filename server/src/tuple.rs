use crate::util::u64_to_usize;
use bytes::Bytes;

/// Represents a single key-value pair in the database.
///
/// A [`Tuple`] has the following layout:
/// | Offset             | Length     | Description |
/// |--------------------|------------|-------------|
/// | 0                  | 8          | Key Length  |
/// | 8                  | Key Length | Key         |
/// | 8 + Key Length     | To end     | Value       |
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

    /// Creates a new [`Tuple`] from a key and value.
    #[must_use]
    pub fn from_key_value(key: &[u8], value: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(size_of::<u64>() + key.len() + value.len());
        let key_len = key.len() as u64;
        bytes.extend_from_slice(&key_len.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        Self {
            bytes: bytes.into(),
        }
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
