//! Various utilities.

use tonic::codegen::http::uri::InvalidUri;
use tonic::transport::Uri;

pub fn u64_to_usize(n: u64) -> usize {
    n.try_into().expect("64-bit platform")
}

pub fn usize_to_u64(n: usize) -> u64 {
    n.try_into().expect("64-bit platform")
}

/// Parses an address, in the format `host:port`, into a [`Uri`].
///
/// # Errors
///
/// Returns an error if the `addr` could not be parsed into a [`Uri`].
pub fn parse_uri(addr: &str) -> Result<Uri, InvalidUri> {
    format!("http://{addr}").parse::<Uri>()
}
