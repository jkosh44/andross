//! Service definitions for Andross.
//!
//! This crate contains the gRPC service definitions and generated code
//! for communication between Andross nodes and clients.

use tonic::codegen::http::uri::InvalidUri;
use tonic::transport::Uri;

pub mod kv {
    #![allow(warnings, clippy::all)]
    tonic::include_proto!("service.proto.kv.v1");
}

/// Parses an address, in the format `host:port`, into a [`Uri`].
///
/// # Errors
///
/// Returns an error if the `addr` could not be parsed into a [`Uri`].
pub fn parse_uri(addr: &str) -> Result<Uri, InvalidUri> {
    format!("http://{addr}").parse::<Uri>()
}
