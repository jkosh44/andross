//! Service definitions for Andross.
//!
//! This crate contains the gRPC service definitions and generated code
//! for communication between Andross nodes and clients.

#![allow(warnings, clippy::all)]
pub mod kv {
    tonic::include_proto!("service.proto.kv.v1");
}
