//! This crate contains the gRPC service definitions and generated code
//! for communication between Andross nodes and clients.

#![allow(warnings, clippy::all)]
tonic::include_proto!("server.proto.kv.v1");
