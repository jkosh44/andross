//! Andross server implementation.
//!
//! This crate provides the core server logic for the Andross distributed key-value store,
//! including the Raft consensus protocol implementation and storage abstractions.

pub mod raft_node;
pub mod storage;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
