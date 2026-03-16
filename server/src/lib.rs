//! Andross server implementation.
//!
//! This crate provides the core server logic for the Andross distributed key-value store,
//! including the Raft consensus protocol implementation and storage abstractions.

use crate::log_storage::LogStorage;
use crate::raft_node::{GrpcPeerClient, initialize};
use crate::service::kv_service_server::KvServiceServer;
use crate::service::raft_service_server::RaftServiceServer;
pub use encodings::{Command, Tuple};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::select;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Server, Uri};
pub use util::parse_uri;

mod async_raft_group;
mod encodings;
pub mod log_storage;
pub mod raft_node;
pub mod service;
pub(crate) mod util;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct AndrossConfig<T: LogStorage> {
    pub id: u64,
    pub addr_config: AddrConfig,
    pub peers: HashMap<u64, Uri>,
    pub raft_tick_interval: Duration,
    pub default_request_timeout: Duration,
    pub log_storage: T,
    pub db: fjall::Database,
    pub cancellation_token: CancellationToken,
}

pub enum AddrConfig {
    Port(u16),
    TcpListener(TcpListener),
}

/// Starts the Andross KV database server.
///
/// Returns a [`tokio::task::JoinHandle`] that completes when the server shuts down, with the
/// result of the server.
///
/// # Errors
///
/// Returns an error if the configuration is invalid or if the server could not be started.
pub async fn start_server<T: LogStorage + Send + 'static>(
    AndrossConfig {
        id,
        addr_config,
        peers,
        raft_tick_interval,
        default_request_timeout,
        log_storage,
        db,
        cancellation_token,
    }: AndrossConfig<T>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let (node, node_handle) = initialize::<T, _, _>(
        id,
        peers,
        GrpcPeerClient::new,
        raft_tick_interval,
        default_request_timeout,
        log_storage,
        db,
    )
    .await?;

    let node_cancellation_token = cancellation_token.clone();
    let server_cancellation_token = cancellation_token.clone();
    let mut node_join_handle = tokio::spawn(async move { node.run(node_cancellation_token).await });
    let mut server_join_handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        let builder = Server::builder()
            .tcp_nodelay(true)
            .add_service(RaftServiceServer::new(node_handle.clone()))
            .add_service(KvServiceServer::new(node_handle));

        match addr_config {
            AddrConfig::Port(port) => {
                let addr = format!("[::1]:{port}").parse()?;
                builder
                    .serve_with_shutdown(addr, server_cancellation_token.cancelled())
                    .await?;
                Ok(())
            }
            AddrConfig::TcpListener(tcp_listener) => {
                let incoming = TcpIncoming::from(tcp_listener).with_nodelay(Some(true));
                builder
                    .serve_with_incoming_shutdown(incoming, server_cancellation_token.cancelled())
                    .await?;
                Ok(())
            }
        }
    });

    let join_handle = tokio::spawn(async move {
        select! {
            node_result = &mut node_join_handle => {
                cancellation_token.cancel();
                let server_result = server_join_handle.await.expect("task panicked");
                node_result.expect("node task panicked")?;
                server_result?;
            },
            server_result = &mut server_join_handle => {
                cancellation_token.cancel();
                let node_result = node_join_handle.await.expect("task panicked");
                server_result.expect("node task panicked")?;
                node_result?;
            },
        }

        Ok(())
    });
    Ok(join_handle)
}

pub async fn test_database() -> (fjall::Database, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = test_database_from_state(temp_dir.path().to_path_buf()).await;
    (db, temp_dir)
}

pub async fn test_database_from_state(path: PathBuf) -> fjall::Database {
    spawn_blocking(move || fjall::Database::builder(path).open())
        .await
        .expect("thread panicked")
        .unwrap()
}
