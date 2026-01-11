//! Andross server implementation.
//!
//! This crate provides the core server logic for the Andross distributed key-value store,
//! including the Raft consensus protocol implementation and storage abstractions.

use crate::raft_node::initialize;
use andross_service::kv::kv_service_server::KvServiceServer;
use andross_service::kv::raft_service_server::RaftServiceServer;
use raft::storage::MemStorage;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

pub mod raft_node;
pub mod storage;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct AndrossConfig {
    pub id: u64,
    pub addr_config: AddrConfig,
    pub peers: HashMap<u64, String>,
    pub raft_tick_interval: Duration,
    pub default_request_timeout: Duration,
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
pub async fn start_server(
    AndrossConfig {
        id,
        addr_config,
        peers,
        raft_tick_interval,
        default_request_timeout,
        cancellation_token,
    }: AndrossConfig,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let (node, node_handle) =
        initialize::<MemStorage>(id, peers, raft_tick_interval, default_request_timeout).await?;

    let node_cancellation_token = cancellation_token.clone();
    let server_cancellation_toke = cancellation_token.clone();
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
                    .serve_with_shutdown(addr, server_cancellation_toke.cancelled())
                    .await?;
                Ok(())
            }
            AddrConfig::TcpListener(tcp_listener) => {
                let incoming = TcpIncoming::from(tcp_listener).with_nodelay(Some(true));
                builder
                    .serve_with_incoming_shutdown(incoming, server_cancellation_toke.cancelled())
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
