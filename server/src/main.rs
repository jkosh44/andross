use andross_server::Result;
use andross_server::raft_node::initialize;
use andross_service::kv::kv_service_server::KvServiceServer;
use andross_service::kv::raft_service_server::RaftServiceServer;
use clap::Parser;
use raft::storage::MemStorage;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Raft ID of this server.
    #[arg(long)]
    id: u64,

    /// The port to listen on.
    #[arg(long, default_value_t = 42666)]
    port: u16,

    /// Map of node IDs to hostnames and ports (e.g., 1=localhost:42667).
    #[arg(long, value_parser = parse_peer)]
    peers: Vec<(u64, String)>,

    #[arg(long, default_value = "100ms", value_parser = humantime::parse_duration)]
    default_request_timeout: Duration,
}

fn parse_peer(s: &str) -> std::result::Result<(u64, String), String> {
    let (id, addr) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid peer format: no `=` found in `{s}`"))?;
    let id = id
        .parse::<u64>()
        .map_err(|e| format!("invalid peer ID: {e}"))?;
    let addr = format!("http://{addr}");
    Ok((id, addr))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let port = args.port;
    let addr = format!("[::1]:{port}").parse()?;

    let peers: HashMap<u64, String> = args.peers.into_iter().collect();

    let (node, node_handle) =
        initialize::<MemStorage>(args.id, peers, args.default_request_timeout).await?;

    let cancellation_token = CancellationToken::new();
    let node_cancellation_token = cancellation_token.clone();
    let server_cancellation_toke = cancellation_token.clone();
    let mut node_task_handle = tokio::spawn(async move { node.run(node_cancellation_token).await });
    let mut server_task_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(RaftServiceServer::new(node_handle.clone()))
            .add_service(KvServiceServer::new(node_handle))
            .serve_with_shutdown(addr, server_cancellation_toke.cancelled())
            .await
    });

    select! {
        node_result = &mut node_task_handle => {
            cancellation_token.cancel();
            let server_result = server_task_handle.await.expect("task panicked");
            node_result.expect("node task panicked")?;
            server_result?;
        },
        server_result = &mut server_task_handle => {
            cancellation_token.cancel();
            let node_result = node_task_handle.await.expect("task panicked");
            server_result.expect("node task panicked")?;
            node_result?;
        },
    }

    Ok(())
}
