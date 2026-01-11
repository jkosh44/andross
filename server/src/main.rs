use andross_server::Result;
use andross_server::raft_node::initialize;
use andross_service::kv::raft_service_server::RaftServiceServer;
use clap::Parser;
use raft::storage::MemStorage;
use std::collections::HashSet;
use tonic::transport::Server;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Raft ID of this server.
    #[arg(short, long)]
    id: u64,

    /// The port to listen on.
    #[arg(short, long, default_value_t = 42666)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let port = args.port;
    let addr = format!("[::1]:{port}").parse()?;

    // TODO: Actually pass peers.
    let (node, node_handle) = initialize::<MemStorage>(args.id, HashSet::new()).await?;

    // TODO: Add way to shutdown the node.
    let _node_task_handle = tokio::spawn(node.run());

    Server::builder()
        .add_service(RaftServiceServer::new(node_handle))
        .serve(addr)
        .await?;

    Ok(())
}
