use andross_server::log_storage::FjallStorage;
use andross_server::{AddrConfig, AndrossConfig, Result, parse_uri, start_server};
use clap::Parser;
use fjall::KeyspaceCreateOptions;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tonic::transport::Uri;

const LOG_KEYSPACE: &str = "log";
const HARD_STATE_KEYSPACE: &str = "hard_state";

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
    peers: Vec<(u64, Uri)>,

    /// How often to advance the internal clock of the Raft node.
    #[arg(long, default_value = "100ms", value_parser = humantime::parse_duration)]
    raft_tick_interval: Duration,

    /// The default timeout of requests, if no timeout is specified for the request.
    #[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
    default_request_timeout: Duration,

    /// The path to the database directory.
    #[arg(long)]
    database_path: PathBuf,
}

fn parse_peer(s: &str) -> std::result::Result<(u64, Uri), String> {
    let (id, addr) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid peer format: no `=` found in `{s}`"))?;
    let id = id
        .parse::<u64>()
        .map_err(|e| format!("invalid peer ID: {e}"))?;
    let uri = parse_uri(addr).map_err(|e| format!("invalid peer address: {e}"))?;
    Ok((id, uri))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let peers: HashMap<u64, Uri> = args.peers.into_iter().collect();
    let database_path = args.database_path;
    let (log_storage, db) = spawn_blocking({
        move || {
            let db = fjall::Database::builder(&database_path)
                // Raft log is what provides durability, not the LSM tree. Any writes that are
                // not persisted will be replayed from the Raft log.
                .manual_journal_persist(true)
                .open()?;
            let log_keyspace = db.keyspace(LOG_KEYSPACE, KeyspaceCreateOptions::default)?;
            let hard_state_keyspace =
                db.keyspace(HARD_STATE_KEYSPACE, KeyspaceCreateOptions::default)?;
            let log_storage = FjallStorage::new(db.clone(), log_keyspace, hard_state_keyspace)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((log_storage, db))
        }
    })
    .await
    .expect("thread panicked")?;

    let config = AndrossConfig {
        id: args.id,
        addr_config: AddrConfig::Port(args.port),
        peers,
        raft_tick_interval: args.raft_tick_interval,
        default_request_timeout: args.default_request_timeout,
        log_storage,
        db,
        cancellation_token: CancellationToken::new(),
    };

    let join_handle = start_server(config).await?;
    join_handle.await??;

    Ok(())
}
