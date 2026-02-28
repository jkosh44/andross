use andross_client::Client;
use andross_server::util::parse_uri;
use bytes::Bytes;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Addresses of the servers to connect to, in the format `host:port`.
    #[arg(long, default_values = ["[::1]:42666"])]
    addrs: Vec<String>,

    /// Command to send to the server.
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Inserts a new key-value pair into the database.
    Insert {
        #[arg(long)]
        key: Bytes,
        #[arg(long)]
        value: Bytes,
    },
    /// Reads a key-value pair from the database.
    Read {
        #[arg(long)]
        key: Bytes,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let uris = args
        .addrs
        .iter()
        .map(|addr| parse_uri(addr).expect("invalid URI"));
    let mut client = Client::new(uris).await;
    match &args.command {
        Commands::Insert { key, value } => match client.insert(key.clone(), value.clone()).await {
            Ok(()) => println!("INSERT SUCCESSFUL"),
            Err(e) => panic!("INSERT FAILED: {e}"),
        },
        Commands::Read { key } => match client.read(key.clone()).await {
            Ok(value) => match std::str::from_utf8(&value) {
                Ok(value) => println!("VALUE: {value}"),
                Err(_) => println!("VALUE: {value:?}"),
            },
            Err(e) => panic!("READ FAILED: {e}"),
        },
    }
}
