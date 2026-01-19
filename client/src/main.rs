use andross_server::service::InsertRequest;
use andross_server::service::kv_service_client::KvServiceClient;
use andross_server::{Tuple, parse_uri};
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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Commands::Insert { key, value } => {
            for addr in args.addrs {
                match send_request(addr, key.clone(), value.clone()).await {
                    Ok(()) => return,
                    Err(e) => println!("CLIENT ERROR: {e}"),
                }
            }
        }
    }
}

async fn send_request(
    addr: String,
    key: Bytes,
    value: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let uri = parse_uri(&addr)?;
    let mut client = KvServiceClient::connect(uri).await?;
    let tuple = Tuple::from_key_value(&key, &value).into_bytes();
    let request = tonic::Request::new(InsertRequest { tuple });
    let response = client.insert(request).await?;
    println!("RESPONSE={response:?}");
    Ok(())
}
