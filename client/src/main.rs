use andross_server::service::CommandRequest;
use andross_server::service::kv_service_client::KvServiceClient;
use andross_server::{Command, parse_uri};
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
    let command = match &args.command {
        Commands::Insert { key, value } => Command::insert(key, value),
        Commands::Read { key } => Command::read(key),
    };
    for addr in args.addrs {
        match send_request(addr, command.clone()).await {
            Ok(()) => return,
            Err(e) => println!("CLIENT ERROR: {e}"),
        }
    }
}

async fn send_request(addr: String, command: Command) -> Result<(), Box<dyn std::error::Error>> {
    let uri = parse_uri(&addr)?;
    let mut client = KvServiceClient::connect(uri).await?;
    let request = tonic::Request::new(CommandRequest {
        command_bytes: command.into_bytes(),
    });
    let response = client.command(request).await?;
    println!("RESPONSE={response:?}");
    Ok(())
}
