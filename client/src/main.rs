use andross_service::kv::CommandRequest;
use andross_service::kv::kv_service_client::KvServiceClient;
use bytes::Bytes;
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Command to write to the server.
    #[arg(long)]
    command: String,

    /// Addresses of the servers to connect to, in the format `host:port`.
    #[arg(long, default_values = ["[::1]:42666"])]
    addrs: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let data = Bytes::from(args.command.into_bytes());

    for addr in args.addrs {
        match send_request(addr, data.clone()).await {
            Ok(()) => return,
            Err(e) => println!("CLIENT ERROR: {e}"),
        }
    }
}

async fn send_request(addr: String, data: Bytes) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvServiceClient::connect(format!("http://{addr}")).await?;
    let request = tonic::Request::new(CommandRequest { data });
    let response = client.command(request).await?;
    println!("RESPONSE={response:?}");
    Ok(())
}
