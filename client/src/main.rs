use andross_service::kv::CommandRequest;
use andross_service::kv::kv_service_client::KvServiceClient;
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Addresses of the servers to connect to, in the format `host:port`.
    #[arg(short, long, default_values = ["[::1]:42666"])]
    addrs: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    for addr in args.addrs {
        match send_request(addr).await {
            Ok(()) => return,
            Err(e) => println!("CLIENT ERROR: {e}"),
        }
    }
}

async fn send_request(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KvServiceClient::connect(format!("http://{addr}")).await?;
    // TODO: Support user defined requests.
    let request = tonic::Request::new(CommandRequest {
        data: b"42 666".to_vec(),
    });
    let response = client.command(request).await?;
    println!("RESPONSE={response:?}");
    Ok(())
}
