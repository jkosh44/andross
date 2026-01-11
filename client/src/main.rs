use andross_service::kv::CommandRequest;
use andross_service::kv::raft_service_client::RaftServiceClient;
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// The port to listen on.
    #[arg(short, long, default_value_t = 42666)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let port = args.port;
    let mut client = RaftServiceClient::connect(format!("http://[::1]:{port}")).await?;

    // TODO: Support user defined requests.
    let request = tonic::Request::new(CommandRequest {
        data: b"42 666".to_vec(),
    });

    let response = client.command(request).await?;

    println!("RESPONSE={response:?}");

    Ok(())
}
