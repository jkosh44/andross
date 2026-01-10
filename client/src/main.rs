use andross_service::kv::NoOpRequest;
use andross_service::kv::no_op_service_client::NoOpServiceClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NoOpServiceClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(NoOpRequest {});

    let response = client.no_op(request).await?;

    println!("RESPONSE={response:?}");

    Ok(())
}
