use andross_service::kv::no_op_service_server::{NoOpService, NoOpServiceServer};
use andross_service::kv::{NoOpRequest, NoOpResponse};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let server = MyNoOpServer::default();

    Server::builder()
        .add_service(NoOpServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug, Default)]
pub struct MyNoOpServer {}

#[tonic::async_trait]
impl NoOpService for MyNoOpServer {
    async fn no_op(
        &self,
        _request: Request<NoOpRequest>,
    ) -> Result<Response<NoOpResponse>, Status> {
        Ok(NoOpResponse {}.into())
    }
}
