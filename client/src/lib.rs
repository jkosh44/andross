//! Client library for the Andross KV database.

use andross_server::Command;
use andross_server::service::kv_service_client::KvServiceClient;
use andross_server::service::{CommandRequest, CommandResponse};
use bytes::Bytes;
use thiserror::Error;
use tonic::Code;
use tonic::transport::Uri;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("invalid request: {msg}")]
    InvalidRequest { msg: String },
    #[error("server unavailable: {msg}")]
    Unavailable { msg: String },
    #[error("no leader: {msg}")]
    NoLeader { msg: String },
    #[error("indeterminate error: {msg}")]
    Indeterminate { msg: String },
    #[error("unexpected internal error: {msg}")]
    InternalError { msg: String },
}

pub struct Client {
    clients: Vec<(Uri, Option<KvServiceClient<tonic::transport::Channel>>)>,
}

impl Client {
    /// Creates a new [`Client`] connecting to servers found at `uri`.
    pub async fn new(uris: impl IntoIterator<Item = Uri>) -> Self {
        let mut clients = Vec::new();
        for uri in uris {
            let client = connect(&uri).await;
            clients.push((uri, client));
        }
        Self { clients }
    }

    /// Insert `key` and `value` into the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert could not be completed successfully.
    pub async fn insert(&mut self, key: Bytes, value: Bytes) -> Result<(), ClientError> {
        let command = Command::insert(&key, &value);
        // Do not retry indeterminate errors.
        let response_bytes = self
            .retry_command(command, |e| !matches!(e, ClientError::Indeterminate { .. }))
            .await?;
        assert_eq!(response_bytes, Some(Bytes::new()));
        Ok(())
    }

    /// Reads `key` from the database and returns the associated value.
    ///
    /// # Errors
    ///
    /// Returns an error if the read could not be completed successfully.
    pub async fn read(&mut self, key: Bytes) -> Result<Bytes, ClientError> {
        let command = Command::read(&key);
        // Retry all errors.
        let response_bytes = self.retry_command(command, |_| true).await?;
        let response_bytes = response_bytes.expect("read command must return a value");
        Ok(response_bytes)
    }

    async fn retry_command(
        &mut self,
        command: Command,
        retry_error: impl Fn(&ClientError) -> bool,
    ) -> Result<Option<Bytes>, ClientError> {
        let command_bytes = command.into_bytes();
        let mut last_error = None;

        for (uri, client) in &mut self.clients {
            if client.is_none() {
                *client = connect(uri).await;
            }
            if let Some(client) = client {
                let request = tonic::Request::new(CommandRequest {
                    command_bytes: command_bytes.clone(),
                });
                match client.command(request).await {
                    Ok(response) => {
                        let CommandResponse { response_bytes } = response.into_inner();
                        return Ok(response_bytes);
                    }
                    Err(e) => {
                        let e = process_error(&e);
                        if !retry_error(&e) {
                            return Err(e);
                        }
                        last_error = Some(e);
                    }
                }
            }
        }

        Err(last_error.unwrap_or(ClientError::Unavailable {
            msg: "no servers are reachable".to_string(),
        }))
    }
}

async fn connect(uri: &Uri) -> Option<KvServiceClient<tonic::transport::Channel>> {
    let client = KvServiceClient::connect(uri.clone()).await;
    if let Err(e) = &client {
        println!("Unable to connect to {uri:?}: {e}");
    }
    client.ok()
}

fn process_error(error: &tonic::Status) -> ClientError {
    match error.code() {
        Code::Ok => unreachable!("errors do not have an OK code"),
        Code::InvalidArgument => ClientError::InvalidRequest {
            msg: error.to_string(),
        },
        Code::DeadlineExceeded | Code::Cancelled => ClientError::Indeterminate {
            msg: error.to_string(),
        },
        Code::FailedPrecondition => ClientError::NoLeader {
            msg: error.to_string(),
        },
        Code::Unavailable => ClientError::Unavailable {
            msg: error.to_string(),
        },
        Code::Unknown
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Internal
        | Code::DataLoss
        | Code::Unauthenticated => ClientError::InternalError {
            msg: error.to_string(),
        },
    }
}
