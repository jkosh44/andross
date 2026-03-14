//! This module contains a binary version of Andross that implements the
//! [Maelstrom protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md) to run
//! [Maelstrom tests](https://github.com/jepsen-io/maelstrom/tree/main).

use andross_server::Result;
use andross_server::log_storage::FjallStorage;
use andross_server::raft_node::{NodeHandle, initialize};
use andross_server::{parse_uri, test_database};
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tonic::codegen::http::uri::Uri;

#[tokio::main]
async fn main() -> Result<()> {
    let node = MaelstromNode::new().await?;
    node.run().await?;
    Ok(())
}

/// Messages produced by Maelstrom and sent to an Andross node.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

/// Body of a [`Message`].
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Body {
    #[serde(rename = "type")]
    message_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// The different types of [`Message`]s that are sent.

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum MessageType {
    Init,
    InitOk,
    Echo,
    EchoOk,
}

#[expect(unused)]
#[repr(u8)]
enum Error {
    /// Indicates that the requested operation could not be completed within a timeout.
    Timeout = 0,
    /// Thrown when a client sends an RPC request to a node which does not exist.
    NodeNotFound = 1,
    /// Use this error to indicate that a requested operation is not supported by the current
    /// implementation. Helpful for stubbing out APIs during development.
    NotSupported = 10,
    /// Indicates that the operation definitely cannot be performed at this time--perhaps because
    /// the server is in a read-only state, has not yet been initialized, believes its peers to be
    /// down, and so on. Do not use this error for indeterminate cases, when the operation may
    /// actually have taken place.
    TemporarilyUnavailable = 11,
    /// The client's request did not conform to the server's expectations, and could not possibly
    /// have been processed.
    MalformedRequest = 12,
    /// Indicates that some kind of general, indefinite error occurred. Use this as a catch-all for
    /// errors you can't otherwise categorize, or as a starting point for your error handler: it's
    /// safe to return crash for every problem by default, then add special cases for more specific
    /// errors later.
    Crash = 13,
    /// Indicates that some kind of general, definite error occurred. Use this as a catch-all for
    /// errors you can't otherwise categorize, when you specifically know that the requested
    /// operation has not taken place. For instance, you might encounter an indefinite failure
    /// during the prepare phase of a transaction: since you haven't started the commit process yet,
    /// the transaction can't have taken place. It's therefore safe to return a definite abort to
    /// the client.
    Abort = 14,
    /// The client requested an operation on a key which does not exist (assuming the operation
    /// should not automatically create missing keys).
    KeyDoesNotExist = 20,
    /// The client requested the creation of a key which already exists, and the server will not
    /// overwrite it.
    KeyAlreadyExists = 21,
    /// The requested operation expected some conditions to hold, and those conditions were not met.
    /// For instance, a compare-and-set operation might assert that the value of a key is currently
    /// 5; if the value is 3, the server would return precondition-failed.
    PreconditionFailed = 22,
    /// The requested transaction has been aborted because of a conflict with another transaction.
    /// Servers need not return this error on every conflict: they may choose to retry automatically
    /// instead.
    TxnConflict = 30,
}

/// Wrapper around a [`NodeHandle`] that translates [`Message`]s into gRPC requests.
struct MaelstromNode {
    node_id: String,
    #[expect(unused)]
    node_ids: Vec<String>,

    #[expect(unused)]
    node_handle: NodeHandle,
    node_join_handle: JoinHandle<Result<()>>,
    node_cancellation_token: CancellationToken,

    next_msg_id: u64,

    stdin: tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>,
    stdout: tokio::io::Stdout,

    _temp_dir: TempDir,
}

impl MaelstromNode {
    async fn new() -> Result<Self> {
        let stdin = tokio::io::stdin();
        let mut lines = tokio::io::BufReader::new(stdin).lines();
        let stdout = tokio::io::stdout();

        // Read init message.
        let init_message = lines.next_line().await?.ok_or("no init message")?;
        let mut init_message: Message = serde_json::from_str(&init_message)?;

        if init_message.body.message_type != MessageType::Init {
            return Err(format!("expected init message, got {init_message:?}").into());
        }

        let node_id = init_message
            .body
            .extra
            .remove("node_id")
            .ok_or("no node_id in init message")?
            .as_str()
            .ok_or("node_id is not a string")?
            .to_string();
        let node_ids = init_message
            .body
            .extra
            .remove("node_ids")
            .ok_or("no node_ids in init message")?;
        let node_ids: Vec<_> = node_ids
            .as_array()
            .ok_or("node_ids is not an array")?
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or("node_ids contains non-string values".into())
                    .map(ToString::to_string)
            })
            .collect::<Result<_>>()?;

        // Initialize Andross node.
        let andross_id = maelstrom_id_to_andross_id(&node_id)?;
        let mut andross_peers = HashMap::with_capacity(node_ids.len());
        for node_id in &node_ids {
            let andross_id = maelstrom_id_to_andross_id(node_id)?;
            let uri = andross_uri(andross_id)?;
            andross_peers.insert(andross_id, uri);
        }
        let raft_tick_interval = Duration::from_millis(100);
        let default_request_timeout = Duration::from_secs(1);
        let (db, temp_dir) = test_database().await;
        let log_storage = spawn_blocking({
            let db = db.clone();
            || {
                let log_keyspace = db.keyspace("log", fjall::KeyspaceCreateOptions::default)?;
                let hard_state_keyspace =
                    db.keyspace("hard_state", fjall::KeyspaceCreateOptions::default)?;
                let log_storage = FjallStorage::new(db, log_keyspace, hard_state_keyspace)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(log_storage)
            }
        })
        .await
        .expect("thread panicked")?;
        let (node, node_handle) = initialize(
            andross_id,
            andross_peers,
            raft_tick_interval,
            default_request_timeout,
            log_storage,
            db,
        )
        .await?;
        let node_cancellation_token = CancellationToken::new();
        let node_join_handle = tokio::spawn({
            let node_cancellation_token = node_cancellation_token.clone();
            async move { node.run(node_cancellation_token).await }
        });

        // Initialize Maelstrom node.
        let mut node = Self {
            node_id,
            node_ids,
            node_handle,
            node_join_handle,
            node_cancellation_token,
            next_msg_id: 0,
            stdin: lines,
            stdout,
            _temp_dir: temp_dir,
        };

        // Response to the init message.
        let msg_id = node.allocate_msg_id();
        node.send_message(Message {
            src: node.node_id.clone(),
            dest: init_message.src,
            body: Body {
                message_type: MessageType::InitOk,
                msg_id: Some(msg_id),
                in_reply_to: init_message.body.msg_id,
                extra: HashMap::new(),
            },
        })
        .await?;

        Ok(node)
    }

    async fn run(mut self) -> Result<()> {
        let _drop_guard = self.node_cancellation_token.clone().drop_guard();
        while let Some(line) = self.stdin.next_line().await? {
            let message: Message = serde_json::from_str(&line)?;
            let response = self.handle_message(message);
            self.send_message(response).await?;
        }
        self.node_cancellation_token.cancel();
        self.node_join_handle.await.expect("task panicked")?;
        Ok(())
    }

    fn handle_message(&mut self, message: Message) -> Message {
        match message.body.message_type {
            MessageType::Init => unreachable!("handled during node initialization"),
            MessageType::Echo => {
                let msg_id = self.allocate_msg_id();
                Message {
                    src: self.node_id.clone(),
                    dest: message.src,
                    body: Body {
                        message_type: MessageType::EchoOk,
                        msg_id: Some(msg_id),
                        in_reply_to: message.body.msg_id,
                        extra: message.body.extra,
                    },
                }
            }
            MessageType::InitOk | MessageType::EchoOk => {
                unreachable!("ok messages are never sent to nodes")
            }
        }
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
        let json = serde_json::to_string(&message)?;
        self.stdout.write_all(json.as_bytes()).await?;
        self.stdout.write_all(b"\n").await?;
        self.stdout.flush().await?;
        Ok(())
    }

    fn allocate_msg_id(&mut self) -> u64 {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        msg_id
    }
}

/// Converts a [Maelstrom node ID](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks)
/// to an Andross node ID.
///
/// Maelstrom IDs are of the format `n\d+`.
fn maelstrom_id_to_andross_id(maelstrom_id: &str) -> Result<u64> {
    let id = maelstrom_id
        .strip_prefix("n")
        .ok_or_else(|| format!("invalid Maelstrom ID: {maelstrom_id}"))?;
    let id = id.parse::<u64>()?;
    Ok(id)
}

/// Constructs a URI for a
fn andross_uri(andross_id: u64) -> Result<Uri> {
    const BASE_PORT: u16 = 42666;
    let port = BASE_PORT + u16::try_from(andross_id)?;
    let host_port = format!("[::1]:{port}");
    let uri = parse_uri(&host_port)?;
    Ok(uri)
}
