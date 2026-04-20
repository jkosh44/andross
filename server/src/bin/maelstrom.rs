//! This module contains a binary version of Andross that implements the
//! [Maelstrom protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md) to run
//! [Maelstrom tests](https://github.com/jepsen-io/maelstrom/tree/main).

use andross_server::Result;
use andross_server::log_storage::FjallStorage;
use andross_server::raft_node::{NodeHandle, PeerClient, initialize};
use andross_server::{parse_uri, test_database};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio_util::sync::CancellationToken;
use tonic::async_trait;
use tonic::codegen::http::uri::Uri;

const RAFT_MESSAGE_KEY: &str = "raft_message";

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
    Topology,
    TopologyOk,
    Broadcast,
    BroadcastOk,
    Read,
    ReadOk,

    Raft,
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

struct MealstromPeerClient {
    tx: mpsc::UnboundedSender<raft::eraftpb::Message>,
}

#[async_trait]
impl PeerClient for MealstromPeerClient {
    async fn send_messages(&mut self, messages: Vec<raft::eraftpb::Message>) -> Result<()> {
        for message in messages {
            if self.tx.send(message).is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// Messages that concurrent operations send to the main [`MaelstromNode`] task.
enum InnerMessage {
    BroadcastDone {
        incoming_message: Message,
        neighbors: Vec<String>,
        broadcast_message: u64,
    },
    ReadDone {
        incoming_message: Message,
        broadcast_messages: Vec<u64>,
    },
}

/// Wrapper around a [`NodeHandle`] that translates [`Message`]s into gRPC requests.
struct MaelstromNode {
    node_id: String,
    #[expect(unused)]
    node_ids: Vec<String>,

    node_handle: NodeHandle,
    node_join_handle: JoinHandle<Result<()>>,
    node_cancellation_token: CancellationToken,

    network_rx: mpsc::UnboundedReceiver<raft::eraftpb::Message>,

    inner_tx: mpsc::UnboundedSender<InnerMessage>,
    inner_rx: mpsc::UnboundedReceiver<InnerMessage>,
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
        for peer_id in &node_ids {
            if *peer_id != node_id {
                let andross_id = maelstrom_id_to_andross_id(peer_id)?;
                let uri = andross_id_to_andross_uri(andross_id)?;
                andross_peers.insert(andross_id, uri);
            }
        }
        // eprintln!("ANDROSS ID: {andross_id:?}");
        // eprintln!("ANDROSS PEERS: {andross_peers:?}");
        let raft_tick_interval = Duration::from_millis(100);
        let default_request_timeout = Duration::from_secs(30);
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
        // let log_storage = MemStorage::new();
        let (network_tx, network_rx) = mpsc::unbounded_channel();
        let (node, node_handle) = initialize(
            andross_id,
            andross_peers,
            move |_uri| MealstromPeerClient {
                tx: network_tx.clone(),
            },
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
        let (inner_tx, inner_rx) = mpsc::unbounded_channel();
        let mut node = Self {
            node_id,
            node_ids,
            node_handle,
            node_join_handle,
            node_cancellation_token,
            network_rx,
            inner_tx,
            inner_rx,
            next_msg_id: 0,
            stdin: lines,
            stdout,
            _temp_dir: temp_dir,
        };

        // Respond to the init message immediately per Maelstrom protocol.
        let response = node.response_message(init_message, MessageType::InitOk, HashMap::new());
        node.send_message(response).await?;

        Ok(node)
    }

    async fn run(mut self) -> Result<()> {
        let _drop_guard = self.node_cancellation_token.clone().drop_guard();
        loop {
            select! {
                line = self.stdin.next_line() => {
                    let Some(line) = line? else {
                        break;
                    };
                    let message: Message = serde_json::from_str(&line)?;
                    if let Some(response) = self.handle_message(message).await? {
                        self.send_message(response).await?;
                    }
                }
                Some(raft_message) = self.network_rx.recv() => {
                    // eprintln!("RECEIVED RAFT MESSAGE OVER THE NETWORK: {raft_message:?}");
                    let to_andross_id = raft_message.to;
                    let to_maelstrom_id = andross_id_to_maelstrom_id(to_andross_id);
                    let mut extra = HashMap::new();
                    let raft_bytes = raft_message.write_to_bytes()?;
                    let raft_base64 = BASE64_STANDARD.encode(&raft_bytes);
                    extra.insert(RAFT_MESSAGE_KEY.to_string(), serde_json::Value::String(raft_base64));

                    let message = Message {
                        src: self.node_id.clone(),
                        dest: to_maelstrom_id,
                        body: Body {
                            message_type: MessageType::Raft,
                            msg_id: None,
                            in_reply_to: None,
                            extra,
                        },
                    };
                    self.send_message(message).await?;
                }
                Some(inner_message) = self.inner_rx.recv() => {
                    self.handle_inner_message(inner_message).await?;
                }
                _ = self.node_cancellation_token.cancelled() => {}
            }
        }
        self.node_cancellation_token.cancel();
        self.node_join_handle.await.expect("task panicked")?;
        Ok(())
    }

    /// Apply `message` to node and return a response message if one is required.
    async fn handle_message(&mut self, mut message: Message) -> Result<Option<Message>> {
        // eprintln!("INCOMING MESSAGE: {message:?}");
        match message.body.message_type {
            MessageType::Init => unreachable!("handled during node initialization"),
            MessageType::Echo => {
                let extra = std::mem::take(&mut message.body.extra);
                let response = self.response_message(message, MessageType::EchoOk, extra);
                Ok(Some(response))
            }
            MessageType::Topology => {
                // When we receive a topology message, we store our neighbors in Andross as a string
                // with neighbors separated by commas.
                let topology = message
                    .body
                    .extra
                    .remove("topology")
                    .ok_or("no topology in topology message")?;
                let topology = topology.as_object().ok_or("topology is not an object")?;
                if let Some(neighbors) = topology.get(&self.node_id) {
                    let neighbors = neighbors.as_array().ok_or("neighbors is not an array")?;
                    let neighbors: Vec<_> = neighbors
                        .into_iter()
                        .map(|neighbor| {
                            Ok(neighbor
                                .as_str()
                                .ok_or("neighbor is not a string")?
                                .to_string())
                        })
                        .collect::<Result<_>>()?;
                    let neighbors: String = neighbors.join(",");
                    let key = self.neighbors_key();

                    let node_handle = self.node_handle.clone();
                    tokio::spawn(async move {
                        while let Err(e) = node_handle.insert(&key, neighbors.as_bytes()).await {
                            eprintln!("failed to insert neighbors: {e}");
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    });

                    let response =
                        self.response_message(message, MessageType::TopologyOk, HashMap::new());
                    Ok(Some(response))
                } else {
                    let response =
                        self.response_message(message, MessageType::TopologyOk, HashMap::new());
                    Ok(Some(response))
                }
            }
            MessageType::Broadcast => {
                let broadcast_message = message
                    .body
                    .extra
                    .remove("message")
                    .ok_or("no message in broadcast message")?;
                let broadcast_message = broadcast_message
                    .as_u64()
                    .ok_or("message is not a number")?;

                let node_handle = self.node_handle.clone();
                let inner_tx = self.inner_tx.clone();
                let neighbors_key = self.neighbors_key();
                let broadcast_message_key = self.broadcast_key();
                tokio::spawn(async move {
                    let broadcast_messages: HashSet<_> =
                        Self::get_broadcast_messages(&node_handle, &broadcast_message_key)
                            .await
                            .into_iter()
                            .collect();
                    // Skip if this message is a duplicate.
                    if broadcast_messages.contains(&broadcast_message) {
                        return;
                    }

                    // Get neighbors.
                    let neighbors = loop {
                        match node_handle.read(&neighbors_key).await {
                            Ok(neighbors) => break neighbors,
                            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                        }
                    };
                    let neighbors = neighbors
                        .map(|neighbors| {
                            String::from_utf8(neighbors.to_vec()).expect("invalid UTF-8")
                        })
                        .unwrap_or(String::new());
                    let neighbors: Vec<_> = neighbors.split(',').map(String::from).collect();

                    // CAS the new message.
                    let mut expected_value = Vec::new();
                    loop {
                        let mut broadcast_messages =
                            Self::decode_broadcast_messages(&expected_value);
                        broadcast_messages.push(broadcast_message);
                        let desired_value = Self::encode_broadcast_messages(&broadcast_messages);
                        match node_handle
                            .cas(&broadcast_message_key, &expected_value, &desired_value)
                            .await
                        {
                            Ok(Some(current_value)) => expected_value = current_value.to_vec(),
                            Ok(None) => break,
                            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                        }
                    }

                    let _ = inner_tx.send(InnerMessage::BroadcastDone {
                        incoming_message: message,
                        neighbors,
                        broadcast_message,
                    });
                });

                Ok(None)
            }
            MessageType::Read => {
                let node_handle = self.node_handle.clone();
                let inner_tx = self.inner_tx.clone();
                let broadcast_message_key = self.broadcast_key();
                tokio::spawn(async move {
                    let broadcast_messages =
                        Self::get_broadcast_messages(&node_handle, &broadcast_message_key).await;
                    let _ = inner_tx.send(InnerMessage::ReadDone {
                        incoming_message: message,
                        broadcast_messages,
                    });
                });

                Ok(None)
            }
            MessageType::Raft => {
                let raft_base64 = message
                    .body
                    .extra
                    .remove(RAFT_MESSAGE_KEY)
                    .ok_or("no raft message")?;
                let raft_base64 = raft_base64.as_str().ok_or("raft message is not a string")?;
                let raft_bytes = BASE64_STANDARD.decode(raft_base64)?;
                let raft_message = raft::eraftpb::Message::parse_from_bytes(&raft_bytes)?;
                // eprintln!("RAFT MESSAGE: {raft_message:?}");
                self.node_handle.execute_messages(vec![raft_message])?;
                Ok(None)
            }
            MessageType::InitOk
            | MessageType::EchoOk
            | MessageType::TopologyOk
            | MessageType::BroadcastOk
            | MessageType::ReadOk => {
                unreachable!("ok messages are never sent to nodes")
            }
        }
    }

    async fn get_broadcast_messages(
        node_handle: &NodeHandle,
        broadcast_message_key: &[u8],
    ) -> Vec<u64> {
        let broadcast_messages = loop {
            match node_handle.read(broadcast_message_key).await {
                Ok(broadcast_messages) => break broadcast_messages,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        let broadcast_messages = broadcast_messages.unwrap_or_default();
        Self::decode_broadcast_messages(&broadcast_messages)
    }

    fn decode_broadcast_messages(broadcast_messages: &[u8]) -> Vec<u64> {
        let broadcast_messages =
            String::from_utf8(broadcast_messages.to_vec()).expect("invalid UTF-8");
        broadcast_messages
            .split(',')
            .filter_map(|m| {
                if m.is_empty() {
                    None
                } else {
                    m.parse::<u64>().ok()
                }
            })
            .collect()
    }

    fn encode_broadcast_messages(broadcast_messages: &[u64]) -> Vec<u8> {
        broadcast_messages
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
            .into_bytes()
    }

    async fn handle_inner_message(&mut self, inner_message: InnerMessage) -> Result<()> {
        match inner_message {
            InnerMessage::BroadcastDone {
                incoming_message,
                neighbors,
                broadcast_message,
            } => {
                if incoming_message.body.msg_id.is_some() {
                    let response = self.response_message(
                        incoming_message,
                        MessageType::BroadcastOk,
                        HashMap::new(),
                    );
                    self.send_message(response).await?;
                }
                for neighbor in neighbors {
                    if neighbor.is_empty() {
                        continue;
                    }
                    let mut extra = HashMap::new();
                    extra.insert(
                        "message".to_string(),
                        serde_json::Value::Number(broadcast_message.into()),
                    );
                    let message = Message {
                        src: self.node_id.clone(),
                        dest: neighbor,
                        body: Body {
                            message_type: MessageType::Broadcast,
                            msg_id: None,
                            in_reply_to: None,
                            extra,
                        },
                    };
                    self.send_message(message).await?;
                }
            }
            InnerMessage::ReadDone {
                incoming_message,
                broadcast_messages,
            } => {
                let mut extra = HashMap::new();
                extra.insert(
                    "messages".to_string(),
                    serde_json::Value::Array(
                        broadcast_messages
                            .iter()
                            .map(|m| serde_json::Value::Number((*m).into()))
                            .collect(),
                    ),
                );
                let response = self.response_message(incoming_message, MessageType::ReadOk, extra);
                self.send_message(response).await?;
            }
        }

        Ok(())
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
        let json = serde_json::to_string(&message)?;
        self.stdout.write_all(json.as_bytes()).await?;
        self.stdout.write_all(b"\n").await?;
        self.stdout.flush().await?;
        Ok(())
    }

    fn response_message(
        &mut self,
        incoming: Message,
        response_type: MessageType,
        extra: HashMap<String, serde_json::Value>,
    ) -> Message {
        let msg_id = self.allocate_msg_id();
        Message {
            src: self.node_id.clone(),
            dest: incoming.src,
            body: Body {
                message_type: response_type,
                msg_id: Some(msg_id),
                in_reply_to: incoming.body.msg_id,
                extra,
            },
        }
    }

    fn allocate_msg_id(&mut self) -> u64 {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        msg_id
    }

    fn neighbors_key(&self) -> Vec<u8> {
        let key = format!("maelstrom/neighbors/{}", self.node_id);
        key.into_bytes()
    }

    fn broadcast_key(&self) -> Vec<u8> {
        let key = format!("maelstrom/broadcast/message/{}", self.node_id);
        key.into_bytes()
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

/// Converts an Andross node ID to a
/// [Maelstrom node ID](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks).
///
/// Maelstrom IDs are of the format `n\d+`.
fn andross_id_to_maelstrom_id(maelstrom_id: u64) -> String {
    format!("n{maelstrom_id}")
}

const BASE_PORT: u16 = 42666;

/// Constructs a URI from an andross node ID.
fn andross_id_to_andross_uri(andross_id: u64) -> Result<Uri> {
    let port = BASE_PORT + u16::try_from(andross_id)?;
    let host_port = format!("[::1]:{port}");
    let uri = parse_uri(&host_port)?;
    Ok(uri)
}
