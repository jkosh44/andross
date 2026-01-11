//! Raft node implementation.
//!
//! This module contains the `Node` struct which manages the Raft state machine
//! and handles communication with other nodes in the cluster.

use crate::Result;
use crate::storage::Storage;
use andross_service::kv::kv_service_server::KvService;
use andross_service::kv::raft_service_client::RaftServiceClient;
use andross_service::kv::raft_service_server::RaftService;
use andross_service::kv::{CommandRequest, CommandResponse, MessageRequest, MessageResponse};
use bytes::Bytes;
use itertools::Itertools;
use protobuf::{Message as ProtobufMessage, ProtobufResult};
use raft::prelude::{ConfState, Entry};
use raft::{Config, RawNode};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

// TODO: This seems too high.
/// Timeout in-between Raft ticks.
const RAFT_TIMEOUT: Duration = Duration::from_millis(100);

enum Message {
    Command(Bytes),
    RaftMessages(Vec<raft::prelude::Message>),
}

pub struct Node<T: Storage> {
    raft_group: RawNode<T>,
    rx: mpsc::UnboundedReceiver<Message>,
    peers: HashMap<u64, PeerClient>,
}

impl<T: Storage> Node<T> {
    async fn new(
        id: u64,
        peers: HashMap<u64, String>,
        rx: mpsc::UnboundedReceiver<Message>,
    ) -> Result<Self> {
        let voters = std::iter::once(id).chain(peers.keys().copied()).collect();
        let conf_state = ConfState {
            voters,
            ..ConfState::default()
        };
        let storage = T::init(conf_state).await;
        let config = Config::new(id);
        let raft = RawNode::with_default_logger(&config, storage)?;

        let peers = peers
            .into_iter()
            .map(|(id, addr)| (id, PeerClient::new(addr)))
            .collect();

        Ok(Self {
            raft_group: raft,
            rx,
            peers,
        })
    }

    /// Runs the Raft node event loop.
    ///
    /// This method will run indefinitely until the server is shut down or encounters an
    /// unrecoverable error.
    ///
    /// # Errors
    ///
    /// Returns an error if Raft processing fails.
    pub async fn run(mut self) -> Result<()> {
        let mut timeout = RAFT_TIMEOUT;
        loop {
            let start = Instant::now();

            select! {
                message = self.rx.recv() => {
                    match message {
                        Some(Message::RaftMessages(messages)) => {
                            for message in messages {
                                self.raft_group.step(message)?;
                            }
                            self.on_ready().await?;
                        }
                        Some(Message::Command(data)) => {
                            // TODO: This is an unfortunate copy of `data`, there are no other
                            // references to the underlying bytes.
                            match self.raft_group.propose(Vec::new(), data.to_vec()) {
                                Ok(()) => {
                                    self.on_ready().await?;
                                }
                                Err(e) => {
                                    // TODO: Handle errors.
                                    println!("PROPOSE ERROR: {e:?}");
                                },
                            }

                        }
                        None => break,
                    }
                    timeout = timeout.saturating_sub(start.elapsed());
                }

                () = tokio::time::sleep(timeout) => {
                    if self.raft_group.tick() {
                        self.on_ready().await?;
                    }
                    timeout = RAFT_TIMEOUT;
                }
            }
        }

        Ok(())
    }

    // TODO: When do we actually want to call this? Does calling it after every message reduce
    // batching?
    async fn on_ready(&mut self) -> Result<()> {
        if !self.raft_group.has_ready() {
            return Ok(());
        }

        let mut ready = self.raft_group.ready();

        // Handle ready tasks.
        self.handle_messages(ready.take_messages()).await?;
        let store = self.raft_group.raft.raft_log.store();
        if !ready.snapshot().is_empty() {
            unimplemented!("snapshots are not yet supported");
        }
        self.handle_committed_entries(ready.take_committed_entries())
            .await?;
        if !ready.entries().is_empty() {
            store.append(ready.entries()).await?;
        }
        if let Some(hard_state) = ready.hs() {
            store.set_hard_state(hard_state.clone()).await;
        }
        self.handle_messages(ready.take_persisted_messages())
            .await?;

        // Advance the Raft state machine.
        let mut light_ready = self.raft_group.advance(ready);

        let store = self.raft_group.raft.raft_log.store();

        // Handle light-ready tasks.
        if let Some(commit_index) = light_ready.commit_index() {
            store.set_commit_index(commit_index).await;
        }
        self.handle_messages(light_ready.take_messages()).await?;
        self.handle_committed_entries(light_ready.take_committed_entries())
            .await?;

        // Advance the Raft state machine again.
        self.raft_group.advance_apply();

        Ok(())
    }

    async fn handle_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        let messages = messages
            .into_iter()
            .into_group_map_by(raft::prelude::Message::get_to);
        let mut peer_futures = Vec::with_capacity(messages.len());
        for (to, messages) in messages {
            let mut peer = self
                .peers
                .remove(&to)
                .ok_or_else(|| format!("unknown peer: {to}"))?;
            let future = async move {
                // We can ignore errors, Raft will retry messages if it's necessary.
                // TODO: Debug or trace this once logging is set up.
                let _result = peer.send_messages(messages).await;
                (to, peer)
            };
            peer_futures.push(future);
        }
        let peers = futures::future::join_all(peer_futures).await;
        self.peers.extend(peers.into_iter());

        Ok(())
    }

    #[expect(clippy::unused_async)]
    async fn handle_committed_entries(&self, committed_entries: Vec<Entry>) -> Result<()> {
        for entry in committed_entries {
            // TODO: Actually apply the entry.
            println!("COMMITTED: {entry:?}");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct NodeHandle {
    tx: mpsc::UnboundedSender<Message>,
}

#[tonic::async_trait]
impl KvService for NodeHandle {
    async fn command(
        &self,
        request: Request<CommandRequest>,
    ) -> std::result::Result<Response<CommandResponse>, Status> {
        let CommandRequest { data } = request.into_inner();
        self.tx
            .send(Message::Command(data))
            .map_err(|_| Status::unavailable("Server is shutting down"))?;
        Ok(Response::new(CommandResponse {}))
    }
}

#[tonic::async_trait]
impl RaftService for NodeHandle {
    async fn message(
        &self,
        request: Request<MessageRequest>,
    ) -> std::result::Result<Response<MessageResponse>, Status> {
        let MessageRequest { messages_bytes } = request.into_inner();
        let messages = messages_bytes
            .into_iter()
            .map(|message_bytes| {
                raft::prelude::Message::parse_from_carllerche_bytes(&message_bytes)
            })
            .collect::<ProtobufResult<Vec<_>>>()
            .map_err(|_| Status::invalid_argument("Failed to parse raft message"))?;
        self.tx
            .send(Message::RaftMessages(messages))
            .map_err(|_| Status::unavailable("Server is shutting down"))?;
        Ok(Response::new(MessageResponse {}))
    }
}

struct PeerClient {
    addr: String,
    client: Option<RaftServiceClient<Channel>>,
}

impl PeerClient {
    fn new(addr: String) -> Self {
        Self { addr, client: None }
    }

    async fn send_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        let client = if let Some(client) = &mut self.client {
            client
        } else {
            let new_client = RaftServiceClient::connect(self.addr.clone()).await?;
            self.client.insert(new_client)
        };

        let messages_bytes = messages
            .into_iter()
            .map(|message| message.write_to_bytes().map(Bytes::from))
            .collect::<ProtobufResult<Vec<_>>>()?;

        client
            .message(Request::new(MessageRequest { messages_bytes }))
            .await?;
        Ok(())
    }
}

/// Initializes a new Raft node and returns it along with a handle to interact with it.
///
/// # Errors
///
/// Returns an error if the Raft node fails to initialize.
pub async fn initialize<T: Storage>(
    id: u64,
    peers: HashMap<u64, String>,
) -> Result<(Node<T>, NodeHandle)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let node = Node::new(id, peers, rx).await?;
    let node_handle = NodeHandle { tx };
    Ok((node, node_handle))
}
