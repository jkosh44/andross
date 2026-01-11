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
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

enum Message {
    Command {
        data: Bytes,
        response_tx: oneshot::Sender<std::result::Result<(), Status>>,
    },
    CommandTimeout {
        command_id: u64,
    },
    RaftMessages(Vec<raft::prelude::Message>),
}

pub struct Node<T: Storage> {
    raft_group: RawNode<T>,
    peers: HashMap<u64, PeerClient>,

    next_command_id: u64,
    pending_commands: HashMap<u64, oneshot::Sender<std::result::Result<(), Status>>>,
    raft_tick_interval: Duration,
    default_request_timeout: Duration,

    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>,
}

impl<T: Storage> Node<T> {
    async fn new(
        id: u64,
        peers: HashMap<u64, String>,
        raft_tick_interval: Duration,
        default_request_timeout: Duration,
        tx: mpsc::UnboundedSender<Message>,
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
            peers,
            next_command_id: u64::MIN,
            pending_commands: HashMap::new(),
            raft_tick_interval,
            default_request_timeout,
            tx,
            rx,
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
    pub async fn run(mut self, cancellation_token: CancellationToken) -> Result<()> {
        let mut timeout = self.raft_tick_interval;
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
                        Some(Message::Command{ data, response_tx }) => {
                            let command_context = self.allocate_command_context()?;
                            let command_id = command_context.command_id;

                            // TODO: This is an unfortunate copy of `data`, there are no other
                            // references to the underlying bytes.
                            match self.raft_group.propose(command_context.into_bytes(), data.to_vec()) {
                                Ok(()) => {
                                    let previous = self.pending_commands.insert(command_id, response_tx);
                                    assert!(previous.is_none(), "command ID reuse");
                                    let request_timeout = self.default_request_timeout;
                                    let tx = self.tx.clone();
                                    // An accepted proposal does not mean that the command will be
                                    // committed, so we eventually have to time it out. This avoids
                                    // memory leaks in `pending_commands` and prevents clients from
                                    // waiting around forever if the command will never commit.
                                    //
                                    // It's tempting to not have any timeout and rely only on
                                    // clients to timeout requests when they're tired of waiting.
                                    // However, if this node is not a leader, then it has no way of
                                    // even knowing if the proposal was received and accepted by the
                                    // leader. It's possible that the network message was dropped
                                    // and we'll wait forever. We also can't blindly retry because
                                    // we don't know if the command is idempotent. So we must
                                    // eventually time a command out or requests will wait on
                                    // dropped proposals forever.
                                    //
                                    // If we disable proposal forwarding (followers forwarding
                                    // proposals to leaders), then we know that the proposal will
                                    // only be accepted by leaders and will get written down to the
                                    // leaders log. We could detect when/if that log entry was
                                    // truncated and abort the request without needing a timeout.
                                    // Unfortunately, that doesn't seem possible to do with the
                                    // current Raft library.
                                    //
                                    // Other similar systems abort all in-flight requests when the
                                    // term changes.
                                    tokio::spawn(async move {
                                        tokio::time::sleep(request_timeout).await;
                                        // Ignore errors if the server is shutting down.
                                        let _ = tx.send(Message::CommandTimeout { command_id });
                                    });

                                    self.on_ready().await?;
                                }
                                Err(e) => {
                                    // Ignore errors if the client has hung up.
                                    let _ = response_tx.send(Err(Status::failed_precondition(e.to_string())));
                                },
                            }

                        }
                        Some(Message::CommandTimeout{ command_id }) => {
                            if let Some(response_tx) = self.pending_commands.remove(&command_id) {
                                // This is an indeterminate error, the command may still succeed.
                                let _ = response_tx.send(Err(Status::deadline_exceeded("indeterminate error, command timed out")));
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
                    timeout = self.raft_tick_interval;
                }

                () = cancellation_token.cancelled() => break,
            }
        }

        Ok(())
    }

    async fn on_ready(&mut self) -> Result<()> {
        while self.raft_group.has_ready() {
            let mut ready = self.raft_group.ready();

            // Handle ready tasks.
            self.handle_messages(ready.take_messages()).await?;
            if !ready.snapshot().is_empty() {
                unimplemented!("snapshots are not yet supported");
            }
            self.handle_committed_entries(ready.take_committed_entries())
                .await?;
            if !ready.entries().is_empty() {
                self.store().append(ready.entries()).await?;
            }
            if let Some(hard_state) = ready.hs() {
                self.store().set_hard_state(hard_state.clone()).await;
            }
            self.handle_messages(ready.take_persisted_messages())
                .await?;

            // Advance the Raft state machine.
            let mut light_ready = self.raft_group.advance(ready);

            // Handle light-ready tasks.
            if let Some(commit_index) = light_ready.commit_index() {
                self.store().set_commit_index(commit_index).await;
            }
            self.handle_messages(light_ready.take_messages()).await?;
            self.handle_committed_entries(light_ready.take_committed_entries())
                .await?;

            // Advance the Raft state machine again.
            self.raft_group.advance_apply();
        }

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
    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Result<()> {
        for entry in committed_entries {
            // TODO: Actually apply the entry.
            let command_context = CommandContext::from_bytes(&entry.context);
            if let Ok(command_context) = command_context
                && command_context.node_id == self.raft_group.raft.id
                && let Some(response_tx) = self.pending_commands.remove(&command_context.command_id)
            {
                // Ignore errors if the client has hung up.
                let _ = response_tx.send(Ok(()));
            }
            println!("COMMITTED: {entry:?}");
        }
        Ok(())
    }

    fn allocate_command_context(&mut self) -> Result<CommandContext> {
        let command_id = self.next_command_id;
        self.next_command_id = self
            .next_command_id
            .checked_add(1)
            .ok_or("command ID overflow")?;
        Ok(CommandContext::new(self.raft_group.raft.id, command_id))
    }

    fn store(&self) -> &T {
        self.raft_group.raft.raft_log.store()
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
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(Message::Command { data, response_tx })
            .map_err(|_| Status::unavailable("Server is shutting down"))?;
        match response_rx.await {
            Ok(Ok(())) => Ok(Response::new(CommandResponse {})),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::unavailable("Server is shutting down")),
        }
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

#[derive(Eq, PartialEq, Debug, Clone)]
struct CommandContext {
    node_id: u64,
    command_id: u64,
}

impl CommandContext {
    fn new(node_id: u64, command_id: u64) -> Self {
        Self {
            node_id,
            command_id,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(size_of::<u64>() * 2);
        bytes.extend_from_slice(&self.node_id.to_le_bytes());
        bytes.extend_from_slice(&self.command_id.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (node_id_bytes, command_id_bytes) = bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid command context: {bytes:?}"))?;
        let node_id = u64::from_le_bytes(*node_id_bytes);
        let command_id_bytes = command_id_bytes
            .try_into()
            .map_err(|_| format!("invalid command context: {bytes:?}"))?;
        let command_id = u64::from_le_bytes(command_id_bytes);
        Ok(Self::new(node_id, command_id))
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
    raft_tick_interval: Duration,
    default_request_timeout: Duration,
) -> Result<(Node<T>, NodeHandle)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let node = Node::new(
        id,
        peers,
        raft_tick_interval,
        default_request_timeout,
        tx.clone(),
        rx,
    )
    .await?;
    let node_handle = NodeHandle { tx };
    Ok((node, node_handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use raft::storage::MemStorage;

    proptest! {
        #[test]
        fn test_command_context_roundtrip(node_id in any::<u64>(), command_id in any::<u64>()) {
            let context = CommandContext::new(node_id, command_id);
            let bytes = context.clone().into_bytes();
            let deserialized = CommandContext::from_bytes(&bytes).unwrap();
            assert_eq!(context, deserialized);
        }
    }

    #[tokio::test]
    async fn test_single_node_command() {
        let (node, node_handle) = initialize::<MemStorage>(
            1,
            HashMap::new(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let cancellation_token = CancellationToken::new();
        let task_handle = {
            let cancellation_token = cancellation_token.clone();
            tokio::spawn(async move {
                node.run(cancellation_token).await.unwrap();
            })
        };

        let data = Bytes::from("hello");
        for _ in 0..500 {
            let request = Request::new(CommandRequest { data: data.clone() });
            match node_handle.command(request).await {
                Ok(response) => {
                    assert_eq!(response.into_inner(), CommandResponse {});
                    cancellation_token.cancel();
                    task_handle.await.unwrap();
                    return;
                }
                // Not a leader yet, so keep waiting.
                Err(e) if e.code() == tonic::Code::FailedPrecondition => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        panic!("test timed out");
    }
}
