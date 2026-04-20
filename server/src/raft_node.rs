//! Raft node implementation.
//!
//! This module contains the `Node` struct which manages the Raft state machine
//! and handles communication with other nodes in the cluster.

use crate::Result;
use crate::async_raft_group::{AsyncLogStorage, AsyncRawNode};
use crate::encodings::{Command, CommandInner, CommandKind};
use crate::log_storage::LogStorage;
use crate::service::kv_service_server::KvService;
use crate::service::raft_service_client::RaftServiceClient;
use crate::service::raft_service_server::RaftService;
use crate::service::{CommandRequest, CommandResponse, MessageRequest, MessageResponse};
use bytes::Bytes;
use fjall::KeyspaceCreateOptions;
use protobuf::{Message as ProtobufMessage, ProtobufResult};
use raft::prelude::{ConfState, Entry};
use raft::{Config, RawNode};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};

// TODO: Support multiple keyspaces.
/// Fjall keyspace where all data is stored.
const GLOBAL_KEYSPACE: &str = "andross";

pub enum Message {
    Command {
        command: Command,
        response_tx: oneshot::Sender<std::result::Result<Option<Bytes>, Status>>,
    },
    CommandTimeout {
        command_id: u64,
    },
    RaftMessages(Vec<raft::prelude::Message>),
}

pub struct NodeConfig<T: LogStorage> {
    id: u64,
    peers: HashMap<u64, Uri>,
    raft_tick_interval: Duration,
    default_request_timeout: Duration,
    log_storage: T,
    db: fjall::Database,
    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>,
}

pub struct Node {
    raft_group: AsyncRawNode,

    next_command_id: u64,
    pending_commands: HashMap<u64, oneshot::Sender<std::result::Result<Option<Bytes>, Status>>>,
    raft_tick_interval: Duration,
    default_request_timeout: Duration,

    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>,

    wal_applier_tx: mpsc::UnboundedSender<CommittedEntry>,
    wal_applier_handle: thread::JoinHandle<Result<()>>,

    message_sender_tx: mpsc::UnboundedSender<raft::prelude::Message>,
    message_sender_handle: tokio::task::JoinHandle<Result<()>>,
}

impl Node {
    async fn new<T: LogStorage + Send + 'static>(
        NodeConfig {
            id,
            peers,
            raft_tick_interval,
            default_request_timeout,
            mut log_storage,
            db,
            tx,
            rx,
        }: NodeConfig<T>,
    ) -> Result<Self> {
        let voters = std::iter::once(id).chain(peers.keys().copied()).collect();
        let conf_state = ConfState {
            voters,
            ..ConfState::default()
        };
        let log_storage = spawn_blocking(move || {
            log_storage.set_conf_state(conf_state);
            log_storage
        })
        .await
        .expect("thread panicked");
        let config = Config::new(id);
        let raft_group = RawNode::with_default_logger(&config, log_storage)?;
        let raft_group = AsyncRawNode::new(raft_group);

        let peers = peers
            .into_iter()
            .map(|(id, addr)| (id, PeerClient::new(addr)))
            .collect();

        let (wal_applier_tx, wal_applier_rx) = mpsc::unbounded_channel();
        let wal_applier = WalApplier::new(wal_applier_rx, db);
        let wal_applier_handle = thread::spawn(move || wal_applier.run());

        let (message_sender_tx, message_sender_rx) = mpsc::unbounded_channel();
        let message_sender = MessageSender::new(message_sender_rx, peers);
        let message_sender_handle = tokio::spawn(async move { message_sender.run().await });

        Ok(Self {
            raft_group,
            next_command_id: u64::MIN,
            pending_commands: HashMap::new(),
            raft_tick_interval,
            default_request_timeout,
            tx,
            rx,
            wal_applier_tx,
            wal_applier_handle,
            message_sender_tx,
            message_sender_handle,
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
        let mut ticker = tokio::time::interval(self.raft_tick_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            select! {
                message = self.rx.recv() => {
                    match message {
                        Some(Message::RaftMessages(messages)) => {
                            for message in messages {
                                self.raft_group.step(message).await?;
                            }
                            self.on_ready().await?;
                        }
                        Some(Message::Command{ command, response_tx }) => {
                            let command_kind = command.kind();
                            let command_context = self.allocate_command_context(command_kind)?;
                            let command_id = command_context.command_id;

                            // TODO: This is an unfortunate copy of `data`, there are no other
                            // references to the underlying bytes.
                            //
                            // Both writes and reads are replicated by Raft as a simple way to
                            // linearize all reads and writes. Replication reads through Raft is
                            // fairly expensive, because we end writing the read down on disk,
                            // turning every read into a write. Ideally, reads are done outside of
                            // Raft to avoid this expense.
                            match self.raft_group.propose(command_context.into_bytes(), command.into_bytes().to_vec()).await {
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
                }

                _ = ticker.tick() => {
                    if self.raft_group.tick().await {
                        self.on_ready().await?;
                    }
                }

                () = cancellation_token.cancelled() => break,
            }
        }

        drop(self.wal_applier_tx);
        spawn_blocking(move || self.wal_applier_handle.join().expect("thread panicked")).await??;

        drop(self.message_sender_tx);
        self.message_sender_handle.await??;

        Ok(())
    }

    async fn on_ready(&mut self) -> Result<()> {
        while self.raft_group.has_ready().await {
            let mut ready = self.raft_group.ready().await;

            // Handle ready tasks.
            self.handle_messages(ready.take_messages())?;
            if !ready.snapshot().is_empty() {
                unimplemented!("snapshots are not yet supported");
            }
            self.handle_committed_entries(ready.take_committed_entries())?;
            if !ready.entries().is_empty() {
                self.mut_store().append(ready.take_entries()).await?;
            }
            if let Some(hard_state) = ready.hs() {
                self.mut_store().set_hard_state(hard_state.clone()).await?;
            }
            self.handle_messages(ready.take_persisted_messages())?;

            // Advance the Raft state machine.
            let mut light_ready = self.raft_group.advance(ready).await;

            // Handle light-ready tasks.
            if let Some(commit_index) = light_ready.commit_index() {
                self.mut_store().set_commit_index(commit_index).await?;
            }
            self.handle_messages(light_ready.take_messages())?;
            self.handle_committed_entries(light_ready.take_committed_entries())?;

            // Advance the Raft state machine again.
            self.raft_group.advance_apply().await;
        }

        Ok(())
    }

    fn handle_messages(&mut self, messages: Vec<raft::prelude::Message>) -> Result<()> {
        // TODO batch messages.
        for message in messages {
            self.message_sender_tx.send(message)?;
        }
        Ok(())
    }

    fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Result<()> {
        // TODO: batch entries.
        for entry in committed_entries {
            if !entry.data.is_empty() {
                let command_context = CommandContext::from_bytes(&entry.context).ok();
                let response_tx = command_context.and_then(|command_context| {
                    if command_context.node_id == self.raft_group.id() {
                        self.pending_commands.remove(&command_context.command_id)
                    } else {
                        None
                    }
                });
                self.wal_applier_tx
                    .send(CommittedEntry { entry, response_tx })?;
            }
        }

        Ok(())
    }

    fn allocate_command_context(&mut self, command_kind: CommandKind) -> Result<CommandContext> {
        let command_id = self.next_command_id;
        self.next_command_id = self
            .next_command_id
            .checked_add(1)
            .ok_or("command ID overflow")?;
        Ok(CommandContext::new(
            self.raft_group.id(),
            command_id,
            command_kind,
        ))
    }

    fn mut_store(&mut self) -> AsyncLogStorage<'_> {
        self.raft_group.mut_store()
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
        let CommandRequest { command_bytes } = request.into_inner();
        let command = Command::new(command_bytes)
            .map_err(|e| Status::invalid_argument(format!("Failed to parse command: {e}")))?;
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(Message::Command {
                command,
                response_tx,
            })
            .map_err(|_| Status::unavailable("Server is shutting down"))?;
        match response_rx.await {
            Ok(Ok(response_bytes)) => Ok(Response::new(CommandResponse { response_bytes })),
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

struct MessageSender {
    rx: mpsc::UnboundedReceiver<raft::prelude::Message>,
    peers: HashMap<u64, PeerClient>,
}

impl MessageSender {
    fn new(
        rx: mpsc::UnboundedReceiver<raft::prelude::Message>,
        peers: HashMap<u64, PeerClient>,
    ) -> Self {
        Self { rx, peers }
    }

    async fn run(mut self) -> Result<()> {
        while let Some(message) = self.rx.recv().await {
            let to = message.get_to();
            let peer = self
                .peers
                .get_mut(&to)
                .ok_or_else(|| format!("unknown peer: {to}"))?;
            // We can ignore errors, Raft will retry messages if it's necessary.
            // TODO: Debug or trace this once logging is set up.
            let _result = peer.send_messages(vec![message]).await;
        }

        Ok(())
    }
}

struct PeerClient {
    addr: Uri,
    client: Option<RaftServiceClient<Channel>>,
}

impl PeerClient {
    fn new(addr: Uri) -> Self {
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

struct CommittedEntry {
    entry: Entry,
    response_tx: Option<oneshot::Sender<std::result::Result<Option<Bytes>, Status>>>,
}

struct WalApplier {
    rx: mpsc::UnboundedReceiver<CommittedEntry>,
    db: fjall::Database,
}

impl WalApplier {
    fn new(rx: mpsc::UnboundedReceiver<CommittedEntry>, db: fjall::Database) -> Self {
        Self { rx, db }
    }

    /// Runs the [`WalApplier`] until it is shut down.
    ///
    /// This if a blocking method and should not be run in an async task.
    fn run(mut self) -> Result<()> {
        while let Some(CommittedEntry { entry, response_tx }) = self.rx.blocking_recv() {
            let command = Command::new(entry.data)?;
            let db = self.db.clone();

            // Raft log is what provides durability, not the LSM tree. Any writes that are
            // not persisted will be replayed from the Raft log.
            let create_options = KeyspaceCreateOptions::default().manual_journal_persist(true);
            let items = db.keyspace(GLOBAL_KEYSPACE, || create_options)?;
            let response = match command.command_inner {
                CommandInner::Insert { tuple } => {
                    let (key, value) = tuple.to_key_value();
                    items.insert(key.as_ref(), value.as_ref())?;
                    None
                }
                CommandInner::Read { key } => {
                    let value = items.get(key)?;
                    value.map(|value| value.to_vec().into())
                }
                CommandInner::Cas { cas_tuple } => {
                    let (key, expected_value, desired_value) = cas_tuple.to_key_and_values();
                    let current_value = items.get(&key)?;

                    let matches_expected = match &current_value {
                        Some(current_value) => current_value.as_ref() == expected_value.as_ref(),
                        None => expected_value.is_empty(),
                    };
                    if matches_expected {
                        items.insert(key.as_ref(), desired_value.as_ref())?;
                        None
                    } else {
                        let current_value =
                            current_value.map_or(Bytes::new(), |value| value.to_vec().into());
                        Some(current_value)
                    }
                }
            };

            if let Some(response_tx) = response_tx {
                // Ignore errors if the client has hung up.
                let _ = response_tx.send(Ok(response));
            }
        }

        Ok(())
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
struct CommandContext {
    node_id: u64,
    command_id: u64,
    command_kind: CommandKind,
}

impl CommandContext {
    fn new(node_id: u64, command_id: u64, command_kind: CommandKind) -> Self {
        Self {
            node_id,
            command_id,
            command_kind,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(size_of::<u64>() * 2 + size_of::<u8>());
        bytes.extend_from_slice(&self.node_id.to_le_bytes());
        bytes.extend_from_slice(&self.command_id.to_le_bytes());
        bytes.push(self.command_kind as u8);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (node_id_bytes, rest_bytes) = bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid command context: {bytes:?}"))?;
        let (command_id_bytes, command_kind) = rest_bytes
            .split_first_chunk()
            .ok_or_else(|| format!("invalid command context: {bytes:?}"))?;
        if command_kind.len() != 1 {
            return Err(format!("invalid command context: {bytes:?}").into());
        }
        let command_kind = command_kind[0];

        let node_id = u64::from_le_bytes(*node_id_bytes);
        let command_id = u64::from_le_bytes(*command_id_bytes);
        let command_kind = command_kind
            .try_into()
            .map_err(|_| format!("invalid command kind: {command_kind}"))?;

        Ok(Self::new(node_id, command_id, command_kind))
    }
}

/// Initializes a new Raft node and returns it along with a handle to interact with it.
///
/// # Errors
///
/// Returns an error if the Raft node fails to initialize.
pub async fn initialize<T: LogStorage + Send + 'static>(
    id: u64,
    peers: HashMap<u64, Uri>,
    raft_tick_interval: Duration,
    default_request_timeout: Duration,
    log_storage: T,
    db: fjall::Database,
) -> Result<(Node, NodeHandle)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let node_config = NodeConfig {
        id,
        peers,
        raft_tick_interval,
        default_request_timeout,
        log_storage,
        db,
        tx: tx.clone(),
        rx,
    };
    let node = Node::new(node_config).await?;
    let node_handle = NodeHandle { tx };
    Ok((node, node_handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_database;
    use proptest::prelude::*;
    use raft::storage::MemStorage;

    proptest! {
        #[test]
        fn test_command_context_roundtrip(node_id in any::<u64>(), command_id in any::<u64>(), command_kind in any::<CommandKind>()) {
            let context = CommandContext::new(node_id, command_id, command_kind);
            let bytes = context.clone().into_bytes();
            let deserialized = CommandContext::from_bytes(&bytes).unwrap();
            assert_eq!(context, deserialized);
        }
    }

    #[tokio::test]
    async fn test_single_node_command() {
        let (db, _temp_dir) = test_database().await;
        let (node, node_handle) = initialize(
            1,
            HashMap::new(),
            Duration::from_millis(1),
            Duration::from_secs(1),
            MemStorage::new(),
            db,
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

        // Insert key-value pair.
        let key = b"k0";
        let value = b"v0";
        let command_bytes = Command::insert(key, value).into_bytes();
        for _ in 0..500 {
            let request = Request::new(CommandRequest {
                command_bytes: command_bytes.clone(),
            });
            match node_handle.command(request).await {
                Ok(response) => {
                    assert_eq!(
                        response.into_inner(),
                        CommandResponse {
                            response_bytes: None
                        }
                    );

                    // Read the value back.
                    let command_bytes = Command::read(key).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, Some(value.as_ref().into()));

                    // Fail to CAS.
                    let command_bytes = Command::cas(key, b"v7", b"v1").into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, Some(value.as_ref().into()));

                    // Successfully CAS.
                    let desired_value = b"v2";
                    let command_bytes = Command::cas(key, value, desired_value).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, None);

                    // Read the value back.
                    let command_bytes = Command::read(key).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, Some(desired_value.as_ref().into()));

                    // Fail to CAS empty key
                    let key = b"k1";
                    let desired_value = b"v3";
                    let command_bytes = Command::cas(key, b"42", desired_value).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, Some(Bytes::new()));

                    // Successfully CAS empty key
                    let key = b"k1";
                    let desired_value = b"v3";
                    let command_bytes = Command::cas(key, b"", desired_value).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, None);

                    // Read the value back.
                    let command_bytes = Command::read(key).into_bytes();
                    let request = Request::new(CommandRequest { command_bytes });
                    let CommandResponse { response_bytes } =
                        node_handle.command(request).await.unwrap().into_inner();
                    assert_eq!(response_bytes, Some(desired_value.as_ref().into()));

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
