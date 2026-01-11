//! Raft node implementation.
//!
//! This module contains the `Node` struct which manages the Raft state machine
//! and handles communication with other nodes in the cluster.

use crate::Result;
use crate::storage::Storage;
use andross_service::kv::raft_service_server::RaftService;
use andross_service::kv::{CommandRequest, CommandResponse, MessageRequest, MessageResponse};
use raft::prelude::{ConfState, Entry};
use raft::{Config, RawNode};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

// TODO: This seems too high, this is basically the lower bound on write latency.
/// Timeout in-between Raft ticks.
const RAFT_TIMEOUT: Duration = Duration::from_millis(100);

enum Message {
    Command(Vec<u8>),
    #[expect(dead_code)]
    RaftMessage(raft::prelude::Message),
}

pub struct Node<T: Storage> {
    raft_group: RawNode<T>,
    rx: mpsc::UnboundedReceiver<Message>,
}

impl<T: Storage> Node<T> {
    async fn new(
        id: u64,
        peers: HashSet<u64>,
        rx: mpsc::UnboundedReceiver<Message>,
    ) -> Result<Self> {
        let voters = std::iter::once(id).chain(peers.iter().copied()).collect();
        let conf_state = ConfState {
            voters,
            ..ConfState::default()
        };
        let storage = T::init(conf_state).await;
        let config = Config::new(id);
        let raft = RawNode::with_default_logger(&config, storage)?;
        Ok(Self {
            raft_group: raft,
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
    pub async fn run(mut self) -> Result<()> {
        let mut timeout = RAFT_TIMEOUT;
        loop {
            let start = Instant::now();

            select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(Message::RaftMessage(msg)) => {
                            self.raft_group.step(msg)?;
                        }
                        Some(Message::Command(data)) => {
                            match self.raft_group.propose(Vec::new(), data) {
                                Ok(()) => {}
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
                        // TODO: We may also want to check on_ready when receiving messages or
                        // command data. That might lessen the amount of batching, but decreases
                        // latency for individual requests.
                        self.on_ready().await?;
                    }
                    timeout = RAFT_TIMEOUT;
                }
            }
        }

        Ok(())
    }

    async fn on_ready(&mut self) -> Result<()> {
        if !self.raft_group.has_ready() {
            return Ok(());
        }

        let mut ready = self.raft_group.ready();
        let store = self.raft_group.raft.raft_log.store();

        // Handle ready tasks.
        self.handle_messages(ready.take_messages()).await?;
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

    #[expect(clippy::unused_async)]
    async fn handle_messages(&self, msgs: Vec<raft::prelude::Message>) -> Result<()> {
        for _msg in msgs {
            todo!()
        }
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

pub struct NodeHandle {
    tx: mpsc::UnboundedSender<Message>,
}

#[tonic::async_trait]
impl RaftService for NodeHandle {
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

    async fn message(
        &self,
        _request: Request<MessageRequest>,
    ) -> std::result::Result<Response<MessageResponse>, Status> {
        // TODO: implement
        unimplemented!()
    }
}

/// Initializes a new Raft node and returns it along with a handle to interact with it.
///
/// # Errors
///
/// Returns an error if the Raft node fails to initialize.
pub async fn initialize<T: Storage>(id: u64, peers: HashSet<u64>) -> Result<(Node<T>, NodeHandle)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let node = Node::new(id, peers, rx).await?;
    let node_handle = NodeHandle { tx };
    Ok((node, node_handle))
}
