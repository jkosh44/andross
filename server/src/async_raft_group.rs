//! This module is responsible for converting all synchronous methods from the [`raft`] crate and
//! [`LogStorage`] extension trait into asynchronous methods. The current approach is to wrap each
//! method in a call to [`tokio::task::spawn_blocking`]. This module takes an overly cautious
//! approach by wrapping ALL methods instead of investigating each one to determine if it is
//! blocking.
//!
//! An alternative approach could be to run the [`RawNode`] on a dedicated thread and communicate
//! with it via channels.

use crate::log_storage::LogStorage;
use raft::eraftpb::{Entry, HardState, Message};
use raft::{LightReady, RawNode, Ready};
use tokio::sync::{mpsc, oneshot};

enum RawNodeMessage {
    Tick {
        response_tx: oneshot::Sender<bool>,
    },
    Propose {
        context: Vec<u8>,
        data: Vec<u8>,
        response_tx: oneshot::Sender<raft::Result<()>>,
    },
    Step {
        m: Message,
        response_tx: oneshot::Sender<raft::Result<()>>,
    },
    Ready {
        response_tx: oneshot::Sender<Ready>,
    },
    HasReady {
        response_tx: oneshot::Sender<bool>,
    },
    Advance {
        rd: Ready,
        response_tx: oneshot::Sender<LightReady>,
    },
    AdvanceApply {
        response_tx: oneshot::Sender<()>,
    },
    Append {
        entries: Vec<Entry>,
        response_tx: oneshot::Sender<raft::Result<()>>,
    },
    SetHardState {
        hard_state: HardState,
        response_tx: oneshot::Sender<raft::Result<()>>,
    },
    SetCommitIndex {
        commit_index: u64,
        response_tx: oneshot::Sender<raft::Result<()>>,
    },
}

/// Wrapper around [`RawNode`] that wraps every method in an async version.
pub(crate) struct AsyncRawNode {
    id: u64,
    tx: mpsc::UnboundedSender<RawNodeMessage>,
}

impl AsyncRawNode {
    /// Creates a new [`AsyncRawNode`]
    pub(crate) fn new<T: LogStorage + Send + 'static>(raft_group: RawNode<T>) -> Self {
        let id = raft_group.raft.id;
        let (tx, rx) = mpsc::unbounded_channel();
        let raw_node_thread = RawNodeThread::new(raft_group, rx);
        std::thread::spawn(move || raw_node_thread.run());
        Self { id, tx }
    }

    /// See [`RawNode::tick`].
    pub(crate) async fn tick(&mut self) -> bool {
        self.execute(|response_tx| RawNodeMessage::Tick { response_tx })
            .await
    }

    /// See [`RawNode::propose`].
    pub(crate) async fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> raft::Result<()> {
        self.execute(|response_tx| RawNodeMessage::Propose {
            context,
            data,
            response_tx,
        })
        .await
    }

    /// See [`RawNode::step`].
    pub(crate) async fn step(&mut self, m: Message) -> raft::Result<()> {
        self.execute(|response_tx| RawNodeMessage::Step { m, response_tx })
            .await
    }

    /// See [`RawNode::ready`].
    pub(crate) async fn ready(&mut self) -> Ready {
        self.execute(|response_tx| RawNodeMessage::Ready { response_tx })
            .await
    }

    /// See [`RawNode::has_ready`].
    pub(crate) async fn has_ready(&mut self) -> bool {
        self.execute(|response_tx| RawNodeMessage::HasReady { response_tx })
            .await
    }

    /// See [`RawNode::advance`].
    pub(crate) async fn advance(&mut self, rd: Ready) -> LightReady {
        self.execute(|response_tx| RawNodeMessage::Advance { rd, response_tx })
            .await
    }

    /// See [`RawNode::advance_apply`].
    pub(crate) async fn advance_apply(&mut self) {
        self.execute(|response_tx| RawNodeMessage::AdvanceApply { response_tx })
            .await;
    }

    /// Returns the ID of this node.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn mut_store(&mut self) -> AsyncLogStorage<'_> {
        AsyncLogStorage::new(self)
    }

    async fn execute<R>(&self, f: impl FnOnce(oneshot::Sender<R>) -> RawNodeMessage) -> R {
        let (tx, rx) = oneshot::channel();
        let msg = f(tx);
        self.tx
            .send(msg)
            .expect("thread only stops when the channel is closed");
        rx.await
            .expect("thread only stops when the channel is closed")
    }
}

/// Wrapper around a [`LogStorage`] that wraps every method in an async version.
pub(crate) struct AsyncLogStorage<'a> {
    async_raw_node: &'a mut AsyncRawNode,
}

impl<'a> AsyncLogStorage<'a> {
    fn new(raft_group: &'a mut AsyncRawNode) -> Self {
        Self {
            async_raw_node: raft_group,
        }
    }

    /// See [`LogStorage::append`].
    pub(crate) async fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        self.async_raw_node
            .execute(|response_tx| RawNodeMessage::Append {
                entries,
                response_tx,
            })
            .await
    }

    /// See [`LogStorage::set_hard_state`].
    pub(crate) async fn set_hard_state(&mut self, hard_state: HardState) -> raft::Result<()> {
        self.async_raw_node
            .execute(|response_tx| RawNodeMessage::SetHardState {
                hard_state,
                response_tx,
            })
            .await
    }

    /// See [`LogStorage::set_commit_index`].
    pub(crate) async fn set_commit_index(&mut self, commit_index: u64) -> raft::Result<()> {
        self.async_raw_node
            .execute(|response_tx| RawNodeMessage::SetCommitIndex {
                commit_index,
                response_tx,
            })
            .await
    }
}

/// Dedicated synchronous thread that runs the [`RawNode`] and underlying [`LogStorage`].
struct RawNodeThread<T: LogStorage> {
    raft_group: RawNode<T>,
    rx: mpsc::UnboundedReceiver<RawNodeMessage>,
}

impl<T: LogStorage> RawNodeThread<T> {
    fn new(raft_group: RawNode<T>, rx: mpsc::UnboundedReceiver<RawNodeMessage>) -> Self {
        Self { raft_group, rx }
    }

    fn run(mut self) {
        while let Some(msg) = self.rx.blocking_recv() {
            match msg {
                RawNodeMessage::Tick { response_tx } => {
                    let tick = self.raft_group.tick();
                    response_tx.send(tick).expect("senders never hang up");
                }
                RawNodeMessage::Propose {
                    context,
                    data,
                    response_tx,
                } => {
                    let result = self.raft_group.propose(context, data);
                    response_tx.send(result).expect("senders never hang up");
                }
                RawNodeMessage::Step { m, response_tx } => {
                    let result = self.raft_group.step(m);
                    response_tx.send(result).expect("senders never hang up");
                }
                RawNodeMessage::Ready { response_tx } => {
                    let ready = self.raft_group.ready();
                    response_tx.send(ready).expect("senders never hang up");
                }
                RawNodeMessage::HasReady { response_tx } => {
                    let has_ready = self.raft_group.has_ready();
                    response_tx.send(has_ready).expect("senders never hang up");
                }
                RawNodeMessage::Advance { rd, response_tx } => {
                    let light_ready = self.raft_group.advance(rd);
                    response_tx
                        .send(light_ready)
                        .expect("senders never hang up");
                }
                RawNodeMessage::AdvanceApply { response_tx } => {
                    self.raft_group.advance_apply();
                    response_tx.send(()).expect("senders never hang up");
                }
                RawNodeMessage::Append {
                    entries,
                    response_tx,
                } => {
                    let log_storage = self.raft_group.mut_store();
                    let result = log_storage.append(&entries);
                    response_tx.send(result).expect("senders never hang up");
                }
                RawNodeMessage::SetHardState {
                    hard_state,
                    response_tx,
                } => {
                    let log_storage = self.raft_group.mut_store();
                    let result = log_storage.set_hard_state(hard_state);
                    response_tx.send(result).expect("senders never hang up");
                }
                RawNodeMessage::SetCommitIndex {
                    commit_index,
                    response_tx,
                } => {
                    let log_storage = self.raft_group.mut_store();
                    let result = log_storage.set_commit_index(commit_index);
                    response_tx.send(result).expect("senders never hang up");
                }
            }
        }
    }
}
