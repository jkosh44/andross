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
use tokio::io;

/// Wrapper around [`RawNode`] that wraps every method in an async version.
pub(crate) struct AsyncRawNode<T: LogStorage> {
    raft_group: Option<RawNode<T>>,
}

impl<T: LogStorage + Send + 'static> AsyncRawNode<T> {
    /// Creates a new [`AsyncRawNode`]
    pub(crate) fn new(raft_group: RawNode<T>) -> Self {
        Self {
            raft_group: Some(raft_group),
        }
    }

    /// See [`RawNode::tick`].
    pub(crate) async fn tick(&mut self) -> bool {
        self.spawn_blocking(RawNode::tick).await
    }

    /// See [`RawNode::propose`].
    pub(crate) async fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> raft::Result<()> {
        self.spawn_blocking(move |raft_group| raft_group.propose(context, data))
            .await
    }

    /// See [`RawNode::step`].
    pub(crate) async fn step(&mut self, m: Message) -> raft::Result<()> {
        self.spawn_blocking(move |raft_group| raft_group.step(m))
            .await
    }

    /// See [`RawNode::ready`].
    pub(crate) async fn ready(&mut self) -> Ready {
        self.spawn_blocking(RawNode::ready).await
    }

    /// See [`RawNode::has_ready`].
    pub(crate) async fn has_ready(&mut self) -> bool {
        self.spawn_blocking(|raft_group| raft_group.has_ready())
            .await
    }

    /// See [`RawNode::advance`].
    pub(crate) async fn advance(&mut self, rd: Ready) -> LightReady {
        self.spawn_blocking(move |raft_group| raft_group.advance(rd))
            .await
    }

    /// See [`RawNode::advance_apply`].
    pub(crate) async fn advance_apply(&mut self) {
        self.spawn_blocking(RawNode::advance_apply).await;
    }

    /// Returns the ID of this node.
    pub(crate) fn id(&self) -> u64 {
        self.raft_group_ref().raft.id
    }

    pub(crate) fn mut_store(&mut self) -> AsyncLogStorage<'_, T> {
        AsyncLogStorage::new(self)
    }

    async fn spawn_blocking<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut RawNode<T>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let mut raft_group = self.raft_group.take().expect("raft group in invalid state");
        let (r, raft_group) = tokio::task::spawn_blocking(move || {
            let r = f(&mut raft_group);
            (r, raft_group)
        })
        .await
        .expect("thread panicked");
        self.raft_group = Some(raft_group);
        r
    }

    async fn spawn_storage<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_blocking(move |raft_group| {
            let log_storage = raft_group.mut_store();
            f(log_storage)
        })
        .await
    }

    fn raft_group_ref(&self) -> &RawNode<T> {
        self.raft_group
            .as_ref()
            .expect("raft group in invalid state")
    }
}

/// Wrapper around a [`LogStorage`] that wraps every method in an async version.
pub(crate) struct AsyncLogStorage<'a, T: LogStorage> {
    raft_group: &'a mut AsyncRawNode<T>,
}

impl<'a, T: LogStorage + Send + 'static> AsyncLogStorage<'a, T> {
    fn new(raft_group: &'a mut AsyncRawNode<T>) -> Self {
        Self { raft_group }
    }

    /// See [`LogStorage::append`].
    pub(crate) async fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        self.raft_group
            .spawn_storage(move |log_storage| log_storage.append(&entries))
            .await
    }

    /// See [`LogStorage::set_hard_state`].
    pub(crate) async fn set_hard_state(&mut self, hard_state: HardState) -> io::Result<()> {
        self.raft_group
            .spawn_storage(move |log_storage| log_storage.set_hard_state(hard_state))
            .await
    }

    /// See [`LogStorage::set_commit_index`].
    pub(crate) async fn set_commit_index(&mut self, commit_index: u64) -> io::Result<()> {
        self.raft_group
            .spawn_storage(move |log_storage| log_storage.set_commit_index(commit_index))
            .await
    }
}
