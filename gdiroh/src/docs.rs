//! Documents: a key-value store several peers can write to at once.
//!
//! Every peer holds the whole document and may write any key. Edits reconcile
//! automatically, and where two peers wrote the same key the later timestamp
//! wins — so this suits shared state that tolerates a last-writer-wins rule
//! (world edits, inventories, settings) rather than anything needing a
//! transaction.
//!
//! Values are stored as blobs, and live updates travel over gossip, so a
//! document leans on both of those rather than opening its own store or swarm.
//!
//! Godot-free, like the rest of the transport, so it can be driven from a test.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use iroh_blobs::Hash;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::engine::LiveEvent;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use iroh_docs::{AuthorId, DocTicket, NamespaceId};
use iroh_gossip::net::Gossip;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;

use crate::blobs::StoreHandle;
use crate::dispatch::{Claim, Dispatcher};
use crate::runtime::{detach, detach_handle};

/// Something that happened to a document.
pub(crate) enum Event {
    /// The document is open and usable. Arrives once.
    Opened(NamespaceId),
    /// A key was written, by us or by a peer.
    Entry {
        key: Bytes,
        author: AuthorId,
        hash: Hash,
        len: u64,
        /// The peer it came from, or `None` when we wrote it ourselves.
        from: Option<PublicKey>,
    },
    /// A value's bytes finished downloading and can now be read.
    ContentReady(Hash),
    /// Reply to a listing: what is in the document under a prefix.
    Keys {
        prefix: Bytes,
        entries: Vec<KeyInfo>,
    },
    /// Reply to a status request.
    Status {
        syncing: bool,
        subscribers: u32,
        handles: u32,
    },
    /// Reply to a read. `data` is `None` when the key is not set.
    Value {
        key: Bytes,
        data: Option<Bytes>,
    },
    /// A ticket that was asked for.
    Shared(String),
    SyncFinished(PublicKey),
    NeighborUp(PublicKey),
    NeighborDown(PublicKey),
    /// The document ended and will produce nothing further.
    Closed(String),
}

/// One key in a document, without its value.
pub(crate) struct KeyInfo {
    pub key: Bytes,
    pub author: AuthorId,
    pub hash: Hash,
    pub len: u64,
}

enum Command {
    Set { key: Bytes, value: Bytes },
    Delete(Bytes),
    Read(Bytes),
    ListKeys(Bytes),
    Status,
    Share(bool),
    Join(Vec<EndpointAddr>),
    Leave,
}

/// Where a document comes from.
enum Source {
    Create,
    Open(NamespaceId),
    Join(Box<DocTicket>),
}

/// A shareable way to reach the docs engine, starting it on first use.
#[derive(Clone)]
struct DocsHandle {
    cell: Arc<OnceCell<Result<Docs, String>>>,
    endpoint: Endpoint,
    store: StoreHandle,
    gossip: Gossip,
    path: Option<PathBuf>,
}

impl DocsHandle {
    async fn open(&self) -> Result<Docs, String> {
        let built = self
            .cell
            .get_or_init(|| async {
                let store = self.store.open().await?;

                let builder = match &self.path {
                    Some(path) => Docs::persistent(path.clone()),
                    None => Docs::memory(),
                };

                builder
                    .spawn(self.endpoint.clone(), store, self.gossip.clone())
                    .await
                    .map_err(|err| format!("could not start documents: {err}"))
            })
            .await;

        match built {
            Ok(docs) => Ok(docs.clone()),
            Err(err) => Err(err.clone()),
        }
    }
}

/// The document engine on one endpoint, sharing its blob store and gossip.
///
/// Started the first time a document is opened. Built on the same blob store and
/// the same gossip swarm as everything else, rather than a second copy of each.
pub(crate) struct Library {
    handle: DocsHandle,
    /// Held here rather than inside the task below, so dropping this releases
    /// the ALPN straight away instead of whenever the runtime reaps the task.
    _claim: Claim,
    accepting: JoinHandle<()>,
}

impl Library {
    pub(crate) fn start(
        dispatcher: &Dispatcher,
        store: StoreHandle,
        gossip: Gossip,
        path: Option<PathBuf>,
    ) -> Option<Self> {
        let (claim, mut inbound) = dispatcher.register(iroh_docs::ALPN)?.split();
        let handle = DocsHandle {
            cell: Arc::default(),
            endpoint: dispatcher.endpoint().clone(),
            store,
            gossip,
            path,
        };

        let serving = handle.clone();
        let accepting = detach_handle(async move {
            while let Some(connection) = inbound.recv().await {
                let Ok(docs) = serving.open().await else {
                    continue;
                };

                detach(async move {
                    let _ = docs.accept(connection).await;
                });
            }
        })?;

        Some(Self {
            handle,
            _claim: claim,
            accepting,
        })
    }

    /// Makes a new, empty document that only we know about until it is shared.
    pub(crate) fn create(&self) -> Document {
        self.spawn(Source::Create)
    }

    /// Reopens a document already in our store.
    pub(crate) fn open(&self, id: NamespaceId) -> Document {
        self.spawn(Source::Open(id))
    }

    /// Joins a document someone shared, and starts syncing it.
    pub(crate) fn join(&self, ticket: DocTicket) -> Document {
        self.spawn(Source::Join(Box::new(ticket)))
    }

    /// Every document in the store.
    pub(crate) fn list(&self) -> Ask {
        self.query(|api| async move {
            let listed = match api.list().await {
                Ok(listed) => listed,
                Err(err) => return Reply::Failed(err.to_string()),
            };

            let mut ids = Vec::new();
            let mut listed = std::pin::pin!(listed);
            while let Some(entry) = listed.next().await {
                match entry {
                    Ok((id, _capability)) => ids.push(id.to_string()),
                    Err(err) => return Reply::Failed(err.to_string()),
                }
            }
            Reply::Names(ids)
        })
    }

    /// Forgets a document, dropping what we hold of it.
    pub(crate) fn drop_doc(&self, id: NamespaceId) -> Ask {
        self.query(move |api| async move {
            match api.drop_doc(id).await {
                Ok(()) => Reply::Done,
                Err(err) => Reply::Failed(err.to_string()),
            }
        })
    }

    /// Every author identity this endpoint can write as.
    pub(crate) fn authors(&self) -> Ask {
        self.query(|api| async move {
            let listed = match api.author_list().await {
                Ok(listed) => listed,
                Err(err) => return Reply::Failed(err.to_string()),
            };

            let mut authors = Vec::new();
            let mut listed = std::pin::pin!(listed);
            while let Some(author) = listed.next().await {
                match author {
                    Ok(author) => authors.push(author.to_string()),
                    Err(err) => return Reply::Failed(err.to_string()),
                }
            }
            Reply::Names(authors)
        })
    }

    /// Makes a new author identity and reports its id.
    pub(crate) fn create_author(&self) -> Ask {
        self.query(|api| async move {
            match api.author_create().await {
                Ok(author) => Reply::Names(vec![author.to_string()]),
                Err(err) => Reply::Failed(err.to_string()),
            }
        })
    }

    /// Chooses which author later writes are made as.
    pub(crate) fn set_default_author(&self, author: AuthorId) -> Ask {
        self.query(move |api| async move {
            match api.author_set_default(author).await {
                Ok(()) => Reply::Done,
                Err(err) => Reply::Failed(err.to_string()),
            }
        })
    }

    /// Shared plumbing for the one-shot questions above.
    fn query<F, Fut>(&self, work: F) -> Ask
    where
        F: FnOnce(iroh_docs::api::DocsApi) -> Fut + Send + 'static,
        Fut: Future<Output = Reply> + Send,
    {
        let (reply, answer) = oneshot::channel();
        let handle = self.handle.clone();

        detach(async move {
            let outcome = match handle.open().await {
                Ok(docs) => work(docs.api().clone()).await,
                Err(err) => Reply::Failed(err),
            };
            let _ = reply.send(outcome);
        });

        Ask(answer)
    }

    fn spawn(&self, source: Source) -> Document {
        let (events, queue) = mpsc::unbounded_channel();
        let (commands, orders) = mpsc::unbounded_channel();

        let failed = events.clone();
        if !detach(run(self.handle.clone(), source, events, orders)) {
            let _ = failed.send(Event::Closed("the network runtime is not running".into()));
        }

        Document {
            events: queue,
            commands,
        }
    }
}

impl Drop for Library {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// What a document-store query came back with.
pub(crate) enum Reply {
    /// Document ids, author ids, or a single new author id.
    Names(Vec<String>),
    /// A mutation that worked.
    Done,
    Failed(String),
}

/// A one-shot question put to the document store.
pub(crate) struct Ask(oneshot::Receiver<Reply>);

impl Ask {
    /// Takes the answer, if it has arrived. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Reply> {
        self.0.try_recv().ok()
    }
}

/// One open document. Dropping it closes it.
pub(crate) struct Document {
    events: UnboundedReceiver<Event>,
    commands: UnboundedSender<Command>,
}

impl Document {
    /// Takes the next event, if one is waiting. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        self.events.try_recv().ok()
    }

    /// Writes `value` at `key`. The result comes back as an [`Event::Entry`].
    pub(crate) fn set(&self, key: Bytes, value: Bytes) -> bool {
        self.commands.send(Command::Set { key, value }).is_ok()
    }

    /// Removes every key starting with `prefix`.
    pub(crate) fn delete(&self, prefix: Bytes) -> bool {
        self.commands.send(Command::Delete(prefix)).is_ok()
    }

    /// Asks for a key's value, answered by an [`Event::Value`].
    pub(crate) fn read(&self, key: Bytes) -> bool {
        self.commands.send(Command::Read(key)).is_ok()
    }

    /// Asks what keys the document holds under `prefix`, answered by an
    /// [`Event::Keys`]. An empty prefix lists everything.
    pub(crate) fn list_keys(&self, prefix: Bytes) -> bool {
        self.commands.send(Command::ListKeys(prefix)).is_ok()
    }

    /// Asks how the document is getting on, answered by an [`Event::Status`].
    pub(crate) fn status(&self) -> bool {
        self.commands.send(Command::Status).is_ok()
    }

    /// Asks for a ticket, answered by an [`Event::Shared`].
    pub(crate) fn share(&self, writable: bool) -> bool {
        self.commands.send(Command::Share(writable)).is_ok()
    }

    /// Starts syncing with more peers.
    pub(crate) fn join(&self, peers: Vec<EndpointAddr>) -> bool {
        self.commands.send(Command::Join(peers)).is_ok()
    }

    /// Stops syncing, keeping what we already hold.
    pub(crate) fn leave(&self) -> bool {
        self.commands.send(Command::Leave).is_ok()
    }
}

async fn run(
    handle: DocsHandle,
    source: Source,
    events: UnboundedSender<Event>,
    mut commands: UnboundedReceiver<Command>,
) {
    let docs = match handle.open().await {
        Ok(docs) => docs,
        Err(err) => {
            let _ = events.send(Event::Closed(err));
            return;
        }
    };

    let api = docs.api();
    let doc = match open_doc(api, source).await {
        Ok(doc) => doc,
        Err(err) => {
            let _ = events.send(Event::Closed(err));
            return;
        }
    };

    // One author per endpoint, kept by the engine, so writes keep the same
    // identity across runs of the same game.
    let author = match api.author_default().await {
        Ok(author) => author,
        Err(err) => {
            let _ = events.send(Event::Closed(err.to_string()));
            return;
        }
    };

    let mut updates = match doc.subscribe().await {
        Ok(updates) => updates,
        Err(err) => {
            let _ = events.send(Event::Closed(err.to_string()));
            return;
        }
    };

    let _ = events.send(Event::Opened(doc.id()));
    let store = match handle.store.open().await {
        Ok(store) => store,
        Err(err) => {
            let _ = events.send(Event::Closed(err));
            return;
        }
    };

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(command) => {
                    if let Err(err) = apply(&doc, &store, author, command, &events).await {
                        let _ = events.send(Event::Closed(err));
                        break;
                    }
                }
                // The handle was dropped, which means close the document.
                None => break,
            },
            update = updates.next() => match update {
                Some(Ok(event)) => {
                    if !report(event, &events) {
                        break;
                    }
                }
                Some(Err(err)) => {
                    let _ = events.send(Event::Closed(err.to_string()));
                    break;
                }
                None => {
                    let _ = events.send(Event::Closed("document closed".into()));
                    break;
                }
            },
        }
    }

    let _ = doc.close().await;
}

async fn open_doc(
    api: &iroh_docs::api::DocsApi,
    source: Source,
) -> Result<iroh_docs::api::Doc, String> {
    match source {
        Source::Create => api.create().await.map_err(|err| err.to_string()),
        Source::Open(id) => match api.open(id).await {
            Ok(Some(doc)) => Ok(doc),
            Ok(None) => Err("no document with that id is in the store".into()),
            Err(err) => Err(err.to_string()),
        },
        Source::Join(ticket) => api.import(*ticket).await.map_err(|err| err.to_string()),
    }
}

async fn apply(
    doc: &iroh_docs::api::Doc,
    store: &iroh_blobs::api::Store,
    author: AuthorId,
    command: Command,
    events: &UnboundedSender<Event>,
) -> Result<(), String> {
    match command {
        Command::Set { key, value } => {
            doc.set_bytes(author, key, value)
                .await
                .map_err(|err| err.to_string())?;
        }
        Command::Delete(prefix) => {
            doc.del(author, prefix)
                .await
                .map_err(|err| err.to_string())?;
        }
        Command::Read(key) => {
            let entry = doc
                .get_one(Query::key_exact(&key))
                .await
                .map_err(|err| err.to_string())?;

            // A key that is set but whose content has not arrived yet reads as
            // missing rather than as an error; `ContentReady` says when it is
            // worth asking again.
            let data = match entry {
                Some(entry) => store.get_bytes(entry.content_hash()).await.ok(),
                None => None,
            };

            let _ = events.send(Event::Value { key, data });
        }
        Command::ListKeys(prefix) => {
            // The latest write per key, so a key written repeatedly appears
            // once rather than once per revision.
            let query = Query::single_latest_per_key().key_prefix(&prefix).build();
            let listed = doc.get_many(query).await.map_err(|err| err.to_string())?;

            let mut entries = Vec::new();
            let mut listed = std::pin::pin!(listed);
            while let Some(entry) = listed.next().await {
                let entry = entry.map_err(|err| err.to_string())?;
                entries.push(KeyInfo {
                    key: Bytes::copy_from_slice(entry.key()),
                    author: entry.author(),
                    hash: entry.content_hash(),
                    len: entry.content_len(),
                });
            }

            let _ = events.send(Event::Keys { prefix, entries });
        }
        Command::Status => {
            let state = doc.status().await.map_err(|err| err.to_string())?;
            let _ = events.send(Event::Status {
                syncing: state.sync,
                subscribers: state.subscribers as u32,
                handles: state.handles as u32,
            });
        }
        Command::Share(writable) => {
            let mode = if writable {
                ShareMode::Write
            } else {
                ShareMode::Read
            };

            let ticket = doc
                .share(mode, AddrInfoOptions::RelayAndAddresses)
                .await
                .map_err(|err| err.to_string())?;

            let _ = events.send(Event::Shared(ticket.to_string()));
        }
        Command::Join(peers) => {
            doc.start_sync(peers).await.map_err(|err| err.to_string())?;
        }
        Command::Leave => {
            doc.leave().await.map_err(|err| err.to_string())?;
        }
    }

    Ok(())
}

/// Translates one live event. Returns `false` when the document is finished.
fn report(event: LiveEvent, events: &UnboundedSender<Event>) -> bool {
    let translated = match event {
        LiveEvent::InsertLocal { entry } => Event::Entry {
            key: Bytes::copy_from_slice(entry.key()),
            author: entry.author(),
            hash: entry.content_hash(),
            len: entry.content_len(),
            from: None,
        },
        LiveEvent::InsertRemote { entry, from, .. } => Event::Entry {
            key: Bytes::copy_from_slice(entry.key()),
            author: entry.author(),
            hash: entry.content_hash(),
            len: entry.content_len(),
            from: Some(from),
        },
        LiveEvent::ContentReady { hash } => Event::ContentReady(hash),
        LiveEvent::NeighborUp(peer) => Event::NeighborUp(peer),
        LiveEvent::NeighborDown(peer) => Event::NeighborDown(peer),
        LiveEvent::SyncFinished(sync) => Event::SyncFinished(sync.peer),
        // Says the last sync run's downloads have all settled, which is not
        // information a game can act on.
        LiveEvent::PendingContentReady => return true,
    };

    events.send(translated).is_ok()
}

/// Parses a document ticket.
pub(crate) fn parse_ticket(ticket: &str) -> Option<DocTicket> {
    DocTicket::from_str(ticket).ok()
}

/// Parses an author id.
pub(crate) fn parse_author(author: &str) -> Option<AuthorId> {
    AuthorId::from_str(author).ok()
}

/// Parses a document id.
pub(crate) fn parse_id(id: &str) -> Option<NamespaceId> {
    NamespaceId::from_str(id).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::blobs::Depot;
    use crate::dispatch::Dispatcher;
    use crate::gossip::Swarm;
    use crate::testing::endpoint_pair;

    /// A library on `dispatcher`, plus the blobs and gossip it stands on.
    /// All three are returned because dropping any of them tears it down.
    fn library(dispatcher: &Dispatcher) -> (Library, Depot, Swarm) {
        let depot = Depot::start(dispatcher, None).expect("blobs should start");
        let swarm = Swarm::start(dispatcher).expect("gossip should start");
        let library = Library::start(dispatcher, depot.store(), swarm.gossip(), None)
            .expect("documents should start");
        (library, depot, swarm)
    }

    /// Drains events for up to fifteen seconds, returning the first match.
    async fn wait_for<T>(doc: &mut Document, mut pick: impl FnMut(&Event) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            while let Some(event) = doc.try_recv() {
                if let Event::Closed(reason) = &event {
                    panic!("document closed: {reason}");
                }
                if let Some(found) = pick(&event) {
                    return found;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for a document event");
    }

    fn opened(event: &Event) -> Option<NamespaceId> {
        match event {
            Event::Opened(id) => Some(*id),
            _ => None,
        }
    }

    /// Reads a key, retrying while the value's content is still arriving.
    async fn read_key(doc: &mut Document, key: &'static [u8]) -> Bytes {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            doc.read(Bytes::from_static(key));

            let value = wait_for(doc, |event| match event {
                Event::Value { data, .. } => Some(data.clone()),
                _ => None,
            })
            .await;

            if let Some(data) = value {
                return data;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the key never became readable");
    }

    #[tokio::test]
    async fn a_document_opens_and_reads_back_what_it_wrote() {
        let (here, _there) = endpoint_pair().await;
        let (library, _depot, _swarm) = library(&here);

        let mut doc = library.create();
        wait_for(&mut doc, opened).await;

        doc.set(
            Bytes::from_static(b"spawn"),
            Bytes::from_static(b"north gate"),
        );
        wait_for(&mut doc, |event| {
            matches!(event, Event::Entry { from: None, .. }).then_some(())
        })
        .await;

        assert_eq!(
            read_key(&mut doc, b"spawn").await,
            Bytes::from_static(b"north gate")
        );
    }

    #[tokio::test]
    async fn a_missing_key_reads_as_nothing() {
        let (here, _there) = endpoint_pair().await;
        let (library, _depot, _swarm) = library(&here);

        let mut doc = library.create();
        wait_for(&mut doc, opened).await;

        doc.read(Bytes::from_static(b"never written"));
        let value = wait_for(&mut doc, |event| match event {
            Event::Value { data, .. } => Some(data.clone()),
            _ => None,
        })
        .await;

        assert!(value.is_none());
    }

    #[tokio::test]
    async fn deleting_a_key_leaves_it_unreadable() {
        let (here, _there) = endpoint_pair().await;
        let (library, _depot, _swarm) = library(&here);

        let mut doc = library.create();
        wait_for(&mut doc, opened).await;

        doc.set(Bytes::from_static(b"door"), Bytes::from_static(b"open"));
        assert_eq!(
            read_key(&mut doc, b"door").await,
            Bytes::from_static(b"open")
        );

        doc.delete(Bytes::from_static(b"door"));
        doc.read(Bytes::from_static(b"door"));

        let value = wait_for(&mut doc, |event| match event {
            Event::Value { data, .. } => Some(data.clone()),
            _ => None,
        })
        .await;
        assert!(value.is_none(), "a deleted key still read back");
    }

    /// The point of the whole feature: two peers, both writing, both converging.
    #[tokio::test]
    async fn two_peers_converge_on_the_same_document() {
        let (here, there) = endpoint_pair().await;
        let (ours, _our_depot, _our_swarm) = library(&here);
        let (theirs, _their_depot, _their_swarm) = library(&there);

        let mut mine = ours.create();
        wait_for(&mut mine, opened).await;
        mine.set(Bytes::from_static(b"map"), Bytes::from_static(b"desert"));

        mine.share(true);
        let ticket = wait_for(&mut mine, |event| match event {
            Event::Shared(ticket) => Some(ticket.clone()),
            _ => None,
        })
        .await;

        let mut yours = theirs.join(parse_ticket(&ticket).expect("the ticket should parse"));
        wait_for(&mut yours, opened).await;

        // What the first peer wrote reaches the second.
        assert_eq!(
            read_key(&mut yours, b"map").await,
            Bytes::from_static(b"desert")
        );

        // And a write from the second reaches the first, which is what makes
        // this multi-writer rather than a one-way copy.
        yours.set(
            Bytes::from_static(b"weather"),
            Bytes::from_static(b"sandstorm"),
        );
        assert_eq!(
            read_key(&mut mine, b"weather").await,
            Bytes::from_static(b"sandstorm")
        );
    }
}
