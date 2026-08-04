//! Content-addressed transfer: hand a peer a hash, get the bytes back.
//!
//! A blob is identified by the BLAKE3 hash of its contents, so a peer that
//! already has it can skip the transfer, an interrupted transfer resumes where
//! it stopped, and what arrives is verified against the name it was asked for.
//! Good for shipping maps, mods and assets between players at runtime.
//!
//! Godot-free, like the rest of the transport, so it can be driven from a test.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointId};
use iroh_blobs::api::Store;
use iroh_blobs::api::downloader::{Downloader, Shuffled};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::{BlobsProtocol, Hash};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;

use crate::dispatch::{Claim, Dispatcher};
use crate::runtime::{detach, detach_handle};

/// What a blob store query came back with.
pub(crate) enum Answer {
    /// A mutation that worked.
    Done,
    /// How much of a blob is here. `complete` is false for a partial fetch.
    Status {
        present: bool,
        complete: bool,
        size: u64,
    },
    /// Hashes or tag names, depending on what was asked.
    Names(Vec<String>),
    Failed(String),
}

/// A one-shot question put to the store.
pub(crate) struct Ask(oneshot::Receiver<Answer>);

impl Ask {
    /// Takes the answer, if it has arrived. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Answer> {
        self.0.try_recv().ok()
    }
}

/// How a transfer is getting on.
pub(crate) enum Event {
    /// Total byte count, once it is known. Arrives at most once.
    Size(u64),
    /// Bytes done so far. Ephemeral — do not count on a steady stream of these.
    Progress(u64),
    /// Finished. `data` is filled in only by a read.
    Done {
        hash: Hash,
        data: Bytes,
    },
    Failed(String),
}

/// One add, fetch, export or read in flight.
pub(crate) struct Transfer(UnboundedReceiver<Event>);

impl Transfer {
    /// Takes the next event, if one is waiting. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        self.0.try_recv().ok()
    }
}

/// Everything the store needs, built once on first use. Both halves are handles
/// to shared actors, so cloning is cheap.
#[derive(Clone)]
struct Ready {
    store: Store,
    downloader: Downloader,
}

type Cell = Arc<OnceCell<Result<Ready, String>>>;

/// A shareable way to reach the blob store, opening it on first use.
///
/// Documents are built on the same store, so they take one of these rather than
/// opening a second one.
#[derive(Clone)]
pub(crate) struct StoreHandle {
    cell: Cell,
    endpoint: Endpoint,
    /// Where blobs live. `None` keeps them in memory for this run only.
    path: Option<PathBuf>,
}

impl StoreHandle {
    /// The open store, opening it if this is the first call.
    pub(crate) async fn open(&self) -> Result<Store, String> {
        Ok(self.ready().await?.store)
    }

    async fn ready(&self) -> Result<Ready, String> {
        let built = self
            .cell
            .get_or_init(|| async {
                let store = match &self.path {
                    Some(path) => FsStore::load(path)
                        .await
                        .map_err(|err| format!("could not open the blob store: {err}"))?
                        .deref()
                        .clone(),
                    None => MemStore::new().deref().clone(),
                };

                let downloader = store.downloader(&self.endpoint);
                Ok(Ready { store, downloader })
            })
            .await;

        match built {
            Ok(ready) => Ok(ready.clone()),
            Err(err) => Err(err.clone()),
        }
    }
}

/// The blob store on one endpoint, shared by every protocol on it.
///
/// Started the first time a blob operation runs, so a game that never transfers
/// anything pays nothing for it. The store itself is built inside the first
/// operation's task rather than up front, because opening one on disk is real
/// I/O and must not happen on Godot's main thread.
pub(crate) struct Depot {
    store: StoreHandle,
    /// Held here rather than inside the task below, so dropping this releases
    /// the ALPN straight away instead of whenever the runtime reaps the task.
    _claim: Claim,
    /// Serves blobs to peers that ask.
    accepting: JoinHandle<()>,
}

impl Depot {
    pub(crate) fn start(dispatcher: &Dispatcher, path: Option<PathBuf>) -> Option<Self> {
        let (claim, mut inbound) = dispatcher.register(iroh_blobs::ALPN)?.split();
        let store = StoreHandle {
            cell: Arc::default(),
            endpoint: dispatcher.endpoint().clone(),
            path,
        };

        let serving = store.clone();
        let accepting = detach_handle(async move {
            while let Some(connection) = inbound.recv().await {
                let Ok(open) = serving.open().await else {
                    continue;
                };

                // `BlobsProtocol` is cheap to make and holds no per-connection
                // state, so one per connection keeps the tasks independent.
                let protocol = BlobsProtocol::new(&open, None);
                detach(async move {
                    let _ = protocol.accept(connection).await;
                });
            }
        })?;

        Some(Self {
            store,
            _claim: claim,
            accepting,
        })
    }

    /// Stores `data` and reports its hash.
    ///
    /// `tag` names the blob so it survives garbage collection — see
    /// [`Self::tag`]. Without one the blob is protected only while the import's
    /// temporary tag lives, which is until this call finishes.
    pub(crate) fn add_bytes(&self, data: Bytes, tag: Option<String>) -> Transfer {
        self.run(|ready, events| async move {
            let mut adding = ready.store.add_bytes(data).stream().await;
            let hash = forward_add(&mut adding, &events).await?;
            keep(&ready.store, hash, tag).await?;
            Ok(hash)
        })
    }

    /// Stores the file at `path` and reports its hash.
    pub(crate) fn add_file(&self, path: PathBuf, tag: Option<String>) -> Transfer {
        self.run(|ready, events| async move {
            let mut adding = ready.store.add_path(path).stream().await;
            let hash = forward_add(&mut adding, &events).await?;
            keep(&ready.store, hash, tag).await?;
            Ok(hash)
        })
    }

    /// Fetches `hash` from any of `providers`.
    ///
    /// Their addresses have to be resolvable, which on a closed network means
    /// teaching the endpoint about them first.
    pub(crate) fn fetch(
        &self,
        hash: Hash,
        providers: Vec<EndpointId>,
        tag: Option<String>,
    ) -> Transfer {
        self.run(move |ready, events| async move {
            let mut download = ready
                .downloader
                .download(hash, Shuffled::new(providers))
                .stream()
                .await
                .map_err(|err| err.to_string())?;

            while let Some(item) = download.next().await {
                use iroh_blobs::api::downloader::DownloadProgressItem as Item;
                match item {
                    Item::Progress(done) => {
                        let _ = events.send(Event::Progress(done));
                    }
                    Item::Error(err) => return Err(err.to_string()),
                    Item::DownloadError => return Err("the download failed".into()),
                    _ => {}
                }
            }

            keep(&ready.store, hash, tag).await?;
            Ok(hash)
        })
    }

    /// Writes a blob we already hold out to `path`.
    pub(crate) fn export(&self, hash: Hash, path: PathBuf) -> Transfer {
        self.run(move |ready, events| async move {
            let mut export = ready.store.export(hash, path).stream().await;

            while let Some(item) = export.next().await {
                use iroh_blobs::api::proto::ExportProgressItem as Item;
                match item {
                    Item::Size(size) => {
                        let _ = events.send(Event::Size(size));
                    }
                    Item::CopyProgress(done) => {
                        let _ = events.send(Event::Progress(done));
                    }
                    Item::Error(err) => return Err(err.to_string()),
                    _ => {}
                }
            }

            Ok(hash)
        })
    }

    /// How much of `hash` is here, which is how to tell a finished blob from a
    /// part-fetched one without starting a transfer.
    pub(crate) fn status(&self, hash: Hash) -> Ask {
        self.query(move |store| async move {
            use iroh_blobs::api::proto::BlobStatus;
            match store.status(hash).await {
                Ok(BlobStatus::NotFound) => Answer::Status {
                    present: false,
                    complete: false,
                    size: 0,
                },
                Ok(BlobStatus::Partial { size }) => Answer::Status {
                    present: true,
                    complete: false,
                    size: size.unwrap_or(0),
                },
                Ok(BlobStatus::Complete { size }) => Answer::Status {
                    present: true,
                    complete: true,
                    size,
                },
                Err(err) => Answer::Failed(err.to_string()),
            }
        })
    }

    /// Every hash in the store.
    pub(crate) fn list(&self) -> Ask {
        self.query(|store| async move {
            match store.list().hashes().await {
                Ok(hashes) => Answer::Names(hashes.iter().map(Hash::to_string).collect()),
                Err(err) => Answer::Failed(err.to_string()),
            }
        })
    }

    /// Names `hash` so garbage collection keeps it.
    pub(crate) fn tag(&self, hash: Hash, name: String) -> Ask {
        self.query(move |store| async move {
            match store.tags().set(name, hash).await {
                Ok(()) => Answer::Done,
                Err(err) => Answer::Failed(err.to_string()),
            }
        })
    }

    /// Removes a tag. The blob goes when garbage collection next runs, if
    /// nothing else names it.
    pub(crate) fn untag(&self, name: String) -> Ask {
        self.query(move |store| async move {
            match store.tags().delete(name).await {
                Ok(_) => Answer::Done,
                Err(err) => Answer::Failed(err.to_string()),
            }
        })
    }

    /// Every tag in the store.
    pub(crate) fn tags(&self) -> Ask {
        self.query(|store| async move {
            let listed = match store.tags().list().await {
                Ok(listed) => listed,
                Err(err) => return Answer::Failed(err.to_string()),
            };

            let mut names = Vec::new();
            let mut listed = std::pin::pin!(listed);
            while let Some(tag) = listed.next().await {
                match tag {
                    Ok(tag) =>
                    // `Tag`'s `Display` wraps the name in quotes; the raw bytes are
                    // what a game passed in and what it will pass back.
                    {
                        names.push(String::from_utf8_lossy(tag.name.as_ref()).into_owned())
                    }
                    Err(err) => return Answer::Failed(err.to_string()),
                }
            }
            Answer::Names(names)
        })
    }

    /// Shared plumbing for the one-shot questions above.
    fn query<F, Fut>(&self, work: F) -> Ask
    where
        F: FnOnce(Store) -> Fut + Send + 'static,
        Fut: Future<Output = Answer> + Send,
    {
        let (reply, answer) = oneshot::channel();
        let store = self.store.clone();

        if !detach(async move {
            let outcome = match store.open().await {
                Ok(store) => work(store).await,
                Err(err) => Answer::Failed(err),
            };
            let _ = reply.send(outcome);
        }) {
            // The channel is already gone, so nothing can be reported.
        }

        Ask(answer)
    }

    /// The store, for anything built on top of it.
    pub(crate) fn store(&self) -> StoreHandle {
        self.store.clone()
    }

    /// Reads a blob we already hold back into memory.
    pub(crate) fn read(&self, hash: Hash) -> Transfer {
        let (events, queue) = mpsc::unbounded_channel();
        let store = self.store.clone();

        let failed = events.clone();
        if !detach(async move {
            let outcome = match store.open().await {
                Ok(store) => store.get_bytes(hash).await.map_err(|err| err.to_string()),
                Err(err) => Err(err),
            };

            let _ = events.send(match outcome {
                Ok(data) => Event::Done { hash, data },
                Err(reason) => Event::Failed(reason),
            });
        }) {
            let _ = failed.send(Event::Failed("the network runtime is not running".into()));
        }

        Transfer(queue)
    }

    /// Shared plumbing: open the store, run `work`, report how it went.
    fn run<F, Fut>(&self, work: F) -> Transfer
    where
        F: FnOnce(Ready, UnboundedSender<Event>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Hash, String>> + Send,
    {
        let (events, queue) = mpsc::unbounded_channel();
        let store = self.store.clone();

        let failed = events.clone();
        if !detach(async move {
            let outcome = match store.ready().await {
                Ok(ready) => work(ready, events.clone()).await,
                Err(err) => Err(err),
            };

            let _ = events.send(match outcome {
                Ok(hash) => Event::Done {
                    hash,
                    data: Bytes::new(),
                },
                Err(reason) => Event::Failed(reason),
            });
        }) {
            let _ = failed.send(Event::Failed("the network runtime is not running".into()));
        }

        Transfer(queue)
    }
}

impl Drop for Depot {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// Parses a blob hash, or `None` if the text is not one.
///
/// The length is checked first, and that is not belt and braces:
/// `Hash::from_str` decodes straight into a fixed 32-byte buffer, and
/// `data_encoding` *panics* when the input would not fill it — before iroh's own
/// length check ever runs. A panic here would cross the FFI boundary into
/// Godot. Only the two lengths that decode to exactly 32 bytes are handed over:
/// 64 hex characters, or 52 base32 ones.
pub(crate) fn parse_hash(text: &str) -> Option<Hash> {
    if !matches!(text.len(), 52 | 64) {
        return None;
    }

    text.parse().ok()
}

/// Gives a freshly stored blob a permanent name, if one was asked for.
///
/// Without this the blob is protected only by the import's temporary tag, which
/// is dropped as soon as the import finishes — so a store with garbage
/// collection turned on would sweep it away.
async fn keep(store: &Store, hash: Hash, tag: Option<String>) -> Result<(), String> {
    let Some(tag) = tag else {
        return Ok(());
    };

    store
        .tags()
        .set(tag, hash)
        .await
        .map_err(|err| format!("stored, but could not tag it: {err}"))
}

/// Drains an add, reporting size and copy progress on the way.
async fn forward_add(
    adding: &mut (impl futures_lite::Stream<Item = iroh_blobs::api::proto::AddProgressItem> + Unpin),
    events: &UnboundedSender<Event>,
) -> Result<Hash, String> {
    use iroh_blobs::api::proto::AddProgressItem as Item;

    let mut hash = None;
    while let Some(item) = adding.next().await {
        match item {
            Item::Size(size) => {
                let _ = events.send(Event::Size(size));
            }
            Item::CopyProgress(done) => {
                let _ = events.send(Event::Progress(done));
            }
            Item::Done(tag) => hash = Some(tag.hash()),
            Item::Error(err) => return Err(err.to_string()),
            _ => {}
        }
    }

    hash.ok_or_else(|| "the store never reported a hash".to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::endpoint_pair;

    /// Drains a transfer to its end, returning the hash and any bytes, plus the
    /// largest progress figure seen on the way.
    async fn finish(transfer: &mut Transfer) -> (Hash, Bytes, u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut furthest = 0;

        while tokio::time::Instant::now() < deadline {
            while let Some(event) = transfer.try_recv() {
                match event {
                    Event::Size(size) => furthest = furthest.max(size),
                    Event::Progress(done) => furthest = furthest.max(done),
                    Event::Done { hash, data } => return (hash, data, furthest),
                    Event::Failed(reason) => panic!("transfer failed: {reason}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("transfer never finished");
    }

    /// `Hash::from_str` panics inside `data_encoding` on anything that would not
    /// fill its 32-byte buffer, so every one of these reached Godot as a crash
    /// before the length guard went in.
    #[test]
    fn a_malformed_hash_is_refused_rather_than_panicking() {
        for bad in [
            "",
            "deadbeef",
            "not a hash at all",
            "zz",
            &"a".repeat(51),
            &"a".repeat(53),
            &"a".repeat(63),
            &"a".repeat(65),
            &"!".repeat(52),
            &"!".repeat(64),
        ] {
            assert!(parse_hash(bad).is_none(), "accepted {bad:?}");
        }
    }

    #[tokio::test]
    async fn a_real_hash_round_trips_through_its_text_form() {
        let hash = Hash::new(b"something");
        assert_eq!(parse_hash(&hash.to_string()), Some(hash));
    }

    #[tokio::test]
    async fn a_blob_survives_a_round_trip_through_the_store() {
        let (here, _there) = endpoint_pair().await;
        let depot = Depot::start(&here, None).expect("blobs should start");

        let (hash, _, _) =
            finish(&mut depot.add_bytes(Bytes::from_static(b"a map, say"), None)).await;
        let (_, data, _) = finish(&mut depot.read(hash)).await;

        assert_eq!(data, Bytes::from_static(b"a map, say"));
    }

    /// The other store backing, which a game turns on so players keep what they
    /// have already downloaded.
    ///
    /// Only that it works and really writes to disk: whether the bytes survive a
    /// *process* restart cannot be shown from inside one process, since the
    /// store holds an exclusive lock on its database for as long as it is open.
    #[tokio::test]
    async fn a_blob_store_on_disk_works_and_writes_there() {
        let (here, _there) = endpoint_pair().await;
        let path = std::env::temp_dir().join(format!("gdiroh-store-{}", here.endpoint().id()));
        let depot = Depot::start(&here, Some(path.clone())).expect("blobs should start");

        let (hash, _, _) =
            finish(&mut depot.add_bytes(Bytes::from_static(b"persisted"), None)).await;
        let (_, data, _) = finish(&mut depot.read(hash)).await;
        assert_eq!(data, Bytes::from_static(b"persisted"));

        let written = std::fs::read_dir(&path)
            .expect("the store directory should exist")
            .count();
        assert!(written > 0, "the store wrote nothing to {path:?}");

        drop(depot);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Waits for a one-shot answer.
    async fn answer(ask: &mut Ask) -> Answer {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            if let Some(answer) = ask.try_recv() {
                return answer;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the store never answered");
    }

    fn names(answer: Answer) -> Vec<String> {
        match answer {
            Answer::Names(names) => names,
            Answer::Failed(reason) => panic!("query failed: {reason}"),
            _ => panic!("unexpected answer"),
        }
    }

    /// Tags are what keep a blob once garbage collection is on, and removing
    /// them is the only way to let it be reclaimed.
    #[tokio::test]
    async fn a_tag_names_a_blob_and_can_be_taken_off_again() {
        let (here, _there) = endpoint_pair().await;
        let depot = Depot::start(&here, None).expect("blobs should start");

        let mut adding = depot.add_bytes(Bytes::from_static(b"keep me"), Some("saved-map".into()));
        finish(&mut adding).await;

        let tagged = names(answer(&mut depot.tags()).await);
        assert!(tagged.contains(&"saved-map".to_string()), "got: {tagged:?}");

        answer(&mut depot.untag("saved-map".into())).await;
        let after = names(answer(&mut depot.tags()).await);
        assert!(!after.contains(&"saved-map".to_string()), "got: {after:?}");
    }

    #[tokio::test]
    async fn status_separates_held_from_missing() {
        let (here, _there) = endpoint_pair().await;
        let depot = Depot::start(&here, None).expect("blobs should start");

        let (hash, _, _) = finish(&mut depot.add_bytes(Bytes::from_static(b"present"), None)).await;

        let held = answer(&mut depot.status(hash)).await;
        assert!(
            matches!(held, Answer::Status { present: true, complete: true, size } if size == 7),
            "a stored blob should read back as complete"
        );

        let missing = answer(&mut depot.status(Hash::new(b"never stored"))).await;
        assert!(matches!(missing, Answer::Status { present: false, .. }));
    }

    #[tokio::test]
    async fn listing_shows_what_was_added() {
        let (here, _there) = endpoint_pair().await;
        let depot = Depot::start(&here, None).expect("blobs should start");

        let (hash, _, _) = finish(&mut depot.add_bytes(Bytes::from_static(b"listed"), None)).await;

        let listed = names(answer(&mut depot.list()).await);
        assert!(listed.contains(&hash.to_string()), "got: {listed:?}");
    }

    #[tokio::test]
    async fn the_same_contents_always_get_the_same_hash() {
        let (here, there) = endpoint_pair().await;
        let ours = Depot::start(&here, None).expect("blobs should start");
        let theirs = Depot::start(&there, None).expect("blobs should start");

        let (mine, _, _) =
            finish(&mut ours.add_bytes(Bytes::from_static(b"identical"), None)).await;
        let (yours, _, _) =
            finish(&mut theirs.add_bytes(Bytes::from_static(b"identical"), None)).await;

        // Content addressing is the whole point: two peers that add the same
        // bytes name them the same way without ever talking to each other.
        assert_eq!(mine, yours);
    }

    #[tokio::test]
    async fn a_blob_can_be_fetched_from_the_peer_that_has_it() {
        let (here, there) = endpoint_pair().await;
        let provider = Depot::start(&here, None).expect("blobs should start");
        let fetcher = Depot::start(&there, None).expect("blobs should start");

        let payload = Bytes::from(vec![7u8; 96 * 1024]);
        let (hash, _, _) = finish(&mut provider.add_bytes(payload.clone(), None)).await;

        let mut download = fetcher.fetch(hash, vec![here.endpoint().id()], None);
        let (fetched, _, _) = finish(&mut download).await;
        assert_eq!(fetched, hash);

        let (_, data, _) = finish(&mut fetcher.read(hash)).await;
        assert_eq!(data, payload, "the fetched bytes differ from the original");
    }

    #[tokio::test]
    async fn fetching_reports_progress() {
        let (here, there) = endpoint_pair().await;
        let provider = Depot::start(&here, None).expect("blobs should start");
        let fetcher = Depot::start(&there, None).expect("blobs should start");

        // Big enough that the transfer cannot finish in a single quiet step.
        let payload = Bytes::from(vec![3u8; 512 * 1024]);
        let (hash, _, _) = finish(&mut provider.add_bytes(payload, None)).await;

        let (_, _, furthest) =
            finish(&mut fetcher.fetch(hash, vec![here.endpoint().id()], None)).await;
        assert!(furthest > 0, "no progress was ever reported");
    }

    #[tokio::test]
    async fn a_blob_can_be_written_out_to_a_file() {
        let (here, _there) = endpoint_pair().await;
        let depot = Depot::start(&here, None).expect("blobs should start");

        let (hash, _, _) =
            finish(&mut depot.add_bytes(Bytes::from_static(b"export me"), None)).await;

        let target = std::env::temp_dir().join(format!("gdiroh-export-{hash}"));
        finish(&mut depot.export(hash, target.clone())).await;

        let written = std::fs::read(&target).expect("the file should exist");
        assert_eq!(written, b"export me");
        let _ = std::fs::remove_file(&target);
    }

    #[tokio::test]
    async fn fetching_a_hash_nobody_has_fails_rather_than_hanging() {
        let (here, there) = endpoint_pair().await;
        let _provider = Depot::start(&here, None).expect("blobs should start");
        let fetcher = Depot::start(&there, None).expect("blobs should start");

        let missing = Hash::new(b"never added to any store");
        let mut download = fetcher.fetch(missing, vec![here.endpoint().id()], None);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            while let Some(event) = download.try_recv() {
                match event {
                    Event::Failed(_) => return,
                    Event::Done { .. } => panic!("a hash nobody has appeared to download"),
                    _ => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the download neither failed nor finished");
    }
}
