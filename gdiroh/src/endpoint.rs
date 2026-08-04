//! An iroh endpoint, exposed to GDScript as `IrohEndpoint`.
//!
//! Wraps [`iroh::Endpoint`](https://docs.rs/iroh/latest/iroh/endpoint/struct.Endpoint.html).

use std::collections::HashMap;

use std::path::PathBuf;
use std::str::FromStr;

use godot::classes::{Engine, IRefCounted, ProjectSettings, RefCounted, SceneTree};
use godot::prelude::*;
use iroh::Watcher;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{BindError, RelayMode, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMap, SecretKey};
use iroh_blobs::Hash;
use iroh_blobs::ticket::BlobTicket;
use iroh_mdns_address_lookup::MdnsAddressLookup;
use iroh_tickets::Ticket;
use iroh_tickets::endpoint::EndpointTicket;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

use crate::blobs::{self, Answer, Ask, Depot};
use crate::connection::IrohConnection;
use crate::dispatch::{Dispatcher, Registration};
use crate::docs::{self, Library, Reply};
use crate::document::IrohDocument;
use crate::gossip::{self, Swarm};
use crate::runtime;
use crate::stats;
use crate::topic::IrohTopic;
use crate::transfer::IrohTransfer;

/// mDNS service name used when a game has not chosen its own.
const DEFAULT_LOCAL_SERVICE: &str = "gdiroh";

/// A peer-to-peer endpoint: one identity, one reachable address, and the thing
/// everything else here hangs off — sessions, protocols, gossip, blobs and
/// documents all come from an endpoint's methods.
///
/// Construct one and **keep a reference to it**. An endpoint is reference
/// counted and closes when the last reference goes, so one held only in a
/// local variable dies when the function returns, taking its bind with it.
/// A member variable is the natural home. Nothing networks — no threads, no
/// sockets — until the first [method bind].
///
/// gdiroh never writes your identity to disk. Generate a key, store it wherever
/// suits your game, and hand it back before binding:
///
/// ```gdscript
/// var endpoint: IrohEndpoint
///
/// func _ready() -> void:
///     var key := my_save.load_network_key()
///     if key.is_empty():
///         key = IrohEndpoint.generate_secret_key()
///         my_save.store_network_key(key)
///
///     endpoint = IrohEndpoint.new()
///     endpoint.set_secret_key(key)
///     endpoint.bound.connect(func(id): print("listening as ", id))
///     endpoint.bind()
/// ```
///
/// A game may hold several endpoints — separate identities, or separate blob
/// and document stores. Give each its own [method set_blob_store_path]; two
/// endpoints sharing one on-disk store fight over its database lock.
#[derive(GodotClass)]
#[class(base=RefCounted)]
pub struct IrohEndpoint {
    /// Identity for the next bind. `None` means a throwaway is generated.
    secret_key: Option<SecretKey>,
    /// Held from the first bind until this endpoint closes. The runtime starts
    /// with the first lease and stops with the last, so a project that never
    /// binds runs no network threads.
    lease: Option<runtime::Lease>,
    /// Present once bound. Owns the endpoint's one accept loop, so protocols
    /// reach it from here rather than racing each other for `accept()`.
    dispatcher: Option<Dispatcher>,
    pending_bind: Option<oneshot::Receiver<Result<Endpoint, BindError>>>,
    /// ALPNs script asked to accept, by protocol name.
    listeners: HashMap<Vec<u8>, Registration>,
    /// ALPNs asked for before the bind finished, claimed as soon as it does.
    /// `bind()` returns immediately, so calling `listen()` on the next line is
    /// the obvious thing to write and would otherwise silently do nothing.
    pending_listens: Vec<Vec<u8>>,
    /// Started the first time a topic is subscribed to, so a game that never
    /// gossips pays nothing for it.
    swarm: Option<Swarm>,
    /// Started on the first blob operation, for the same reason.
    depot: Option<Depot>,
    /// Started on the first document, which needs blobs and gossip under it.
    library: Option<Library>,
    /// Where blobs are kept. Empty means in memory, for this run only.
    blob_path: String,
    /// Store questions waiting on an answer, drained on the frame signal.
    questions: Vec<Question>,
    /// The same, for the document store.
    document_questions: Vec<DocQuestion>,
    /// Addresses learned out of band, from tickets. Gossip bootstraps by bare
    /// endpoint id, so without this a closed network cannot join a swarm.
    lookup: MemoryLookup,
    ticking: bool,
    /// Applied at the next bind, not to a live endpoint.
    relay_mode: RelayMode,
    dns_lookup: bool,
    local_discovery: bool,
    local_service: String,
    base: Base<RefCounted>,
}

#[godot_api]
impl IRefCounted for IrohEndpoint {
    fn init(base: Base<RefCounted>) -> Self {
        Self {
            secret_key: None,
            lease: None,
            dispatcher: None,
            pending_bind: None,
            listeners: HashMap::new(),
            pending_listens: Vec::new(),
            swarm: None,
            depot: None,
            library: None,
            blob_path: String::new(),
            questions: Vec::new(),
            document_questions: Vec::new(),
            lookup: MemoryLookup::new(),
            ticking: false,
            relay_mode: RelayMode::Default,
            dns_lookup: true,
            // Off by default: it advertises this machine on the local network,
            // which should be a deliberate choice rather than a surprise.
            local_discovery: false,
            local_service: DEFAULT_LOCAL_SERVICE.to_string(),
            base,
        }
    }
}

impl IrohEndpoint {
    /// Subscribes `_drain` to the frame signal while there is work outstanding.
    fn start_ticking(&mut self) {
        if self.ticking {
            return;
        }

        let Some(mut tree) = scene_tree() else {
            crate::log::error!("no scene tree to poll on; results cannot be delivered");
            return;
        };

        let callable = Callable::from_object_method(&self.to_gd(), "_drain");
        tree.connect("process_frame", &callable);
        self.ticking = true;
    }

    /// Keeps the frame subscription matching whether anything needs polling, so
    /// idle frames stay free.
    fn refresh_ticking(&mut self) {
        if self.pending_bind.is_some()
            || !self.listeners.is_empty()
            || !self.questions.is_empty()
            || !self.document_questions.is_empty()
        {
            self.start_ticking();
        } else {
            self.stop_ticking();
        }
    }

    /// Unsubscribes once nothing is outstanding, so idle frames stay free.
    fn stop_ticking(&mut self) {
        if !self.ticking {
            return;
        }

        if let Some(mut tree) = scene_tree() {
            let callable = Callable::from_object_method(&self.to_gd(), "_drain");
            tree.disconnect("process_frame", &callable);
        }

        self.ticking = false;
    }

    /// The bound endpoint, if there is one.
    fn endpoint(&self) -> Option<&Endpoint> {
        self.dispatcher.as_ref().map(Dispatcher::endpoint)
    }

    /// Settles a bind that has finished, if one has.
    fn drain_bind(&mut self) {
        let Some(receiver) = self.pending_bind.as_mut() else {
            return;
        };

        let outcome = match receiver.try_recv() {
            Ok(result) => result.map_err(|err| err.to_string()),
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Closed) => Err("the bind task was dropped".to_string()),
        };

        self.pending_bind = None;
        self.refresh_ticking();

        match outcome {
            Ok(endpoint) => {
                let id = endpoint.id().to_string();
                self.dispatcher = Some(Dispatcher::start(endpoint));

                // Anything asked for while the bind was in flight.
                for alpn in std::mem::take(&mut self.pending_listens) {
                    self.claim(alpn);
                }

                crate::log::info!("listening as {id}");
                self.emit_later("bound", &[GString::from(&id).to_variant()]);
            }
            Err(reason) => {
                crate::log::error!("could not bind the endpoint: {reason}");
                self.emit_later("bind_failed", &[GString::from(&reason).to_variant()]);
            }
        }
    }

    /// Emits after the current call unwinds. Emitting inline would re-enter this
    /// object while it is still mutably borrowed, which panics.
    fn emit_later(&mut self, signal: &str, args: &[Variant]) {
        let mut call = Vec::with_capacity(args.len() + 1);
        call.push(signal.to_variant());
        call.extend_from_slice(args);
        self.base_mut().call_deferred("emit_signal", &call);
    }

    /// Shared tail of [`IrohEndpoint::connect_to`] and
    /// [`IrohEndpoint::connect_to_ticket`].
    fn dial(&self, peer: EndpointAddr, alpn: GString) -> Option<Gd<IrohConnection>> {
        let alpn = alpn.to_string().into_bytes();
        if alpn.is_empty() {
            crate::log::error!("a protocol name cannot be empty");
            return None;
        }

        let Some(dispatcher) = self.dispatcher.as_ref() else {
            crate::log::error!("bind the endpoint before connecting");
            return None;
        };

        Some(IrohConnection::dial(dispatcher, peer, alpn))
    }

    /// Claims one ALPN on the dispatcher, if it is not already held.
    fn claim(&mut self, alpn: Vec<u8>) -> bool {
        if self.listeners.contains_key(&alpn) {
            return true;
        }

        let Some(dispatcher) = self.dispatcher.as_ref() else {
            return false;
        };
        let Some(registration) = dispatcher.register(&alpn) else {
            let name = String::from_utf8_lossy(&alpn);
            crate::log::error!("something is already listening for '{name}'");
            return false;
        };

        self.listeners.insert(alpn, registration);
        self.refresh_ticking();
        true
    }

    /// The gossip swarm, started on first use.
    fn swarm(&mut self) -> Option<&Swarm> {
        if self.swarm.is_none() {
            let dispatcher = self.dispatcher.clone()?;
            self.swarm = Swarm::start(&dispatcher);
            if self.swarm.is_none() {
                crate::log::error!("could not start gossip on this endpoint");
            }
        }

        self.swarm.as_ref()
    }

    /// The document engine, started on first use.
    fn library(&mut self) -> Option<&Library> {
        if self.library.is_none() {
            let dispatcher = self.dispatcher.clone()?;
            // Documents keep their values as blobs and their live updates on
            // gossip, so both of those come up first and are shared, not copied.
            let store = self.depot()?.store();
            let gossip = self.swarm()?.gossip();
            let path = (!self.blob_path.is_empty()).then(|| PathBuf::from(&self.blob_path));

            self.library = Library::start(&dispatcher, store, gossip, path);
            if self.library.is_none() {
                crate::log::error!("could not start documents on this endpoint");
            }
        }

        self.library.as_ref()
    }

    /// The blob store, started on first use.
    fn depot(&mut self) -> Option<&Depot> {
        if self.depot.is_none() {
            let dispatcher = self.dispatcher.clone()?;
            let path = (!self.blob_path.is_empty()).then(|| PathBuf::from(&self.blob_path));

            self.depot = Depot::start(&dispatcher, path);
            if self.depot.is_none() {
                crate::log::error!("could not start the blob store on this endpoint");
            }
        }

        self.depot.as_ref()
    }

    /// Answers the store questions that have come back.
    fn drain_questions(&mut self) {
        let mut answered = Vec::new();
        self.questions
            .retain_mut(|question| match question.ask.try_recv() {
                Some(answer) => {
                    answered.push((question.about.clone(), question.kind, answer));
                    false
                }
                None => true,
            });

        for (about, kind, answer) in answered {
            self.answer(about, kind, answer);
        }

        self.refresh_ticking();
    }

    /// Answers the document-store questions that have come back.
    fn drain_document_questions(&mut self) {
        let mut answered = Vec::new();
        self.document_questions
            .retain_mut(|question| match question.ask.try_recv() {
                Some(reply) => {
                    answered.push((question.about.clone(), question.kind, reply));
                    false
                }
                None => true,
            });

        for (about, kind, reply) in answered {
            match (kind, reply) {
                (DocKind::Documents, Reply::Names(ids)) => {
                    let ids: PackedStringArray = ids.iter().map(GString::from).collect();
                    self.emit_later("document_list", &[ids.to_variant()]);
                }
                (DocKind::Authors, Reply::Names(authors)) => {
                    let authors: PackedStringArray = authors.iter().map(GString::from).collect();
                    self.emit_later("author_list", &[authors.to_variant()]);
                }
                (DocKind::NewAuthor, Reply::Names(author)) => {
                    let author = GString::from(author.first().map(String::as_str).unwrap_or(""));
                    self.emit_later("author_created", &[author.to_variant()]);
                }
                (DocKind::Mutation, Reply::Done) => {}
                (_, Reply::Failed(reason)) => {
                    crate::log::error!("documents: {about} failed: {reason}");
                }
                (_, _) => crate::log::error!("documents: unexpected answer for {about}"),
            }
        }

        self.refresh_ticking();
    }

    /// Queues a document question and starts polling for its answer.
    fn asking_documents(&mut self, kind: DocKind, about: impl Into<String>, ask: docs::Ask) {
        self.document_questions.push(DocQuestion {
            kind,
            about: about.into(),
            ask,
        });
        self.refresh_ticking();
    }

    fn answer(&mut self, about: String, kind: Kind, answer: Answer) {
        let about = GString::from(&about);

        match (kind, answer) {
            (
                Kind::Status,
                Answer::Status {
                    present,
                    complete,
                    size,
                },
            ) => self.emit_later(
                "blob_status",
                &[
                    about.to_variant(),
                    present.to_variant(),
                    complete.to_variant(),
                    (size as i64).to_variant(),
                ],
            ),
            (Kind::Blobs, Answer::Names(names)) => {
                let names: PackedStringArray = names.iter().map(GString::from).collect();
                self.emit_later("blob_list", &[names.to_variant()]);
            }
            (Kind::Tags, Answer::Names(names)) => {
                let names: PackedStringArray = names.iter().map(GString::from).collect();
                self.emit_later("tag_list", &[names.to_variant()]);
            }
            // A mutation only reports back when it went wrong.
            (Kind::Mutation, Answer::Done) => {}
            (_, Answer::Failed(reason)) => {
                crate::log::error!("blob store: {about} failed: {reason}");
            }
            // The store answered a different question than was asked, which
            // would be a bug here rather than anything a game can act on.
            (_, _) => crate::log::error!("blob store: unexpected answer for {about}"),
        }
    }

    /// Queues a question and starts polling for its answer.
    fn asking(&mut self, kind: Kind, about: impl Into<String>, ask: Ask) {
        self.questions.push(Question {
            kind,
            about: about.into(),
            ask,
        });
        self.refresh_ticking();
    }

    /// Hands script every connection that arrived for a listened-for ALPN.
    fn drain_listeners(&mut self) {
        let mut arrived = Vec::new();
        for (alpn, registration) in self.listeners.iter_mut() {
            while let Some(connection) = registration.try_accept() {
                arrived.push((alpn.clone(), connection));
            }
        }

        for (alpn, connection) in arrived {
            let protocol = String::from_utf8_lossy(&alpn).into_owned();
            self.emit_later(
                "connection_received",
                &[
                    GString::from(&protocol).to_variant(),
                    IrohConnection::accepted(connection).to_variant(),
                ],
            );
        }
    }
}

/// What a pending document-store question was asked for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DocKind {
    Documents,
    Authors,
    NewAuthor,
    Mutation,
}

struct DocQuestion {
    kind: DocKind,
    about: String,
    ask: docs::Ask,
}

/// What a pending store question was asked for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Status,
    Blobs,
    Tags,
    Mutation,
}

/// A question in flight, and what it was about, so the answer can say.
struct Question {
    kind: Kind,
    about: String,
    ask: Ask,
}

/// Treats an empty string as "not given", which is how optional arguments
/// arrive from GDScript.
fn optional(value: &GString) -> Option<String> {
    let value = value.to_string();
    (!value.trim().is_empty()).then_some(value)
}

/// Turns a Godot path such as `user://blobs` into a real filesystem path.
fn global_path(path: &GString) -> Option<PathBuf> {
    let path = path.to_string();
    if path.trim().is_empty() {
        crate::log::error!("a path cannot be empty");
        return None;
    }

    let global = ProjectSettings::singleton().globalize_path(&GString::from(&path));
    Some(PathBuf::from(global.to_string()))
}

/// Parses a blob hash, reporting a malformed one rather than failing silently.
fn parse_hash(hash: &GString) -> Option<Hash> {
    match blobs::parse_hash(&hash.to_string()) {
        Some(hash) => Some(hash),
        None => {
            crate::log::error!("'{hash}' is not a valid blob hash");
            None
        }
    }
}

pub(crate) fn scene_tree() -> Option<Gd<SceneTree>> {
    Engine::singleton()
        .get_main_loop()
        .and_then(|main_loop| main_loop.try_cast::<SceneTree>().ok())
}

#[godot_api]
impl IrohEndpoint {
    /// Emitted once the endpoint is listening. Carries this peer's public id,
    /// which is what other peers dial.
    #[signal]
    fn bound(endpoint_id: GString);

    /// Emitted when binding failed. The endpoint stays unbound.
    #[signal]
    fn bind_failed(reason: GString);

    /// Emitted when a peer connects on a protocol [method listen]
    /// claimed. The connection is already open.
    #[signal]
    fn connection_received(alpn: GString, connection: Gd<IrohConnection>);

    /// Answers [method request_blob_status].
    ///
    /// `present` is false when the store has never seen the hash. `complete`
    /// separates a finished blob from a part-finished fetch, and `size` is what
    /// is held so far in that case.
    #[signal]
    fn blob_status(hash: GString, present: bool, complete: bool, size: i64);

    /// Answers [method request_blob_list].
    #[signal]
    fn blob_list(hashes: PackedStringArray);

    /// Answers [method request_tag_list].
    #[signal]
    fn tag_list(tags: PackedStringArray);

    /// Answers [method request_document_list].
    #[signal]
    fn document_list(ids: PackedStringArray);

    /// Answers [method request_author_list].
    #[signal]
    fn author_list(authors: PackedStringArray);

    /// Answers [method create_author] with the new id.
    #[signal]
    fn author_created(author: GString);

    /// n0's production relays. The default.
    #[constant]
    const RELAY_DEFAULT: i32 = 0;

    /// n0's staging relays. For testing against unreleased relay changes.
    #[constant]
    const RELAY_STAGING: i32 = 1;

    /// No relays at all. Peers that cannot hole punch will fail to connect,
    /// which is what you want on a LAN-only or fully private deployment.
    #[constant]
    const RELAY_DISABLED: i32 = 2;

    /// Chooses which relays carry traffic when a direct path cannot be made.
    ///
    /// Takes effect at the next [method bind]; a live endpoint keeps the
    /// mode it was bound with. See
    /// [relays](https://www.iroh.computer/docs/concepts/relay).
    #[func]
    fn set_relay_mode(&mut self, mode: i32) -> bool {
        self.relay_mode = match mode {
            Self::RELAY_DEFAULT => RelayMode::Default,
            Self::RELAY_STAGING => RelayMode::Staging,
            Self::RELAY_DISABLED => RelayMode::Disabled,
            other => {
                crate::log::error!("{other} is not a relay mode");
                return false;
            }
        };
        true
    }

    /// Routes through your own relay servers instead of n0's.
    ///
    /// Takes effect at the next [method bind]. Returns `false` if any
    /// url is malformed, leaving the previous mode in place.
    #[func]
    fn set_custom_relays(&mut self, urls: PackedStringArray) -> bool {
        let urls: Vec<String> = urls.as_slice().iter().map(GString::to_string).collect();

        match RelayMap::try_from_iter(urls.iter().map(String::as_str)) {
            Ok(map) => {
                self.relay_mode = RelayMode::Custom(map);
                true
            }
            Err(err) => {
                crate::log::error!("could not use those relay urls: {err}");
                false
            }
        }
    }

    /// Whether peer ids are resolved through n0's DNS service.
    ///
    /// Turn it off for a closed network where peers only ever reach each other
    /// by ticket. Takes effect at the next [method bind].
    #[func]
    fn set_dns_lookup(&mut self, enabled: bool) {
        self.dns_lookup = enabled;
    }

    /// Finds peers on the same local network over mDNS, and advertises this one
    /// to them.
    ///
    /// Off by default, since it announces this machine to the network. With it
    /// on, a bare endpoint id resolves on a LAN with no DNS and no relays at
    /// all. Takes effect at the next [method bind].
    #[func]
    fn set_local_discovery(&mut self, enabled: bool) {
        self.local_discovery = enabled;
    }

    /// Names the mDNS service this game advertises under.
    ///
    /// Worth setting. The shared default means two different gdiroh games on one
    /// network would find each other's peers; a name of your own keeps your
    /// lobby to yourself. Empty restores the default. Takes effect at the next
    /// [method bind].
    #[func]
    fn set_local_discovery_service(&mut self, name: GString) {
        let name = name.to_string();
        self.local_service = if name.trim().is_empty() {
            DEFAULT_LOCAL_SERVICE.to_string()
        } else {
            name
        };
    }

    /// A shareable string carrying this peer's id *and* its current addresses.
    ///
    /// Where an endpoint id needs a lookup service to become reachable, a ticket
    /// already contains the direct addresses and relay url — so it works on a
    /// closed network. Empty while unbound.
    #[func]
    fn ticket(&self) -> GString {
        match self.endpoint() {
            Some(endpoint) => {
                let ticket = EndpointTicket::new(endpoint.addr());
                GString::from(&ticket.encode_string())
            }
            None => GString::new(),
        }
    }

    /// Creates a fresh identity as 32 raw bytes.
    ///
    /// Store it yourself — `Marshalls.raw_to_base64()` turns it into text for a
    /// config file. Reusing the same key keeps your peer id stable across runs.
    #[func]
    fn generate_secret_key() -> PackedByteArray {
        PackedByteArray::from(&SecretKey::generate().to_bytes()[..])
    }

    /// Sets the identity used by the next [method bind]. Expects the 32
    /// bytes from [method generate_secret_key].
    ///
    /// Returns `false` if the key is the wrong length.
    #[func]
    fn set_secret_key(&mut self, key: PackedByteArray) -> bool {
        let Ok(bytes) = <[u8; 32]>::try_from(key.as_slice()) else {
            crate::log::error!("a secret key must be 32 bytes, got {}", key.len());
            return false;
        };

        self.secret_key = Some(SecretKey::from_bytes(&bytes));
        true
    }

    /// Starts listening. Returns immediately; wait for
    /// [signal bound] or [signal bind_failed].
    ///
    /// Without a prior [method set_secret_key] the identity is
    /// random and changes every run.
    #[func]
    fn bind(&mut self) {
        if self.pending_bind.is_some() || self.dispatcher.is_some() {
            crate::log::warning!("endpoint is already bound or binding");
            return;
        }

        // Starts the network runtime if this is the first endpoint to want it.
        if self.lease.is_none() {
            self.lease = runtime::Lease::acquire();
        }

        let secret_key = match self.secret_key.clone() {
            Some(key) => key,
            None => {
                crate::log::info!("no secret key set, using a throwaway identity");
                SecretKey::generate()
            }
        };

        let relay_mode = self.relay_mode.clone();
        let lookup = self.lookup.clone();
        let dns_lookup = self.dns_lookup;
        let local_discovery = self.local_discovery;
        let local_service = self.local_service.clone();

        let (sender, receiver) = oneshot::channel();
        let spawned = runtime::spawn(async move {
            // ALPNs are left to the dispatcher, which publishes them as
            // protocols register and withdraws them as they finish.
            let mut builder = Endpoint::builder(presets::N0)
                .secret_key(secret_key)
                .address_lookup(lookup)
                .relay_mode(relay_mode);

            if !dns_lookup {
                builder = builder.clear_address_lookup();
            }

            if local_discovery {
                builder = builder
                    .address_lookup(MdnsAddressLookup::builder().service_name(local_service));
            }

            let result = builder.bind().await;

            // Nothing to do if the receiver is gone; the extension is shutting down.
            let _ = sender.send(result);
        });

        if spawned.is_none() {
            crate::log::error!("cannot bind: the network runtime is not running");
            return;
        }

        self.pending_bind = Some(receiver);
        self.refresh_ticking();
    }

    /// Drains work finished on the runtime. Connected to `SceneTree`'s
    /// `process_frame`, so it always runs on the main thread.
    #[func]
    fn _drain(&mut self) {
        self.drain_bind();
        self.drain_listeners();
        self.drain_questions();
        self.drain_document_questions();
    }

    /// This peer's public id, or an empty string while unbound. Other peers use
    /// it to dial you.
    #[func]
    fn endpoint_id(&self) -> GString {
        match self.endpoint() {
            Some(endpoint) => GString::from(&endpoint.id().to_string()),
            None => GString::new(),
        }
    }

    /// Whether the endpoint is currently listening.
    #[func]
    fn is_bound(&self) -> bool {
        self.dispatcher.is_some()
    }

    /// Dials `peer_id` and speaks `alpn` to it, for a protocol of your own
    /// outside Godot's multiplayer.
    ///
    /// Returns straight away; wait for the connection's
    /// `opened` signal before using it. `null` if the endpoint is not bound or
    /// the id is malformed. A bare id needs a lookup service — use
    /// [method connect_to_ticket] on a closed network.
    ///
    /// `alpn` names your protocol and both ends must agree on it. Version it
    /// (`"mygame/chat/1"`) so a later release can change the rules without
    /// confusing an older client.
    #[func]
    fn connect_to(&mut self, peer_id: GString, alpn: GString) -> Option<Gd<IrohConnection>> {
        let Ok(peer) = peer_id.to_string().parse::<EndpointId>() else {
            crate::log::error!("'{peer_id}' is not a valid endpoint id");
            return None;
        };

        self.dial(peer.into(), alpn)
    }

    /// Same as [method connect_to], using a ticket from
    /// [method ticket] instead of a bare id.
    #[func]
    fn connect_to_ticket(&mut self, ticket: GString, alpn: GString) -> Option<Gd<IrohConnection>> {
        let Ok(parsed) = EndpointTicket::decode_string(&ticket.to_string()) else {
            crate::log::error!("that is not a valid gdiroh ticket");
            return None;
        };

        self.dial(parsed.into(), alpn)
    }

    /// Accepts incoming connections for `alpn`, reported through
    /// [signal connection_received].
    ///
    /// Needs the endpoint bound first, but can be called at any point after
    /// that — no rebind. Returns `false` if the endpoint is unbound or another
    /// part of the game already holds that protocol.
    #[func]
    fn listen(&mut self, alpn: GString) -> bool {
        let name = alpn.to_string();
        let alpn = name.clone().into_bytes();

        if alpn.is_empty() {
            crate::log::error!("a protocol name cannot be empty");
            return false;
        }
        if alpn == crate::session::ALPN {
            crate::log::error!("'{name}' is reserved for IrohPeer");
            return false;
        }

        // Queued rather than refused when the bind is still in flight, because
        // `bind()` returns immediately and listening on the next line is the
        // natural thing to write.
        if self.dispatcher.is_none() {
            if self.pending_bind.is_none() {
                crate::log::error!("bind the endpoint before listening for '{name}'");
                return false;
            }
            if !self.pending_listens.contains(&alpn) {
                self.pending_listens.push(alpn);
            }
            return true;
        }

        self.claim(alpn)
    }

    /// Stops accepting connections for `alpn`. Ones already open are unaffected.
    #[func]
    fn stop_listening(&mut self, alpn: GString) {
        let alpn = alpn.to_string().into_bytes();
        self.pending_listens.retain(|queued| queued != &alpn);
        self.listeners.remove(&alpn);
        self.refresh_ticking();
    }

    /// Teaches this endpoint how to reach the peer in `ticket`, and returns its
    /// endpoint id.
    ///
    /// Anything that dials by bare id — [method subscribe]
    /// bootstrapping, most obviously — needs the address resolvable. On a
    /// network with no DNS and no relays, this is how that happens. Returns an
    /// empty string if the ticket is malformed.
    #[func]
    fn remember_peer(&mut self, ticket: GString) -> GString {
        let Ok(parsed) = EndpointTicket::decode_string(&ticket.to_string()) else {
            crate::log::error!("that is not a valid gdiroh ticket");
            return GString::new();
        };

        let addr: EndpointAddr = parsed.into();
        let id = addr.id.to_string();
        self.lookup.add_endpoint_info(addr);
        GString::from(&id)
    }

    /// Joins the gossip topic called `topic`, returning a handle to it.
    ///
    /// Every peer on a topic receives every message, relayed peer to peer with
    /// no server involved — good for lobby listings, presence and chat. The
    /// name is hashed, so both ends only have to agree on the same string.
    ///
    /// `bootstrap` is the endpoint ids of peers already on the topic; you need
    /// at least one to find the swarm, and their addresses have to be
    /// resolvable. Returns `null` if the endpoint is unbound or an id is
    /// malformed. See [gossip](https://www.iroh.computer/proto/iroh-gossip).
    #[func]
    fn subscribe(&mut self, topic: GString, bootstrap: PackedStringArray) -> Option<Gd<IrohTopic>> {
        if self.dispatcher.is_none() {
            crate::log::error!("bind the endpoint before subscribing");
            return None;
        }

        let mut peers = Vec::with_capacity(bootstrap.len());
        for peer in bootstrap.as_slice() {
            match peer.to_string().parse::<EndpointId>() {
                Ok(id) => peers.push(id),
                Err(_) => {
                    crate::log::error!("'{peer}' is not a valid endpoint id");
                    return None;
                }
            }
        }

        let name = topic.to_string();
        let topic = gossip::topic_id(&name);
        Some(IrohTopic::wrap(self.swarm()?.subscribe(topic, peers)))
    }

    /// Keeps blobs at `path` instead of in memory.
    ///
    /// A Godot path such as `user://blobs` is fine. Blobs then survive between
    /// runs, so a player who already has an asset never fetches it twice.
    /// Empty restores in-memory storage. Set this before the first blob
    /// operation; afterwards it does nothing until the endpoint is closed.
    #[func]
    fn set_blob_store_path(&mut self, path: GString) {
        let path = path.to_string();
        self.blob_path = if path.trim().is_empty() {
            String::new()
        } else {
            ProjectSettings::singleton()
                .globalize_path(&GString::from(&path))
                .to_string()
        };
    }

    /// Stores `data` and reports its hash through
    /// [signal IrohTransfer.completed].
    ///
    /// Blobs are named by the hash of their contents, so adding the same bytes
    /// twice costs nothing the second time and any peer naming that hash gets
    /// exactly those bytes back. See
    /// [blobs](https://www.iroh.computer/proto/iroh-blobs).
    #[func]
    fn add_bytes(&mut self, data: PackedByteArray, tag: GString) -> Option<Gd<IrohTransfer>> {
        let data = bytes::Bytes::copy_from_slice(data.as_slice());
        let tag = optional(&tag);
        Some(IrohTransfer::wrap(self.depot()?.add_bytes(data, tag)))
    }

    /// Stores the file at `path`, which may be a Godot path such as
    /// `user://level.dat`.
    #[func]
    fn add_file(&mut self, path: GString, tag: GString) -> Option<Gd<IrohTransfer>> {
        let path = global_path(&path)?;
        let tag = optional(&tag);
        Some(IrohTransfer::wrap(self.depot()?.add_file(path, tag)))
    }

    /// Fetches the blob named `hash` from any of `providers`.
    ///
    /// Their addresses have to be resolvable — on a closed network, call
    /// [method remember_peer] first, or use
    /// [method fetch_blob_ticket], which does it for you.
    #[func]
    fn fetch_blob(
        &mut self,
        hash: GString,
        providers: PackedStringArray,
        tag: GString,
    ) -> Option<Gd<IrohTransfer>> {
        let hash = parse_hash(&hash)?;

        let mut peers = Vec::with_capacity(providers.len());
        for peer in providers.as_slice() {
            match peer.to_string().parse::<EndpointId>() {
                Ok(id) => peers.push(id),
                Err(_) => {
                    crate::log::error!("'{peer}' is not a valid endpoint id");
                    return None;
                }
            }
        }

        let tag = optional(&tag);
        Some(IrohTransfer::wrap(self.depot()?.fetch(hash, peers, tag)))
    }

    /// Fetches using a ticket from [method blob_ticket].
    ///
    /// The ticket carries the provider's addresses as well as the hash, so this
    /// works with no lookup service at all.
    #[func]
    fn fetch_blob_ticket(&mut self, ticket: GString, tag: GString) -> Option<Gd<IrohTransfer>> {
        let Ok(parsed) = BlobTicket::from_str(&ticket.to_string()) else {
            crate::log::error!("that is not a valid blob ticket");
            return None;
        };

        // The ticket's addresses are the only way to reach the provider on a
        // closed network, so they go into the address book before the fetch.
        let hash = parsed.hash();
        let (addr, ..) = parsed.into_parts();
        let provider = addr.id;
        self.lookup.add_endpoint_info(addr);

        let tag = optional(&tag);
        Some(IrohTransfer::wrap(self.depot()?.fetch(
            hash,
            vec![provider],
            tag,
        )))
    }

    /// A shareable string carrying a blob's hash *and* our addresses, so a peer
    /// can fetch it without any lookup service.
    ///
    /// Empty while unbound or if the hash is malformed.
    #[func]
    fn blob_ticket(&self, hash: GString) -> GString {
        let (Some(endpoint), Some(hash)) = (self.endpoint(), parse_hash(&hash)) else {
            return GString::new();
        };

        let ticket = BlobTicket::new(endpoint.addr(), hash, iroh_blobs::BlobFormat::Raw);
        GString::from(&ticket.to_string())
    }

    /// Writes a blob we hold out to `path`, which may be a Godot path.
    #[func]
    fn export_blob(&mut self, hash: GString, path: GString) -> Option<Gd<IrohTransfer>> {
        let hash = parse_hash(&hash)?;
        let path = global_path(&path)?;
        Some(IrohTransfer::wrap(self.depot()?.export(hash, path)))
    }

    /// Reads a blob we hold back into memory, delivered as the `data` argument
    /// of `completed`.
    ///
    /// Fine for anything small. Prefer [method export_blob] for a
    /// whole asset, which streams to disk instead of building the lot in
    /// memory first.
    #[func]
    fn read_blob(&mut self, hash: GString) -> Option<Gd<IrohTransfer>> {
        let hash = parse_hash(&hash)?;
        Some(IrohTransfer::wrap(self.depot()?.read(hash)))
    }

    /// Creates a new document: a key-value store several peers can write to.
    ///
    /// Only you know about it until [method IrohDocument.share] hands out a ticket.
    /// Values live in the blob store and live updates travel over gossip, so
    /// this starts both if they are not already running. See
    /// [documents](https://www.iroh.computer/proto/iroh-docs).
    #[func]
    fn create_document(&mut self) -> Option<Gd<IrohDocument>> {
        Some(IrohDocument::wrap(self.library()?.create()))
    }

    /// Reopens a document already in this endpoint's store, by id.
    ///
    /// Only useful with an on-disk store — see
    /// [method set_blob_store_path] — since an in-memory
    /// one starts empty every run.
    #[func]
    fn open_document(&mut self, id: GString) -> Option<Gd<IrohDocument>> {
        let Some(id) = docs::parse_id(&id.to_string()) else {
            crate::log::error!("'{id}' is not a valid document id");
            return None;
        };

        Some(IrohDocument::wrap(self.library()?.open(id)))
    }

    /// Joins a document someone shared, and starts syncing it.
    #[func]
    fn join_document(&mut self, ticket: GString) -> Option<Gd<IrohDocument>> {
        let Some(ticket) = docs::parse_ticket(&ticket.to_string()) else {
            crate::log::error!("that is not a valid document ticket");
            return None;
        };

        Some(IrohDocument::wrap(self.library()?.join(ticket)))
    }

    /// Asks what the store holds for `hash`, answered by
    /// [signal blob_status].
    ///
    /// The cheap way to decide whether a fetch is needed at all, and to tell a
    /// finished blob from one that stopped part way.
    #[func]
    fn request_blob_status(&mut self, hash: GString) -> bool {
        let Some(parsed) = parse_hash(&hash) else {
            return false;
        };
        let Some(ask) = self.depot().map(|depot| depot.status(parsed)) else {
            return false;
        };

        self.asking(Kind::Status, hash.to_string(), ask);
        true
    }

    /// Asks for every hash in the store, answered by
    /// [signal blob_list].
    #[func]
    fn request_blob_list(&mut self) -> bool {
        let Some(ask) = self.depot().map(Depot::list) else {
            return false;
        };

        self.asking(Kind::Blobs, "blob list", ask);
        true
    }

    /// Names a blob so it survives garbage collection.
    ///
    /// **Blobs with no tag are not kept.** Collection is off by default in this
    /// build, so nothing is deleted today — but an untagged blob is protected
    /// only while it is being imported, and turning collection on would sweep
    /// it. Tag anything a game means to keep, and tagging is also the only way
    /// to make a blob deletable: removing every tag is what lets collection
    /// reclaim it.
    #[func]
    fn tag_blob(&mut self, hash: GString, tag: GString) -> bool {
        let Some(parsed) = parse_hash(&hash) else {
            return false;
        };
        let Some(name) = optional(&tag) else {
            crate::log::error!("a tag name cannot be empty");
            return false;
        };
        let Some(ask) = self.depot().map(|depot| depot.tag(parsed, name)) else {
            return false;
        };

        self.asking(Kind::Mutation, format!("tagging {hash}"), ask);
        true
    }

    /// Removes a tag. Once nothing names a blob, garbage collection may reclaim
    /// it — which is how a game frees space.
    #[func]
    fn untag_blob(&mut self, tag: GString) -> bool {
        let Some(name) = optional(&tag) else {
            crate::log::error!("a tag name cannot be empty");
            return false;
        };
        let Some(ask) = self.depot().map(|depot| depot.untag(name)) else {
            return false;
        };

        self.asking(Kind::Mutation, format!("untagging {tag}"), ask);
        true
    }

    /// Asks for every tag in the store, answered by
    /// [signal tag_list].
    #[func]
    fn request_tag_list(&mut self) -> bool {
        let Some(ask) = self.depot().map(Depot::tags) else {
            return false;
        };

        self.asking(Kind::Tags, "tag list", ask);
        true
    }

    /// Asks which documents this endpoint's store holds, answered by
    /// [signal document_list].
    ///
    /// Without this, [method open_document] is only usable if
    /// the game wrote the id down somewhere itself.
    #[func]
    fn request_document_list(&mut self) -> bool {
        let Some(ask) = self.library().map(Library::list) else {
            return false;
        };

        self.asking_documents(DocKind::Documents, "document list", ask);
        true
    }

    /// Forgets a document and drops what we hold of it.
    #[func]
    fn drop_document(&mut self, id: GString) -> bool {
        let Some(parsed) = docs::parse_id(&id.to_string()) else {
            crate::log::error!("'{id}' is not a valid document id");
            return false;
        };
        let Some(ask) = self.library().map(|library| library.drop_doc(parsed)) else {
            return false;
        };

        self.asking_documents(DocKind::Mutation, format!("dropping {id}"), ask);
        true
    }

    /// Asks which author identities this endpoint can write as, answered by
    /// [signal author_list].
    #[func]
    fn request_author_list(&mut self) -> bool {
        let Some(ask) = self.library().map(Library::authors) else {
            return false;
        };

        self.asking_documents(DocKind::Authors, "author list", ask);
        true
    }

    /// Makes a new author identity, answered by
    /// [signal author_created].
    ///
    /// Every document write is attributed to an author. One is enough for most
    /// games; more than one is for telling local profiles apart.
    #[func]
    fn create_author(&mut self) -> bool {
        let Some(ask) = self.library().map(Library::create_author) else {
            return false;
        };

        self.asking_documents(DocKind::NewAuthor, "creating an author", ask);
        true
    }

    /// Chooses which author later document writes are made as.
    #[func]
    fn set_default_author(&mut self, author: GString) -> bool {
        let Some(parsed) = docs::parse_author(&author.to_string()) else {
            crate::log::error!("'{author}' is not a valid author id");
            return false;
        };
        let Some(ask) = self
            .library()
            .map(|library| library.set_default_author(parsed))
        else {
            return false;
        };

        self.asking_documents(DocKind::Mutation, format!("choosing author {author}"), ask);
        true
    }

    /// Every counter iroh collects, as `{ group: { name: value } }`.
    ///
    /// Each counter is accompanied by `<name>__help` describing it. Nothing here
    /// names an individual counter, so this reports whatever the linked version
    /// of iroh collects rather than a list that can fall out of date. Empty
    /// while unbound. A snapshot, not a live view — read it when you want to
    /// show it.
    #[func]
    fn get_metrics(&self) -> VarDictionary {
        match self.endpoint() {
            Some(endpoint) => stats::metrics(endpoint.metrics()),
            None => VarDictionary::new(),
        }
    }

    /// The relays this endpoint is currently reachable through.
    ///
    /// Empty means no relay is in use — either none is configured, or every
    /// peer is reachable directly.
    #[func]
    fn home_relays(&self) -> PackedStringArray {
        let Some(endpoint) = self.endpoint() else {
            return PackedStringArray::new();
        };

        // `Watcher::get` takes `&mut self`, so the watcher is bound first.
        let mut status = endpoint.home_relay_status();
        status
            .get()
            .iter()
            .map(|status| GString::from(&status.url().to_string()))
            .collect()
    }

    /// The addresses other peers can reach this endpoint on right now.
    ///
    /// These change as the network does — a peer that moves between networks
    /// gets different ones — so read this rather than caching it.
    #[func]
    fn direct_addresses(&self) -> PackedStringArray {
        let Some(endpoint) = self.endpoint() else {
            return PackedStringArray::new();
        };

        endpoint
            .addr()
            .addrs
            .iter()
            .map(|addr| GString::from(&addr.to_string()))
            .collect()
    }

    /// Whether the endpoint has been closed.
    #[func]
    fn is_closed(&self) -> bool {
        match self.endpoint() {
            Some(endpoint) => endpoint.is_closed(),
            None => true,
        }
    }

    /// Stops listening and drops every connection. Binding again afterwards is
    /// allowed.
    #[func]
    fn close(&mut self) {
        self.teardown();
        self.refresh_ticking();
    }
}

impl IrohEndpoint {
    /// The bound endpoint's dispatcher, for anything that needs to run a
    /// protocol on this endpoint. `None` while unbound.
    pub(crate) fn dispatcher(&self) -> Option<Dispatcher> {
        self.dispatcher.clone()
    }

    /// Everything closing has to do that does not touch Godot, shared between
    /// [`close`][Self::close] and `Drop`. Idempotent; the second run is a no-op.
    fn teardown(&mut self) {
        // A bind still in flight is abandoned rather than left to land on an
        // endpoint that has already been told to go away.
        self.pending_bind = None;
        self.listeners.clear();
        self.pending_listens.clear();
        self.swarm = None;
        self.depot = None;
        self.library = None;

        // Dropping the dispatcher stops the accept loop; closing the endpoint
        // ends every connection still open on it.
        if let Some(dispatcher) = self.dispatcher.take() {
            let endpoint = dispatcher.endpoint().clone();
            runtime::spawn(async move { endpoint.close().await });
            crate::log::info!("endpoint closed");
        }

        // Stops the runtime if no other endpoint is still using it. The close
        // task above keeps running: shutdown gives it the grace period first.
        self.lease = None;
    }
}

impl Drop for IrohEndpoint {
    /// Releasing the last reference closes the endpoint. The frame subscription
    /// needs no disconnect here — Godot removes a dying object's signal
    /// connections itself, and `to_gd()` is off limits during drop anyway.
    fn drop(&mut self) {
        self.teardown();
    }
}
