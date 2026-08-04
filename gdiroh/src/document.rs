//! `IrohDocument` — a multi-writer key-value store, as a Godot object.
//!
//! ```gdscript
//! var world := endpoint.create_document()
//! world.opened.connect(func(_id): world.share(true))
//! world.shared.connect(func(ticket): tell_players(ticket))
//! world.entry.connect(func(key, _author, _hash, _len, from):
//!     if not from.is_empty():
//!         print(from, " changed ", key))
//! ```
//!
//! Keep the returned object in a variable. It is reference counted, and dropping
//! the last reference closes the document.

use bytes::Bytes;
use godot::classes::RefCounted;
use godot::prelude::*;
use iroh::EndpointId;

use crate::docs::{Document, Event};
use crate::endpoint::scene_tree;

/// A key-value store several peers can write to at once.
///
/// Every peer holds the whole document and may write any key; edits reconcile on
/// their own, and where two peers wrote the same key the later one wins. That
/// suits world edits, inventories and settings — it is not a transaction, so
/// anything needing peers to agree before committing wants an [IrohConnection]
/// instead. Comes from [method IrohEndpoint.create_document],
/// [method IrohEndpoint.open_document] or [method IrohEndpoint.join_document].
///
/// Values are stored as blobs and live updates travel over gossip, so a document
/// starts both of those if they are not already running.
#[derive(GodotClass)]
// `no_init` because documents come from an endpoint, never from `new()`.
#[class(no_init, base=RefCounted)]
pub struct IrohDocument {
    /// Present until the document closes.
    document: Option<Document>,
    id: String,
    ticking: bool,
    base: Base<RefCounted>,
}

impl IrohDocument {
    pub(crate) fn wrap(document: Document) -> Gd<Self> {
        let mut object = Gd::from_init_fn(|base| Self {
            document: Some(document),
            id: String::new(),
            ticking: false,
            base,
        });
        object.bind_mut().start_ticking();
        object
    }

    fn start_ticking(&mut self) {
        if self.ticking {
            return;
        }

        let Some(mut tree) = scene_tree() else {
            crate::log::error!("no scene tree to poll on; this document cannot report anything");
            return;
        };

        let callable = Callable::from_object_method(&self.to_gd(), "_drain");
        tree.connect("process_frame", &callable);
        self.ticking = true;
    }

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

    /// Emits after the current call unwinds. Emitting inline would re-enter this
    /// object while it is still mutably borrowed, which panics.
    fn emit_later(&mut self, signal: &str, args: &[Variant]) {
        let mut call = Vec::with_capacity(args.len() + 1);
        call.push(signal.to_variant());
        call.extend_from_slice(args);
        self.base_mut().call_deferred("emit_signal", &call);
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Opened(id) => {
                self.id = id.to_string();
                let id = GString::from(&self.id);
                self.emit_later("opened", &[id.to_variant()]);
            }
            Event::Entry {
                key,
                author,
                hash,
                len,
                from,
            } => {
                let from = from.map(|peer| peer.to_string()).unwrap_or_default();
                self.emit_later(
                    "entry",
                    &[
                        text(&key).to_variant(),
                        GString::from(&author.to_string()).to_variant(),
                        GString::from(&hash.to_string()).to_variant(),
                        (len as i64).to_variant(),
                        GString::from(&from).to_variant(),
                    ],
                );
            }
            Event::ContentReady(hash) => {
                let hash = GString::from(&hash.to_string());
                self.emit_later("content_ready", &[hash.to_variant()]);
            }
            Event::Value { key, data } => {
                let found = data.is_some();
                let data = data.unwrap_or_default();
                self.emit_later(
                    "value",
                    &[
                        text(&key).to_variant(),
                        PackedByteArray::from(&data[..]).to_variant(),
                        found.to_variant(),
                    ],
                );
            }
            Event::Keys { prefix, entries } => {
                // One dictionary per key, so a game can show a listing without
                // fetching any values.
                let mut listed = VarArray::new();
                for entry in entries {
                    let mut row = VarDictionary::new();
                    row.set("key", &text(&entry.key).to_variant());
                    row.set(
                        "author",
                        &GString::from(&entry.author.to_string()).to_variant(),
                    );
                    row.set("hash", &GString::from(&entry.hash.to_string()).to_variant());
                    row.set("length", &(entry.len as i64).to_variant());
                    listed.push(&row.to_variant());
                }

                self.emit_later("keys", &[text(&prefix).to_variant(), listed.to_variant()]);
            }
            Event::Status {
                syncing,
                subscribers,
                handles,
            } => {
                self.emit_later(
                    "status",
                    &[
                        syncing.to_variant(),
                        (subscribers as i64).to_variant(),
                        (handles as i64).to_variant(),
                    ],
                );
            }
            Event::Shared(ticket) => {
                let ticket = GString::from(&ticket);
                self.emit_later("shared", &[ticket.to_variant()]);
            }
            Event::SyncFinished(peer) => {
                let peer = GString::from(&peer.to_string());
                self.emit_later("sync_finished", &[peer.to_variant()]);
            }
            Event::NeighborUp(peer) => {
                let peer = GString::from(&peer.to_string());
                self.emit_later("neighbor_up", &[peer.to_variant()]);
            }
            Event::NeighborDown(peer) => {
                let peer = GString::from(&peer.to_string());
                self.emit_later("neighbor_down", &[peer.to_variant()]);
            }
            Event::Closed(reason) => {
                self.document = None;
                self.stop_ticking();
                self.emit_later("closed", &[GString::from(&reason).to_variant()]);
            }
        }
    }
}

/// Keys are bytes on the wire. Script works in strings, and a key that is not
/// valid UTF-8 came from somewhere else, so it is shown rather than dropped.
fn text(key: &Bytes) -> GString {
    GString::from(String::from_utf8_lossy(key).as_ref())
}

#[godot_api]
impl IrohDocument {
    /// Emitted once the document is open and usable, carrying its id.
    #[signal]
    fn opened(id: GString);

    /// Emitted for every key written, by us or by a peer.
    ///
    /// `from` is the peer that sent it, or empty when we wrote it ourselves.
    /// `hash` names the value; the bytes may still be arriving, so read the key
    /// rather than assuming they are here.
    #[signal]
    fn entry(key: GString, author: GString, hash: GString, length: i64, from: GString);

    /// Emitted when a value's bytes have finished arriving and can be read.
    #[signal]
    fn content_ready(hash: GString);

    /// Emitted in reply to [method read].
    ///
    /// `found` is false when the key is unset — or when its value has not
    /// arrived yet, which `content_ready` will announce.
    #[signal]
    fn value(key: GString, data: PackedByteArray, found: bool);

    /// Emitted in reply to [method list_keys].
    ///
    /// `entries` is one dictionary per key, with `key`, `author`, `hash` and
    /// `length`. Values are not included — read the ones you want.
    #[signal]
    fn keys(prefix: GString, entries: VarArray);

    /// Emitted in reply to [method request_status].
    #[signal]
    fn status(syncing: bool, subscribers: i64, handles: i64);

    /// Emitted in reply to [method share].
    #[signal]
    fn shared(ticket: GString);

    /// Emitted when a round of syncing with `peer` finishes.
    #[signal]
    fn sync_finished(peer: GString);

    /// Emitted when a peer joins this document's swarm.
    #[signal]
    fn neighbor_up(peer: GString);

    /// Emitted when a peer leaves it.
    #[signal]
    fn neighbor_down(peer: GString);

    /// Emitted when the document closes. Nothing follows it.
    #[signal]
    fn closed(reason: GString);

    /// Drains work finished on the runtime. Connected to `SceneTree`'s
    /// `process_frame`, so it always runs on the main thread.
    #[func]
    fn _drain(&mut self) {
        let Some(document) = self.document.as_mut() else {
            return;
        };

        // Collected first because handling an event borrows `self` again.
        let mut pending = Vec::new();
        while let Some(event) = document.try_recv() {
            pending.push(event);
        }

        for event in pending {
            self.handle(event);
        }
    }

    /// Writes `value` at `key`, replacing whatever was there.
    ///
    /// The write comes back as an [signal entry] signal once it lands.
    /// Where two peers write the same key, the later one wins — so this suits
    /// state that tolerates that rule rather than anything needing agreement.
    #[func]
    fn set(&mut self, key: GString, value: PackedByteArray) -> bool {
        match self.document.as_ref() {
            Some(document) => document.set(
                Bytes::from(key.to_string()),
                Bytes::copy_from_slice(value.as_slice()),
            ),
            None => false,
        }
    }

    /// Removes every key starting with `prefix`. An empty prefix clears the
    /// document.
    #[func]
    fn delete_prefix(&mut self, prefix: GString) -> bool {
        match self.document.as_ref() {
            Some(document) => document.delete(Bytes::from(prefix.to_string())),
            None => false,
        }
    }

    /// Asks for a key's value, answered by [signal value].
    #[func]
    fn read(&mut self, key: GString) -> bool {
        match self.document.as_ref() {
            Some(document) => document.read(Bytes::from(key.to_string())),
            None => false,
        }
    }

    /// Asks what keys the document holds under `prefix`, answered by
    /// [signal keys]. An empty prefix lists everything.
    ///
    /// This is the only way to discover what is already in a document you
    /// joined — `entry` only fires for writes that happen while you are
    /// watching.
    #[func]
    fn list_keys(&mut self, prefix: GString) -> bool {
        match self.document.as_ref() {
            Some(document) => document.list_keys(Bytes::from(prefix.to_string())),
            None => false,
        }
    }

    /// Asks how the document is getting on, answered by
    /// [signal status].
    #[func]
    fn request_status(&mut self) -> bool {
        match self.document.as_ref() {
            Some(document) => document.status(),
            None => false,
        }
    }

    /// Asks for a ticket others can join with, answered by
    /// [signal shared].
    ///
    /// `writable` decides whether they may only read the document or write to
    /// it as well. A write ticket hands over the document's write key, and
    /// cannot be taken back — treat it accordingly.
    #[func]
    fn share(&mut self, writable: bool) -> bool {
        match self.document.as_ref() {
            Some(document) => document.share(writable),
            None => false,
        }
    }

    /// Starts syncing with more peers, by endpoint id.
    ///
    /// Their addresses have to be resolvable — on a closed network that means
    /// the endpoint's [method IrohEndpoint.remember_peer] first.
    #[func]
    fn join_peers(&mut self, peers: PackedStringArray) -> bool {
        let Some(document) = self.document.as_ref() else {
            return false;
        };

        let mut parsed = Vec::with_capacity(peers.len());
        for peer in peers.as_slice() {
            match peer.to_string().parse::<EndpointId>() {
                Ok(id) => parsed.push(id.into()),
                Err(_) => {
                    crate::log::error!("'{peer}' is not a valid endpoint id");
                    return false;
                }
            }
        }

        document.join(parsed)
    }

    /// Stops syncing. What we already hold stays readable.
    #[func]
    fn leave(&mut self) -> bool {
        match self.document.as_ref() {
            Some(document) => document.leave(),
            None => false,
        }
    }

    /// This document's id, or empty until [signal opened] fires. Pass
    /// it to the endpoint's `open_document` to reopen it later.
    #[func]
    fn get_id(&self) -> GString {
        GString::from(&self.id)
    }

    /// Whether the document is open.
    #[func]
    fn is_open(&self) -> bool {
        self.document.is_some()
    }

    /// Closes the document. No further signals follow.
    #[func]
    fn close(&mut self) {
        self.document = None;
        self.stop_ticking();
    }
}
