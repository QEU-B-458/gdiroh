//! `IrohTopic` — a gossip topic you can broadcast on, as a Godot object.
//!
//! ```gdscript
//! var lobby := endpoint.subscribe("mygame/lobbies", [host_id])
//! lobby.message.connect(func(data, from, _direct):
//!     print(from, " says ", data.get_string_from_utf8()))
//! lobby.broadcast("anyone there?".to_utf8_buffer())
//! ```
//!
//! Keep the returned object in a variable. It is reference counted, and dropping
//! the last reference leaves the topic.

use bytes::Bytes;
use godot::classes::RefCounted;
use godot::prelude::*;
use iroh::EndpointId;

use crate::endpoint::scene_tree;
use crate::gossip::{Event, Topic};

/// A gossip topic: every peer subscribed to it receives every message.
///
/// These come from [method IrohEndpoint.subscribe]. Messages are relayed peer
/// to peer with no server and nobody holding the full membership list, which
/// is what lets a topic scale where a mesh of direct connections would not.
/// Good for lobby listings, presence and chat.
///
/// Delivery is best effort: a peer that is still joining can miss a message, and
/// a subscriber that falls behind is dropped rather than allowed to stall the
/// swarm. Use an [IrohStream] when something has to arrive.
#[derive(GodotClass)]
// `no_init` because topics come from an endpoint's `subscribe`, never `new()`.
#[class(no_init, base=RefCounted)]
pub struct IrohTopic {
    /// Present until the topic ends.
    topic: Option<Topic>,
    ticking: bool,
    base: Base<RefCounted>,
}

impl IrohTopic {
    pub(crate) fn wrap(topic: Topic) -> Gd<Self> {
        let mut object = Gd::from_init_fn(|base| Self {
            topic: Some(topic),
            ticking: false,
            base,
        });
        object.bind_mut().start_ticking();
        object
    }

    /// Subscribes `_drain` to the frame signal while the topic is live.
    fn start_ticking(&mut self) {
        if self.ticking {
            return;
        }

        let Some(mut tree) = scene_tree() else {
            crate::log::error!("no scene tree to poll on; this topic cannot report anything");
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
            Event::NeighborUp(peer) => {
                self.emit_later("neighbor_up", &[peer_name(peer).to_variant()]);
            }
            Event::NeighborDown(peer) => {
                self.emit_later("neighbor_down", &[peer_name(peer).to_variant()]);
            }
            Event::Message {
                content,
                from,
                direct,
            } => {
                self.emit_later(
                    "message",
                    &[
                        PackedByteArray::from(&content[..]).to_variant(),
                        peer_name(from).to_variant(),
                        direct.to_variant(),
                    ],
                );
            }
            Event::Lagged => {
                self.finished();
                self.emit_later("lagged", &[]);
            }
            Event::Closed(reason) => {
                self.finished();
                self.emit_later("closed", &[GString::from(&reason).to_variant()]);
            }
        }
    }

    /// Shared teardown for both ways a topic can end.
    fn finished(&mut self) {
        self.topic = None;
        self.stop_ticking();
    }
}

fn peer_name(peer: EndpointId) -> GString {
    GString::from(&peer.to_string())
}

#[godot_api]
impl IrohTopic {
    /// Emitted for each message on the topic.
    ///
    /// `from` is whoever passed it to us, which is not necessarily who wrote
    /// it — gossip relays peer to peer. `direct` is true when it came straight
    /// from its author. Put the author in the payload if you need to know.
    #[signal]
    fn message(data: PackedByteArray, from: GString, direct: bool);

    /// Emitted when a peer becomes a direct neighbour of ours on this topic.
    ///
    /// Neighbours are gossip's own view of the swarm, not the full membership:
    /// messages still reach peers you are not directly attached to.
    #[signal]
    fn neighbor_up(peer: GString);

    /// Emitted when a direct neighbour goes away.
    #[signal]
    fn neighbor_down(peer: GString);

    /// Emitted if we could not keep up and were dropped from the topic.
    ///
    /// Messages were lost and the topic is finished — subscribe again to
    /// rejoin. Exactly one of this and [signal closed] ever fires.
    #[signal]
    fn lagged();

    /// Emitted when the topic ends for any other reason. Nothing follows it.
    #[signal]
    fn closed(reason: GString);

    /// Drains work finished on the runtime. Connected to `SceneTree`'s
    /// `process_frame`, so it always runs on the main thread.
    #[func]
    fn _drain(&mut self) {
        let Some(topic) = self.topic.as_mut() else {
            return;
        };

        // Collected first because handling an event borrows `self` again.
        let mut pending = Vec::new();
        while let Some(event) = topic.try_recv() {
            pending.push(event);
        }

        for event in pending {
            self.handle(event);
        }
    }

    /// Sends to everyone on the topic.
    ///
    /// Best effort: a peer still joining may miss it, and there is no
    /// acknowledgement. Returns `false` once the topic has ended.
    #[func]
    fn broadcast(&mut self, data: PackedByteArray) -> bool {
        match self.topic.as_ref() {
            Some(topic) => topic.broadcast(Bytes::copy_from_slice(data.as_slice())),
            None => false,
        }
    }

    /// Sends only to our direct neighbours, with no onward relaying.
    ///
    /// Cheaper than [method broadcast] and useful for chatter that
    /// does not need to cross the whole swarm, like a heartbeat.
    #[func]
    fn broadcast_neighbors(&mut self, data: PackedByteArray) -> bool {
        match self.topic.as_ref() {
            Some(topic) => topic.broadcast_neighbors(Bytes::copy_from_slice(data.as_slice())),
            None => false,
        }
    }

    /// Dials more peers to widen or repair our view of the swarm.
    ///
    /// Their addresses have to be resolvable — on a closed network that means
    /// the endpoint's [method IrohEndpoint.remember_peer] first.
    #[func]
    fn join_peers(&mut self, peers: PackedStringArray) -> bool {
        let Some(topic) = self.topic.as_ref() else {
            return false;
        };

        let mut parsed = Vec::with_capacity(peers.len());
        for peer in peers.as_slice() {
            match peer.to_string().parse::<EndpointId>() {
                Ok(id) => parsed.push(id),
                Err(_) => {
                    crate::log::error!("'{peer}' is not a valid endpoint id");
                    return false;
                }
            }
        }

        topic.join(parsed)
    }

    /// Our current direct neighbours on this topic.
    #[func]
    fn neighbors(&self) -> PackedStringArray {
        let Some(topic) = self.topic.as_ref() else {
            return PackedStringArray::new();
        };

        topic.neighbors().into_iter().map(peer_name).collect()
    }

    /// Whether we are attached to at least one peer on this topic.
    #[func]
    fn is_joined(&self) -> bool {
        match self.topic.as_ref() {
            Some(topic) => !topic.neighbors().is_empty(),
            None => false,
        }
    }

    /// The hashed id this topic actually uses on the wire.
    ///
    /// Names are hashed, so two games agreeing on a string agree on this. Worth
    /// logging when a subscription is not finding the peers you expect.
    #[func]
    fn get_topic_id(&self) -> GString {
        match self.topic.as_ref() {
            Some(topic) => GString::from(&topic.id()),
            None => GString::new(),
        }
    }

    /// Leaves the topic. No further signals follow.
    #[func]
    fn leave(&mut self) {
        self.finished();
    }
}
