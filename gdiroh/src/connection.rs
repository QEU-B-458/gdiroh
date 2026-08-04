//! `IrohConnection` — one peer-to-peer connection speaking a protocol of your
//! own, outside Godot's multiplayer.
//!
//! Use this when `IrohPeer` is the wrong shape: file transfer, a voice channel,
//! a lobby query — anything with its own rules that should not go through
//! `MultiplayerAPI`. Both sides agree on an ALPN, and everything else is yours.
//!
//! ```gdscript
//! var conn := endpoint.connect_to(peer_id, "mygame/chat/1")
//! conn.opened.connect(func():
//!     var stream := conn.open_stream()
//!     stream.put_utf8_string("hello"))
//! ```
//!
//! Keep the returned object in a variable. It is reference counted, so dropping
//! the last reference closes the connection.

use bytes::Bytes;
use godot::classes::RefCounted;
use godot::prelude::*;
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, VarInt};

use crate::dispatch::Dispatcher;
use crate::endpoint::scene_tree;
use crate::raw::{self, Event, Events, Stream};
use crate::stream::IrohStream;

/// Close code used when script closes a connection deliberately.
const CLOSED_BY_SCRIPT: u32 = 0;

/// One connection speaking a protocol of your own, outside Godot's multiplayer.
///
/// For when [IrohPeer] is the wrong shape: file transfer, a voice channel, a
/// lobby query — anything with its own rules. Both ends agree on a protocol
/// name and everything else is yours. Comes from
/// [method IrohEndpoint.connect_to] or the endpoint's
/// [signal IrohEndpoint.connection_received] signal.
///
/// Keep it in a variable. It is reference counted, and dropping the last
/// reference closes the connection.
#[derive(GodotClass)]
// `no_init` because connections come from an endpoint, never from `new()`.
#[class(no_init, base=RefCounted)]
pub struct IrohConnection {
    /// Present until the connection ends.
    events: Option<Events>,
    /// Present once the handshake is done. Accepted connections have it from
    /// the outset.
    connection: Option<Connection>,
    ticking: bool,
    base: Base<RefCounted>,
}

impl IrohConnection {
    /// Dials `peer` and speaks `alpn` to it.
    pub(crate) fn dial(dispatcher: &Dispatcher, peer: EndpointAddr, alpn: Vec<u8>) -> Gd<Self> {
        let events = raw::dial(dispatcher, peer, alpn);
        Self::wrap(events, None)
    }

    /// Wraps a connection the dispatcher accepted for us.
    pub(crate) fn accepted(connection: Connection) -> Gd<Self> {
        let events = raw::adopt(connection.clone());
        Self::wrap(events, Some(connection))
    }

    fn wrap(events: Events, connection: Option<Connection>) -> Gd<Self> {
        let mut object = Gd::from_init_fn(|base| Self {
            events: Some(events),
            connection,
            ticking: false,
            base,
        });
        object.bind_mut().start_ticking();
        object
    }

    /// Subscribes `_drain` to the frame signal while the connection is live.
    fn start_ticking(&mut self) {
        if self.ticking {
            return;
        }

        let Some(mut tree) = scene_tree() else {
            crate::log::error!("no scene tree to poll on; this connection cannot report anything");
            return;
        };

        let callable = Callable::from_object_method(&self.to_gd(), "_drain");
        tree.connect("process_frame", &callable);
        self.ticking = true;
    }

    /// Unsubscribes once the connection is finished with.
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
            Event::Opened(connection) => {
                self.connection = Some(connection);
                self.emit_later("opened", &[]);
            }
            Event::Stream(stream) => {
                let stream = IrohStream::wrap(stream);
                self.emit_later("stream_opened", &[stream.to_variant()]);
            }
            Event::Datagram(data) => {
                let data = PackedByteArray::from(&data[..]);
                self.emit_later("datagram_received", &[data.to_variant()]);
            }
            Event::Closed(reason) => {
                // A connection that never opened failed to dial; one that did
                // has simply ended. They are different problems to the caller.
                let opened = self.connection.is_some();
                self.connection = None;
                self.events = None;
                self.stop_ticking();

                let signal = if opened { "closed" } else { "failed" };
                self.emit_later(signal, &[GString::from(&reason).to_variant()]);
            }
        }
    }
}

#[godot_api]
impl IrohConnection {
    /// Emitted once a dialled connection is usable. Connections you receive
    /// from an endpoint's `connection_received` signal are already open and
    /// never emit this.
    #[signal]
    fn opened();

    /// Emitted when a dial never succeeded. Nothing follows it.
    #[signal]
    fn failed(reason: GString);

    /// Emitted when the remote opens a stream to us.
    #[signal]
    fn stream_opened(stream: Gd<IrohStream>);

    /// Emitted for each datagram. Unreliable and unordered — a datagram may be
    /// lost or overtaken, and is dropped entirely if it exceeds
    /// [method max_datagram_size].
    #[signal]
    fn datagram_received(data: PackedByteArray);

    /// Emitted when the remote or the network ends an open connection.
    ///
    /// Ending it from this side — [method close], or dropping the last
    /// reference — does not emit it: you already know.
    #[signal]
    fn closed(reason: GString);

    /// Path to the remote is unknown — not connected yet, or still settling.
    #[constant]
    const PATH_UNKNOWN: i32 = 0;

    /// Traffic is going through a relay. It works, but it costs latency and
    /// someone else's bandwidth.
    #[constant]
    const PATH_RELAY: i32 = 1;

    /// Traffic is peer-to-peer. Hole punching succeeded.
    #[constant]
    const PATH_DIRECT: i32 = 2;

    /// Drains work finished on the runtime. Connected to `SceneTree`'s
    /// `process_frame`, so it always runs on the main thread.
    #[func]
    fn _drain(&mut self) {
        let Some(events) = self.events.as_mut() else {
            return;
        };

        // Collected first because handling an event borrows `self` again.
        let mut pending = Vec::new();
        while let Some(event) = events.try_recv() {
            pending.push(event);
        }

        for event in pending {
            self.handle(event);
        }
    }

    /// Opens a stream to the remote, which surfaces there as
    /// [signal stream_opened].
    ///
    /// Returns `null` before the connection is open. Streams are bidirectional:
    /// both sides can write, whoever opened it.
    #[func]
    fn open_stream(&mut self) -> Option<Gd<IrohStream>> {
        let Some(connection) = self.connection.as_ref() else {
            crate::log::error!("wait for `opened` before opening a stream");
            return None;
        };

        Some(IrohStream::wrap(Stream::open(connection.clone())))
    }

    /// Sends one datagram: unreliable, unordered, and never split up.
    ///
    /// Returns `false` if it could not be handed to the network — most often
    /// because it is larger than [method max_datagram_size]. Use a stream for
    /// anything that must arrive.
    #[func]
    fn send_datagram(&mut self, data: PackedByteArray) -> bool {
        let Some(connection) = self.connection.as_ref() else {
            crate::log::error!("wait for `opened` before sending a datagram");
            return false;
        };

        match connection.send_datagram(Bytes::copy_from_slice(data.as_slice())) {
            Ok(()) => true,
            Err(err) => {
                crate::log::warning!("datagram not sent: {err}");
                false
            }
        }
    }

    /// Largest datagram this connection will carry right now, or `0` if it is
    /// not open. Follows the path MTU, so it can change during a connection.
    #[func]
    fn max_datagram_size(&self) -> i32 {
        self.connection
            .as_ref()
            .and_then(Connection::max_datagram_size)
            .unwrap_or(0)
            .min(i32::MAX as usize) as i32
    }

    /// The remote's endpoint id, or an empty string before the connection opens.
    #[func]
    fn remote_id(&self) -> GString {
        match &self.connection {
            Some(connection) => GString::from(&connection.remote_id().to_string()),
            None => GString::new(),
        }
    }

    /// The protocol both sides settled on.
    #[func]
    fn alpn(&self) -> GString {
        match &self.connection {
            Some(connection) => GString::from(String::from_utf8_lossy(connection.alpn()).as_ref()),
            None => GString::new(),
        }
    }

    /// Whether the connection is open for business.
    #[func]
    fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    /// Whether traffic is direct or relayed right now.
    ///
    /// iroh starts on a relay and upgrades to a direct path once hole punching
    /// succeeds, so poll this rather than reading it once.
    #[func]
    fn get_path_type(&self) -> i32 {
        match self.connection.as_ref().and_then(raw::path_info) {
            Some(path) if path.relay => Self::PATH_RELAY,
            Some(_) => Self::PATH_DIRECT,
            None => Self::PATH_UNKNOWN,
        }
    }

    /// Round-trip time to the remote in milliseconds, or `-1.0` if unknown.
    #[func]
    fn get_latency_ms(&self) -> f64 {
        match self.connection.as_ref().and_then(raw::path_info) {
            Some(path) => path.rtt.as_secs_f64() * 1000.0,
            None => -1.0,
        }
    }

    /// Traffic counters for this connection: bytes and datagrams each way,
    /// packets lost, round-trip time, and whether it is relayed.
    ///
    /// A snapshot taken when called, not a live view.
    #[func]
    fn get_stats(&self) -> VarDictionary {
        match self.connection.as_ref() {
            Some(connection) => crate::stats::connection(connection),
            None => VarDictionary::new(),
        }
    }

    /// Closes the connection. `reason` reaches the remote, so it is worth
    /// filling in.
    #[func]
    fn close(&mut self, reason: GString) {
        if let Some(connection) = self.connection.take() {
            connection.close(
                VarInt::from_u32(CLOSED_BY_SCRIPT),
                reason.to_string().as_bytes(),
            );
        }

        self.events = None;
        self.stop_ticking();
    }
}
