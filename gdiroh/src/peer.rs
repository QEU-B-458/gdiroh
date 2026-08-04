//! `IrohPeer` — a Godot [`MultiplayerPeer`] backed by iroh.
//!
//! Assigning one to `multiplayer.multiplayer_peer` makes Godot's high-level
//! multiplayer — RPCs, `MultiplayerSpawner`, `MultiplayerSynchronizer` — run
//! over peer-to-peer QUIC instead of a server socket.
//!
//! ```gdscript
//! var peer := IrohPeer.new()
//! peer.host(endpoint)              # or peer.join(endpoint, host_id)
//! multiplayer.multiplayer_peer = peer
//! ```
//!
//! [`MultiplayerPeer`]: https://docs.godotengine.org/en/stable/classes/class_multiplayerpeer.html

use std::collections::VecDeque;

use bytes::Bytes;
use godot::classes::multiplayer_peer::{ConnectionStatus, TransferMode};
use godot::classes::{IMultiplayerPeerExtension, MultiplayerPeerExtension};
use godot::global::Error;
use godot::prelude::*;
// Not reachable through the `godot` facade; see the note in Cargo.toml.
use godot_core::meta::RawPtr;
use iroh::EndpointId;
use iroh_tickets::Ticket;
use iroh_tickets::endpoint::EndpointTicket;

use crate::endpoint::IrohEndpoint;
use crate::session::{Event, HOST_ID, Mode, Session};

/// Largest packet the reliable path will carry. Datagrams are additionally
/// bounded by the path MTU, which the unreliable modes handle separately.
const MAX_PACKET_SIZE: i32 = 8 * 1024 * 1024;

/// A packet on its way to Godot.
struct Packet {
    peer: i32,
    channel: i32,
    mode: TransferMode,
    data: Bytes,
}

/// A Godot [MultiplayerPeer] backed by iroh, so Godot's own high-level
/// multiplayer runs peer-to-peer with no server socket.
///
/// Assign one to [code]multiplayer.multiplayer_peer[/code] and RPCs,
/// [MultiplayerSpawner] and [MultiplayerSynchronizer] work unchanged. Create
/// it, then call [method host] or [method join] with a bound [IrohEndpoint].
/// The session is one protocol on that endpoint, so the same endpoint can
/// carry your own protocols, gossip and blobs alongside it.
///
/// [codeblock]
/// var peer := IrohPeer.new()
/// peer.host(endpoint)
/// multiplayer.multiplayer_peer = peer
/// [/codeblock]
#[derive(GodotClass)]
// `tool` is mandatory: MultiplayerPeerExtension is a virtual extension class,
// so Godot instantiates it in the editor as well as at runtime.
#[class(tool, base=MultiplayerPeerExtension)]
pub struct IrohPeer {
    session: Option<Session>,
    /// Held while a session runs, so the endpoint cannot be freed out from
    /// under it by a game that dropped its own reference.
    endpoint: Option<Gd<IrohEndpoint>>,
    unique_id: i32,
    target_peer: i32,
    transfer_channel: i32,
    transfer_mode: TransferMode,
    refuse_new_connections: bool,
    status: ConnectionStatus,

    /// Packets waiting to be handed to Godot.
    incoming: VecDeque<Packet>,

    /// The packet most recently handed out. Godot reads it through a borrowed
    /// pointer, so it has to outlive the call that returned it.
    current: Option<Packet>,

    base: Base<MultiplayerPeerExtension>,
}

#[godot_api]
impl IrohPeer {
    /// Emitted when the transport wants the developer's attention but carries
    /// on regardless.
    ///
    /// The one that fires in practice is a packet too large for an unreliable
    /// datagram, which is sent reliably instead — once per channel, not once
    /// per packet. Worth watching while tuning what a game puts on the
    /// unreliable path.
    #[signal]
    fn warning(text: GString);

    /// Starts a session as the host, on `endpoint`. The host is always peer 1.
    ///
    /// The endpoint must be bound already; returns `false` if it is not. The
    /// peer holds the endpoint for as long as the session runs.
    #[func]
    fn host(&mut self, endpoint: Gd<IrohEndpoint>) -> bool {
        let Some(dispatcher) = endpoint.bind().dispatcher() else {
            crate::log::error!("bind the endpoint before hosting");
            return false;
        };

        let session = Session::host(&dispatcher);
        session.set_refuse_new_connections(self.refuse_new_connections);
        self.session = Some(session);
        self.endpoint = Some(endpoint);
        // A host is peer 1 from the outset. Waiting for the session to confirm it
        // would leave `is_server()` false until the first poll.
        self.unique_id = HOST_ID;
        self.status = ConnectionStatus::CONNECTING;
        true
    }

    /// Joins, through `endpoint`, the session hosted by `endpoint_id` — which
    /// the host reads from its own endpoint's
    /// [method IrohEndpoint.endpoint_id].
    ///
    /// A bare id has to be resolved through a lookup service, so this needs DNS
    /// lookup left on. Use [method join_ticket] on a closed network.
    #[func]
    fn join(&mut self, endpoint: Gd<IrohEndpoint>, endpoint_id: GString) -> bool {
        let Ok(host) = endpoint_id.to_string().parse::<EndpointId>() else {
            crate::log::error!("'{endpoint_id}' is not a valid endpoint id");
            return false;
        };

        self.start_join(endpoint, host)
    }

    /// Path to `peer` is unknown — not connected, or still being established.
    #[constant]
    const PATH_UNKNOWN: i32 = 0;

    /// Traffic to `peer` is going through a relay. It works, but it costs
    /// latency and someone else's bandwidth.
    #[constant]
    const PATH_RELAY: i32 = 1;

    /// Traffic to `peer` is peer-to-peer. Hole punching succeeded.
    #[constant]
    const PATH_DIRECT: i32 = 2;

    /// Whether traffic to `peer` is direct or relayed right now.
    ///
    /// iroh starts on a relay and upgrades to a direct path once hole punching
    /// succeeds, so this can change during a session — poll it rather than
    /// reading it once.
    #[func]
    fn get_peer_path_type(&self, peer: i32) -> i32 {
        match self.session.as_ref().and_then(|s| s.peer_path(peer)) {
            Some(path) if path.relay => Self::PATH_RELAY,
            Some(_) => Self::PATH_DIRECT,
            None => Self::PATH_UNKNOWN,
        }
    }

    /// The endpoint id behind a Godot peer id, or an empty string if we hold no
    /// connection to it.
    ///
    /// Godot's peer ids (1, 2, 3…) are local to a session and mean nothing
    /// elsewhere. This is what ties a multiplayer peer to the rest of the
    /// plugin — a blob ticket, a document author, a gossip neighbour. Clients
    /// only connect to the host, so a client can name the host and no one else.
    #[func]
    fn get_peer_endpoint_id(&self, peer: i32) -> GString {
        match self.session.as_ref().and_then(|s| s.peer_endpoint_id(peer)) {
            Some(id) => GString::from(&id),
            None => GString::new(),
        }
    }

    /// Round-trip time to `peer` in milliseconds, or `-1.0` if unknown.
    #[func]
    fn get_peer_latency_ms(&self, peer: i32) -> f64 {
        match self.session.as_ref().and_then(|s| s.peer_path(peer)) {
            Some(path) => path.rtt.as_secs_f64() * 1000.0,
            None => -1.0,
        }
    }

    /// Traffic counters for the connection to `peer`, in the same shape as
    /// [method IrohConnection.get_stats]. Empty if there is no direct
    /// connection.
    #[func]
    fn get_peer_stats(&self, peer: i32) -> VarDictionary {
        match self.session.as_ref().and_then(|s| s.peer_connection(peer)) {
            Some(connection) => crate::stats::connection(&connection),
            None => VarDictionary::new(),
        }
    }

    /// Joins using a ticket from the host endpoint's
    /// [method IrohEndpoint.ticket].
    ///
    /// The ticket carries the host's addresses as well as its id, so this works
    /// with no lookup service at all.
    #[func]
    fn join_ticket(&mut self, endpoint: Gd<IrohEndpoint>, ticket: GString) -> bool {
        let Ok(parsed) = EndpointTicket::decode_string(&ticket.to_string()) else {
            crate::log::error!("that is not a valid gdiroh ticket");
            return false;
        };

        self.start_join(endpoint, parsed)
    }
}

#[godot_api]
impl IMultiplayerPeerExtension for IrohPeer {
    fn init(base: Base<MultiplayerPeerExtension>) -> Self {
        Self {
            session: None,
            endpoint: None,
            // Zero until a session assigns one; Godot treats it as "no peer".
            unique_id: 0,
            target_peer: 0,
            transfer_channel: 0,
            transfer_mode: TransferMode::RELIABLE,
            refuse_new_connections: false,
            status: ConnectionStatus::DISCONNECTED,
            incoming: VecDeque::new(),
            current: None,
            base,
        }
    }

    fn get_available_packet_count(&self) -> i32 {
        self.incoming.len() as i32
    }

    fn get_max_packet_size(&self) -> i32 {
        MAX_PACKET_SIZE
    }

    // These three describe the packet `get_packet` is about to return, not the
    // one it last returned — `MultiplayerAPI` reads them before each fetch. They
    // peek the queue for that reason; `current` only exists to keep the bytes of
    // an already-handed-out packet alive.

    fn get_packet_channel(&self) -> i32 {
        self.incoming.front().map_or(0, |packet| packet.channel)
    }

    fn get_packet_mode(&self) -> TransferMode {
        self.incoming
            .front()
            .map_or(TransferMode::RELIABLE, |packet| packet.mode)
    }

    fn get_packet_peer(&self) -> i32 {
        self.incoming.front().map_or(0, |packet| packet.peer)
    }

    /// Hands Godot a borrowed view of the next packet — the path
    /// `MultiplayerAPI` actually uses.
    ///
    /// The bytes stay owned by `self.current` until the following call, which is
    /// exactly the lifetime Godot expects, so nothing is copied per packet.
    unsafe fn get_packet_rawptr(
        &mut self,
        r_buffer: RawPtr<*mut RawPtr<*const u8>>,
        r_buffer_size: RawPtr<*mut i32>,
    ) -> Error {
        let Some(packet) = self.incoming.pop_front() else {
            return Error::ERR_UNAVAILABLE;
        };

        // Parking it here is what keeps the pointer below alive.
        let packet = self.current.insert(packet);

        // SAFETY: Godot passes valid out-pointers, and the buffer they will point
        // at lives in `self.current` until the next call replaces it.
        unsafe {
            *r_buffer.ptr() = RawPtr::new(packet.data.as_ptr());
            *r_buffer_size.ptr() = packet.data.len() as i32;
        }

        Error::OK
    }

    /// Takes a packet from Godot without going through `PackedByteArray`.
    unsafe fn put_packet_rawptr(
        &mut self,
        p_buffer: RawPtr<*const u8>,
        p_buffer_size: i32,
    ) -> Error {
        if p_buffer_size < 0 {
            return Error::ERR_INVALID_PARAMETER;
        }

        let data = if p_buffer_size == 0 {
            // `from_raw_parts` demands a non-null aligned pointer even for an
            // empty slice, which an empty packet does not promise.
            &[][..]
        } else {
            // SAFETY: Godot guarantees `p_buffer` is readable for `p_buffer_size`
            // bytes for the duration of this call.
            unsafe { std::slice::from_raw_parts(p_buffer.ptr(), p_buffer_size as usize) }
        };

        self.send(data)
    }

    /// Script-facing counterpart of [`Self::get_packet_rawptr`], used when
    /// GDScript calls `get_packet()` directly.
    fn get_packet_script(&mut self) -> PackedByteArray {
        match self.incoming.pop_front() {
            Some(packet) => {
                let bytes = PackedByteArray::from(&packet.data[..]);
                self.current = Some(packet);
                bytes
            }
            None => PackedByteArray::new(),
        }
    }

    /// Sends a packet on the current channel and transfer mode.
    fn put_packet_script(&mut self, p_buffer: PackedByteArray) -> Error {
        self.send(p_buffer.as_slice())
    }

    fn set_transfer_channel(&mut self, p_channel: i32) {
        self.transfer_channel = p_channel;
    }

    fn get_transfer_channel(&self) -> i32 {
        self.transfer_channel
    }

    fn set_transfer_mode(&mut self, p_mode: TransferMode) {
        self.transfer_mode = p_mode;
    }

    fn get_transfer_mode(&self) -> TransferMode {
        self.transfer_mode
    }

    fn set_target_peer(&mut self, p_peer: i32) {
        self.target_peer = p_peer;
    }

    fn is_server(&self) -> bool {
        self.unique_id == HOST_ID
    }

    /// The host relays between clients, so Godot may address peers it has no
    /// direct link to.
    fn is_server_relay_supported(&self) -> bool {
        true
    }

    /// Called by `MultiplayerAPI` every frame, always on the main thread.
    fn poll(&mut self) {
        // Drained up front so the session is no longer borrowed while the
        // handlers below emit signals.
        let mut events = Vec::new();
        if let Some(session) = self.session.as_mut() {
            while let Some(event) = session.try_recv() {
                events.push(event);
            }
        }

        for event in events {
            self.handle(event);
        }
    }

    fn close(&mut self) {
        if let Some(session) = self.session.take() {
            session.close();
        }

        self.endpoint = None;
        self.incoming.clear();
        self.current = None;
        self.unique_id = 0;
        self.status = ConnectionStatus::DISCONNECTED;
    }

    fn disconnect_peer(&mut self, p_peer: i32, _p_force: bool) {
        if let Some(session) = &self.session {
            session.disconnect(p_peer);
        }
    }

    fn get_unique_id(&self) -> i32 {
        self.unique_id
    }

    fn set_refuse_new_connections(&mut self, p_enable: bool) {
        self.refuse_new_connections = p_enable;

        // Pushed down so the accept loop turns peers away before the handshake,
        // instead of admitting them and disconnecting a moment later.
        if let Some(session) = &self.session {
            session.set_refuse_new_connections(p_enable);
        }
    }

    fn is_refusing_new_connections(&self) -> bool {
        self.refuse_new_connections
    }

    fn get_connection_status(&self) -> ConnectionStatus {
        self.status
    }
}

impl IrohPeer {
    /// Shared tail of [`IrohPeer::join`] and [`IrohPeer::join_ticket`].
    fn start_join(
        &mut self,
        endpoint: Gd<IrohEndpoint>,
        host: impl Into<iroh::EndpointAddr>,
    ) -> bool {
        let Some(dispatcher) = endpoint.bind().dispatcher() else {
            crate::log::error!("bind the endpoint before joining");
            return false;
        };

        // Dialling your own ticket is a paste mistake, not a session — caught
        // here so the caller finds out now rather than from a refused dial.
        let host: iroh::EndpointAddr = host.into();
        if host.id == dispatcher.endpoint().id() {
            crate::log::error!("an endpoint cannot join its own session");
            return false;
        }

        self.session = Some(Session::join(&dispatcher, host));
        self.endpoint = Some(endpoint);
        self.status = ConnectionStatus::CONNECTING;
        true
    }

    /// Queues a packet for the current target, channel and transfer mode.
    fn send(&mut self, data: &[u8]) -> Error {
        if self.status != ConnectionStatus::CONNECTED {
            return Error::ERR_UNCONFIGURED;
        }

        let Some(session) = &self.session else {
            return Error::ERR_UNCONFIGURED;
        };

        session.send(
            self.target_peer,
            self.transfer_channel,
            to_session_mode(self.transfer_mode),
            Bytes::copy_from_slice(data),
        );

        Error::OK
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Ready(id) => {
                self.unique_id = id;
                self.status = ConnectionStatus::CONNECTED;
            }
            Event::PeerConnected(id) => self.emit("peer_connected", id),
            Event::PeerDisconnected(id) => self.emit("peer_disconnected", id),
            Event::Packet(packet) => self.incoming.push_back(Packet {
                peer: packet.peer,
                channel: packet.channel,
                mode: from_session_mode(packet.mode),
                data: packet.data,
            }),
            Event::Warning(text) => {
                crate::log::warning!("{text}");
                // Also surfaced as a signal: the oversize-datagram fallback is
                // something a game may want to react to, not just read in the
                // console.
                self.base_mut()
                    .emit_signal("warning", &[GString::from(&text).to_variant()]);
            }
            Event::Closed(reason) => {
                crate::log::info!("session ended: {reason}");
                self.session = None;
                self.endpoint = None;
                self.unique_id = 0;
                self.status = ConnectionStatus::DISCONNECTED;
            }
        }
    }

    /// Emitted inline rather than deferred: `MultiplayerAPI` needs its peer list
    /// updated before the packets that follow in this same poll.
    fn emit(&mut self, signal: &str, peer: i32) {
        self.base_mut().emit_signal(signal, &[peer.to_variant()]);
    }
}

fn to_session_mode(mode: TransferMode) -> Mode {
    if mode == TransferMode::UNRELIABLE {
        Mode::Unreliable
    } else if mode == TransferMode::UNRELIABLE_ORDERED {
        Mode::UnreliableOrdered
    } else {
        Mode::Reliable
    }
}

fn from_session_mode(mode: Mode) -> TransferMode {
    match mode {
        Mode::Unreliable => TransferMode::UNRELIABLE,
        Mode::UnreliableOrdered => TransferMode::UNRELIABLE_ORDERED,
        Mode::Reliable => TransferMode::RELIABLE,
    }
}
