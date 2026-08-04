//! Transport behind `IrohPeer`: handshake, membership, per-channel reliable
//! streams and datagrams.
//!
//! Deliberately free of Godot types, so a whole session can be driven from a
//! test with two endpoints in one process.
//!
//! # Wire format
//!
//! Every unidirectional stream opens with an `i32` channel id, then carries
//! `u32`-length-prefixed frames. Channel [`CONTROL_CHANNEL`] carries membership
//! messages; the rest carry packets for the matching Godot channel.
//!
//! Datagrams are `[i32 channel][u32 sequence][payload]`. Sequence `0` means
//! unordered; anything else is compared against the last seen value so stale
//! datagrams are dropped rather than delivered out of order.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, RecvStream, VarInt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, error::TryRecvError};

use crate::dispatch::{Dispatcher, Registration};
use crate::raw::{self, PathInfo};
use crate::runtime::detach;

/// Protocol identifier negotiated on every gdiroh connection.
pub(crate) const ALPN: &[u8] = b"gdiroh/0";

/// Peer id Godot reserves for the host.
pub(crate) const HOST_ID: i32 = 1;

/// Stream channel reserved for membership messages.
const CONTROL_CHANNEL: i32 = i32::MIN;

/// First id hosts hand out; 1 is always the host itself.
const FIRST_CLIENT_ID: i32 = 2;

/// Stream header plus frame header, in bytes.
const DATAGRAM_HEADER: usize = 8;

/// How a packet should be carried.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    Reliable,
    Unreliable,
    UnreliableOrdered,
}

/// Live connections, readable from the main thread for diagnostics without
/// disturbing the session task that owns everything else.
type Connections = Arc<Mutex<HashMap<i32, Connection>>>;

/// A packet crossing between the network and Godot.
#[derive(Debug)]
pub(crate) struct Packet {
    pub peer: i32,
    pub channel: i32,
    pub mode: Mode,
    pub data: Bytes,
}

/// Something the main thread needs to know about.
#[derive(Debug)]
pub(crate) enum Event {
    /// Our own peer id is settled. Hosts get this immediately.
    Ready(i32),
    PeerConnected(i32),
    PeerDisconnected(i32),
    Packet(Packet),
    /// Something the developer should see. Surfaced as an event so this module
    /// stays free of Godot's logging.
    Warning(String),
    /// The session ended and will produce nothing further.
    Closed(String),
}

/// Work handed to the session task.
enum Command {
    Send {
        target: i32,
        channel: i32,
        mode: Mode,
        data: Bytes,
    },
    Disconnect(i32),
    Close,
}

/// Membership messages exchanged on [`CONTROL_CHANNEL`].
enum Control {
    /// Host to client, once: your id, then everyone already present.
    Welcome {
        your_id: i32,
        peers: Vec<i32>,
    },
    PeerJoined(i32),
    PeerLeft(i32),
}

impl Control {
    fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        match self {
            Control::Welcome { your_id, peers } => {
                buffer.put_u8(1);
                buffer.put_i32(*your_id);
                buffer.put_u32(peers.len() as u32);
                for peer in peers {
                    buffer.put_i32(*peer);
                }
            }
            Control::PeerJoined(id) => {
                buffer.put_u8(2);
                buffer.put_i32(*id);
            }
            Control::PeerLeft(id) => {
                buffer.put_u8(3);
                buffer.put_i32(*id);
            }
        }
        buffer.freeze()
    }

    fn decode(mut frame: Bytes) -> Option<Self> {
        if frame.is_empty() {
            return None;
        }

        match frame.get_u8() {
            1 => {
                if frame.len() < 8 {
                    return None;
                }
                let your_id = frame.get_i32();
                let count = frame.get_u32() as usize;
                if frame.len() < count * 4 {
                    return None;
                }
                let peers = (0..count).map(|_| frame.get_i32()).collect();
                Some(Control::Welcome { your_id, peers })
            }
            2 if frame.len() >= 4 => Some(Control::PeerJoined(frame.get_i32())),
            3 if frame.len() >= 4 => Some(Control::PeerLeft(frame.get_i32())),
            _ => None,
        }
    }
}

/// Reported by a connection's reader tasks back to the session task.
enum Internal {
    Control(Control),
    Lost(i32),
}

/// A live session. Drop or [`Session::close`] to tear it down.
pub(crate) struct Session {
    events: UnboundedReceiver<Event>,
    commands: UnboundedSender<Command>,
    /// Shared with the dispatcher's accept loop, which reads it before the
    /// handshake so a refusal costs nothing beyond a load.
    refuse: Arc<AtomicBool>,
    connections: Connections,
}

impl Session {
    /// Starts listening as the host. The host is always peer 1.
    ///
    /// Claims [`ALPN`] on its endpoint, so one session hosts per endpoint.
    pub(crate) fn host(dispatcher: &Dispatcher) -> Self {
        Self::start(dispatcher, None)
    }

    /// Dials a host and joins its session.
    pub(crate) fn join(dispatcher: &Dispatcher, host: impl Into<EndpointAddr>) -> Self {
        Self::start(dispatcher, Some(host.into()))
    }

    fn start(dispatcher: &Dispatcher, host: Option<EndpointAddr>) -> Self {
        let (events_tx, events) = mpsc::unbounded_channel();
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let connections: Connections = Arc::new(Mutex::new(HashMap::new()));

        // Only a host accepts, so only a host claims the ALPN.
        let registration = match host {
            Some(_) => None,
            None => match dispatcher.register(ALPN) {
                Some(registration) => Some(registration),
                None => {
                    let _ =
                        events_tx.send(Event::Closed("this endpoint is already hosting".into()));
                    return Self {
                        events,
                        commands,
                        refuse: Arc::default(),
                        connections,
                    };
                }
            },
        };

        // A client never accepts, so its flag exists only to be stored into.
        let refuse = registration
            .as_ref()
            .map(Registration::refusals)
            .unwrap_or_default();

        let failed = events_tx.clone();
        if !detach(run(
            dispatcher.clone(),
            host,
            registration,
            events_tx,
            commands_rx,
            connections.clone(),
        )) {
            let _ = failed.send(Event::Closed("the network runtime is not running".into()));
        }

        Self {
            events,
            commands,
            refuse,
            connections,
        }
    }

    /// The endpoint id behind a Godot peer id, if we hold a connection to it.
    ///
    /// Clients only connect to the host, so a client can name the host and
    /// itself; the host can name everyone.
    pub(crate) fn peer_endpoint_id(&self, peer: i32) -> Option<String> {
        let connections = self.connections.lock().ok()?;
        Some(connections.get(&peer)?.remote_id().to_string())
    }

    /// The live connection to `peer`, for diagnostics.
    pub(crate) fn peer_connection(&self, peer: i32) -> Option<Connection> {
        self.connections.lock().ok()?.get(&peer).cloned()
    }

    /// The path currently carrying traffic to `peer`, if there is one.
    ///
    /// Reports whether the connection is direct or relayed, which is the
    /// question worth asking when hole punching may or may not have worked.
    pub(crate) fn peer_path(&self, peer: i32) -> Option<PathInfo> {
        let connections = self.connections.lock().ok()?;
        raw::path_info(connections.get(&peer)?)
    }

    /// Stops or resumes accepting inbound connections. Peers already connected
    /// are unaffected, which is what Godot's flag means.
    pub(crate) fn set_refuse_new_connections(&self, refuse: bool) {
        self.refuse.store(refuse, Ordering::Relaxed);
    }

    /// Takes the next event, if one is waiting. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        match self.events.try_recv() {
            Ok(event) => Some(event),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => None,
        }
    }

    pub(crate) fn send(&self, target: i32, channel: i32, mode: Mode, data: Bytes) {
        let _ = self.commands.send(Command::Send {
            target,
            channel,
            mode,
            data,
        });
    }

    pub(crate) fn disconnect(&self, peer: i32) {
        let _ = self.commands.send(Command::Disconnect(peer));
    }

    pub(crate) fn close(&self) {
        let _ = self.commands.send(Command::Close);
    }
}

/// Everything the session task owns. Only this task touches it, so the maps
/// need no locking.
struct State {
    events: UnboundedSender<Event>,
    internal: UnboundedSender<Internal>,
    is_host: bool,
    links: HashMap<i32, Link>,
    /// Peers the main thread has been told about. A departure is only worth
    /// reporting for one that was actually announced — an abrupt close can beat
    /// the handshake, leaving a peer that never existed as far as Godot knows.
    announced: HashSet<i32>,
    connections: Connections,
}

/// One connected peer.
struct Link {
    connection: Connection,
    /// Lazily opened writer task per reliable channel.
    reliable: HashMap<i32, UnboundedSender<Bytes>>,
    /// Last sequence number sent per channel, for ordered datagrams.
    sequences: HashMap<i32, u32>,
    /// Channels already warned about oversized datagrams, so the warning fires
    /// once rather than once per packet.
    warned: HashSet<i32>,
}

async fn run(
    dispatcher: Dispatcher,
    host: Option<EndpointAddr>,
    registration: Option<Registration>,
    events: UnboundedSender<Event>,
    mut commands: UnboundedReceiver<Command>,
    connections: Connections,
) {
    let (internal_tx, mut internal) = mpsc::unbounded_channel();

    let mut state = State {
        events: events.clone(),
        internal: internal_tx,
        is_host: host.is_none(),
        links: HashMap::new(),
        announced: HashSet::new(),
        connections,
    };

    if let Some(addr) = host {
        match dispatcher.endpoint().connect(addr, ALPN).await {
            Ok(connection) => state.add_link(HOST_ID, connection),
            Err(err) => {
                let _ = events.send(Event::Closed(err.to_string()));
                return;
            }
        }
    } else {
        let _ = events.send(Event::Ready(HOST_ID));
    }

    // Held here rather than in a task of its own, so the ALPN claim is released
    // the moment this session ends.
    let mut inbound = registration;
    let mut next_id = FIRST_CLIENT_ID;

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Send { target, channel, mode, data }) => {
                    state.send(target, channel, mode, data);
                }
                Some(Command::Disconnect(peer)) => state.drop_link(peer),
                Some(Command::Close) | None => break,
            },
            message = internal.recv() => match message {
                Some(Internal::Control(control)) => state.apply(control),
                Some(Internal::Lost(peer)) => state.lost(peer),
                None => break,
            },
            connection = accept_next(&mut inbound) => match connection {
                Some(connection) => {
                    let peer = next_id;
                    next_id = next_id.wrapping_add(1).max(FIRST_CLIENT_ID);
                    state.welcome(peer, connection);
                }
                // The dispatcher holds our sender until this registration drops,
                // so this only fires if that ever stops being true.
                None => inbound = None,
            },
        }
    }

    for (_, link) in state.links.drain() {
        link.connection.close(VarInt::from_u32(0), b"closing");
    }
    // The endpoint is shared with every other protocol, so it is not ours to
    // close. Releasing the ALPN before the last event, rather than on the way
    // out, lets whoever is watching for `Closed` host again straight away.
    drop(inbound);
    let _ = events.send(Event::Closed("session closed".into()));
}

/// Yields the next inbound connection the dispatcher routed to us.
///
/// A client registers nothing, and parking forever leaves the other arms of the
/// select to do the work.
async fn accept_next(registration: &mut Option<Registration>) -> Option<Connection> {
    match registration {
        Some(registration) => registration.accept().await,
        None => std::future::pending().await,
    }
}

impl State {
    /// Registers a connection and starts its reader tasks.
    fn add_link(&mut self, peer: i32, connection: Connection) {
        if let Ok(mut shared) = self.connections.lock() {
            shared.insert(peer, connection.clone());
        }

        spawn_readers(
            peer,
            connection.clone(),
            self.events.clone(),
            self.internal.clone(),
        );

        self.links.insert(
            peer,
            Link {
                connection,
                reliable: HashMap::new(),
                sequences: HashMap::new(),
                warned: HashSet::new(),
            },
        );
    }

    /// Host side: greet a freshly accepted peer and tell everyone else.
    fn welcome(&mut self, peer: i32, connection: Connection) {
        let existing: Vec<i32> = self.links.keys().copied().collect();
        self.add_link(peer, connection);

        self.control_to(
            peer,
            Control::Welcome {
                your_id: peer,
                peers: existing.clone(),
            },
        );

        for other in existing {
            self.control_to(other, Control::PeerJoined(peer));
        }

        self.announce(peer);
    }

    /// Client side: act on a membership message from the host.
    fn apply(&mut self, control: Control) {
        match control {
            Control::Welcome { your_id, peers } => {
                let _ = self.events.send(Event::Ready(your_id));
                self.announce(HOST_ID);
                for peer in peers {
                    self.announce(peer);
                }
            }
            // Clients reach other clients through the host, so membership is
            // reported without a direct connection behind it.
            Control::PeerJoined(id) => self.announce(id),
            Control::PeerLeft(id) => self.forget(id),
        }
    }

    fn lost(&mut self, peer: i32) {
        if self.links.remove(&peer).is_none() {
            return;
        }

        if let Ok(mut shared) = self.connections.lock() {
            shared.remove(&peer);
        }

        self.forget(peer);

        if self.is_host {
            let others: Vec<i32> = self.links.keys().copied().collect();
            for other in others {
                self.control_to(other, Control::PeerLeft(peer));
            }
        } else if peer == HOST_ID {
            let _ = self.events.send(Event::Closed("host disconnected".into()));
        }
    }

    /// Closes a peer's connection. Removal, the `PeerDisconnected` event and
    /// telling the other clients all happen in [`State::lost`], which the
    /// connection's close watcher triggers — so a deliberate disconnect and a
    /// dropped link follow exactly the same path.
    fn drop_link(&mut self, peer: i32) {
        if let Some(link) = self.links.get(&peer) {
            link.connection.close(VarInt::from_u32(0), b"disconnected");
        }
    }

    /// Reports a peer as connected, once.
    fn announce(&mut self, peer: i32) {
        if self.announced.insert(peer) {
            let _ = self.events.send(Event::PeerConnected(peer));
        }
    }

    /// Reports a peer as gone, but only if it was ever announced.
    fn forget(&mut self, peer: i32) {
        if self.announced.remove(&peer) {
            let _ = self.events.send(Event::PeerDisconnected(peer));
        }
    }

    fn control_to(&mut self, peer: i32, control: Control) {
        self.write_reliable(peer, CONTROL_CHANNEL, control.encode());
    }

    /// Routes a packet, following Godot's target convention: `0` is everyone,
    /// a negative id is everyone except that peer, positive is one peer.
    fn send(&mut self, target: i32, channel: i32, mode: Mode, data: Bytes) {
        if target <= 0 {
            let excluded = -target;
            let peers: Vec<i32> = self
                .links
                .keys()
                .copied()
                .filter(|peer| *peer != excluded)
                .collect();
            for peer in peers {
                self.send_one(peer, channel, mode, data.clone());
            }
            return;
        }

        // Anything not directly connected goes via the host, which relays.
        let peer = if self.links.contains_key(&target) {
            target
        } else {
            HOST_ID
        };

        self.send_one(peer, channel, mode, data);
    }

    fn send_one(&mut self, peer: i32, channel: i32, mode: Mode, data: Bytes) {
        match mode {
            Mode::Reliable => self.write_reliable(peer, channel, data),
            Mode::Unreliable | Mode::UnreliableOrdered => {
                self.write_datagram(peer, channel, mode, data)
            }
        }
    }

    /// Appends to the peer's stream for this channel, opening one on first use.
    fn write_reliable(&mut self, peer: i32, channel: i32, data: Bytes) {
        let Some(link) = self.links.get_mut(&peer) else {
            return;
        };

        let sender = link.reliable.entry(channel).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            spawn_stream_writer(link.connection.clone(), channel, rx);
            tx
        });

        let _ = sender.send(data);
    }

    fn write_datagram(&mut self, peer: i32, channel: i32, mode: Mode, data: Bytes) {
        let Some(link) = self.links.get_mut(&peer) else {
            return;
        };

        let sequence = if mode == Mode::UnreliableOrdered {
            let counter = link.sequences.entry(channel).or_insert(0);
            // Zero is reserved for "unordered", so skip it on wrap.
            *counter = counter.wrapping_add(1).max(1);
            *counter
        } else {
            0
        };

        let mut framed = BytesMut::with_capacity(DATAGRAM_HEADER + data.len());
        framed.put_i32(channel);
        framed.put_u32(sequence);
        framed.put_slice(&data);
        let framed = framed.freeze();

        // Oversized payloads go out reliably rather than being dropped on the
        // floor. Delivery is kept; ordering guarantees only get stronger.
        let limit = link.connection.max_datagram_size().unwrap_or(0);
        if framed.len() > limit {
            if link.warned.insert(channel) {
                let warning = format!(
                    "packet of {} bytes exceeds the {} byte datagram limit on channel {}; \
                     sending it reliably instead",
                    data.len(),
                    limit.saturating_sub(DATAGRAM_HEADER),
                    channel,
                );
                let _ = self.events.send(Event::Warning(warning));
            }
            self.write_reliable(peer, channel, data);
            return;
        }

        let _ = link.connection.send_datagram(framed);
    }
}

/// Drains a channel into one unidirectional stream.
fn spawn_stream_writer(connection: Connection, channel: i32, mut frames: UnboundedReceiver<Bytes>) {
    detach(async move {
        let Ok(mut stream) = connection.open_uni().await else {
            return;
        };

        if stream.write_all(&channel.to_be_bytes()).await.is_err() {
            return;
        }

        while let Some(frame) = frames.recv().await {
            let length = frame.len() as u32;
            if stream.write_all(&length.to_be_bytes()).await.is_err() {
                break;
            }
            if stream.write_all(&frame).await.is_err() {
                break;
            }
        }
    });
}

/// Starts the datagram reader, the stream acceptor and the close watcher.
fn spawn_readers(
    peer: i32,
    connection: Connection,
    events: UnboundedSender<Event>,
    internal: UnboundedSender<Internal>,
) {
    // Datagrams.
    let datagrams = connection.clone();
    let datagram_events = events.clone();
    detach(async move {
        let mut last: HashMap<i32, u32> = HashMap::new();

        while let Ok(mut datagram) = datagrams.read_datagram().await {
            if datagram.len() < DATAGRAM_HEADER {
                continue;
            }

            let channel = datagram.get_i32();
            let sequence = datagram.get_u32();

            let mode = if sequence == 0 {
                Mode::Unreliable
            } else {
                let seen = last.entry(channel).or_insert(0);
                // Ignore anything older than what we already delivered, while
                // tolerating the counter wrapping around.
                if sequence < *seen && seen.wrapping_sub(sequence) < u32::MAX / 2 {
                    continue;
                }
                *seen = sequence;
                Mode::UnreliableOrdered
            };

            let packet = Packet {
                peer,
                channel,
                mode,
                data: datagram,
            };

            if datagram_events.send(Event::Packet(packet)).is_err() {
                break;
            }
        }
    });

    // Incoming streams, one task each.
    let streams = connection.clone();
    let stream_internal = internal.clone();
    detach(async move {
        while let Ok(stream) = streams.accept_uni().await {
            detach(read_stream(
                peer,
                stream,
                events.clone(),
                stream_internal.clone(),
            ));
        }
    });

    // Closure.
    detach(async move {
        connection.closed().await;
        let _ = internal.send(Internal::Lost(peer));
    });
}

/// Reads one stream to exhaustion, routing frames by its channel header.
async fn read_stream(
    peer: i32,
    mut stream: RecvStream,
    events: UnboundedSender<Event>,
    internal: UnboundedSender<Internal>,
) {
    let mut header = [0u8; 4];
    if stream.read_exact(&mut header).await.is_err() {
        return;
    }
    let channel = i32::from_be_bytes(header);

    loop {
        let mut length = [0u8; 4];
        if stream.read_exact(&mut length).await.is_err() {
            return;
        }

        let mut frame = vec![0u8; u32::from_be_bytes(length) as usize];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }

        let frame = Bytes::from(frame);
        if channel == CONTROL_CHANNEL {
            if let Some(control) = Control::decode(frame)
                && internal.send(Internal::Control(control)).is_err()
            {
                return;
            }
        } else {
            let packet = Packet {
                peer,
                channel,
                mode: Mode::Reliable,
                data: frame,
            };
            if events.send(Event::Packet(packet)).is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{dialable, endpoint};

    /// A dispatcher over a fresh test endpoint.
    async fn dispatcher() -> Dispatcher {
        Dispatcher::start(endpoint().await)
    }

    /// Drains events for up to `ticks` * 25ms, returning the first match.
    async fn poll_for<T>(
        session: &mut Session,
        mut pick: impl FnMut(&Event) -> Option<T>,
        ticks: u32,
    ) -> Option<T> {
        for _ in 0..ticks {
            while let Some(event) = session.try_recv() {
                if let Some(found) = pick(&event) {
                    return Some(found);
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        None
    }

    /// Drains events until `pick` returns something, or fails the test.
    async fn wait_for<T>(session: &mut Session, pick: impl FnMut(&Event) -> Option<T>) -> T {
        poll_for(session, pick, 400)
            .await
            .expect("timed out waiting for an event")
    }

    /// Long enough for a local connection to have completed if it were going to.
    const SETTLE: u32 = 60;

    fn peer_connected(id: i32) -> impl FnMut(&Event) -> Option<()> {
        move |event| matches!(event, Event::PeerConnected(p) if *p == id).then_some(())
    }

    fn packet(event: &Event) -> Option<(i32, i32, Mode, Bytes)> {
        match event {
            Event::Packet(p) => Some((p.peer, p.channel, p.mode, p.data.clone())),
            _ => None,
        }
    }

    /// Brings up a host and one client that have completed the handshake.
    async fn connected_pair() -> (Session, Session) {
        let host_side = dispatcher().await;
        let mut host = Session::host(&host_side);
        let addr = dialable(host_side.endpoint()).await;
        let mut client = Session::join(&dispatcher().await, addr);

        wait_for(&mut client, peer_connected(HOST_ID)).await;
        wait_for(&mut host, peer_connected(FIRST_CLIENT_ID)).await;
        (host, client)
    }

    #[tokio::test]
    async fn host_is_peer_one_immediately() {
        let mut host = Session::host(&dispatcher().await);
        let id = wait_for(&mut host, |e| match e {
            Event::Ready(id) => Some(*id),
            _ => None,
        })
        .await;
        assert_eq!(id, HOST_ID);
    }

    #[tokio::test]
    async fn handshake_assigns_the_client_an_id() {
        let host_side = dispatcher().await;
        let _host = Session::host(&host_side);
        let addr = dialable(host_side.endpoint()).await;
        let mut client = Session::join(&dispatcher().await, addr);

        let id = wait_for(&mut client, |e| match e {
            Event::Ready(id) => Some(*id),
            _ => None,
        })
        .await;
        assert_eq!(id, FIRST_CLIENT_ID);
    }

    #[tokio::test]
    async fn reliable_packets_keep_their_channel() {
        let (host, mut client) = connected_pair().await;

        host.send(
            FIRST_CLIENT_ID,
            0,
            Mode::Reliable,
            Bytes::from_static(b"zero"),
        );
        host.send(
            FIRST_CLIENT_ID,
            7,
            Mode::Reliable,
            Bytes::from_static(b"seven"),
        );

        let mut seen = Vec::new();
        while seen.len() < 2 {
            let received = wait_for(&mut client, packet).await;
            seen.push(received);
        }
        seen.sort_by_key(|(_, channel, _, _)| *channel);

        assert_eq!(
            seen[0],
            (HOST_ID, 0, Mode::Reliable, Bytes::from_static(b"zero"))
        );
        assert_eq!(
            seen[1],
            (HOST_ID, 7, Mode::Reliable, Bytes::from_static(b"seven"))
        );
    }

    #[tokio::test]
    async fn unreliable_packets_arrive_as_datagrams() {
        let (host, mut client) = connected_pair().await;

        host.send(
            FIRST_CLIENT_ID,
            2,
            Mode::Unreliable,
            Bytes::from_static(b"loose"),
        );

        let received = wait_for(&mut client, packet).await;
        assert_eq!(
            received,
            (HOST_ID, 2, Mode::Unreliable, Bytes::from_static(b"loose"))
        );
    }

    #[tokio::test]
    async fn ordered_datagrams_report_their_mode() {
        let (host, mut client) = connected_pair().await;

        host.send(
            FIRST_CLIENT_ID,
            1,
            Mode::UnreliableOrdered,
            Bytes::from_static(b"first"),
        );

        let received = wait_for(&mut client, packet).await;
        assert_eq!(received.2, Mode::UnreliableOrdered);
        assert_eq!(received.3, Bytes::from_static(b"first"));
    }

    /// A payload too large for a datagram must still arrive, and the sender
    /// must be told its unreliable send changed shape.
    #[tokio::test]
    async fn oversized_datagrams_fall_back_to_the_reliable_path() {
        let (mut host, mut client) = connected_pair().await;

        let big = Bytes::from(vec![7u8; 16 * 1024]);
        host.send(FIRST_CLIENT_ID, 4, Mode::Unreliable, big.clone());

        let warning = wait_for(&mut host, |e| match e {
            Event::Warning(text) => Some(text.clone()),
            _ => None,
        })
        .await;
        assert!(
            warning.contains("channel 4"),
            "unexpected warning: {warning}"
        );

        let received = wait_for(&mut client, packet).await;
        assert_eq!(received.1, 4);
        assert_eq!(received.2, Mode::Reliable);
        assert_eq!(received.3, big);
    }

    #[tokio::test]
    async fn refusing_new_connections_turns_a_client_away() {
        let host_side = dispatcher().await;
        let mut host = Session::host(&host_side);
        host.set_refuse_new_connections(true);
        let addr = dialable(host_side.endpoint()).await;

        let mut client = Session::join(&dispatcher().await, addr);

        let admitted = poll_for(
            &mut host,
            |e| matches!(e, Event::PeerConnected(_)).then_some(()),
            SETTLE,
        )
        .await;
        assert!(admitted.is_none(), "a refused peer was still admitted");

        let welcomed = poll_for(
            &mut client,
            |e| matches!(e, Event::Ready(_)).then_some(()),
            4,
        )
        .await;
        assert!(welcomed.is_none(), "a refused client was still given an id");
    }

    #[tokio::test]
    async fn clearing_the_refusal_lets_clients_back_in() {
        let host_side = dispatcher().await;
        let mut host = Session::host(&host_side);

        host.set_refuse_new_connections(true);
        host.set_refuse_new_connections(false);

        let addr = dialable(host_side.endpoint()).await;
        let _client = Session::join(&dispatcher().await, addr);
        wait_for(&mut host, peer_connected(FIRST_CLIENT_ID)).await;
    }

    #[tokio::test]
    async fn disconnecting_a_client_drops_it_on_both_sides() {
        let (mut host, mut client) = connected_pair().await;

        host.disconnect(FIRST_CLIENT_ID);

        wait_for(&mut host, |e| {
            matches!(e, Event::PeerDisconnected(p) if *p == FIRST_CLIENT_ID).then_some(())
        })
        .await;

        wait_for(&mut client, |e| match e {
            Event::Closed(reason) => Some(reason.clone()),
            _ => None,
        })
        .await;
    }

    /// The host relays membership, so a departure has to reach the peers that
    /// were never directly connected to the one that left.
    #[tokio::test]
    async fn other_clients_hear_about_a_departure() {
        let host_side = dispatcher().await;
        let mut host = Session::host(&host_side);
        let addr = dialable(host_side.endpoint()).await;

        let mut first = Session::join(&dispatcher().await, addr.clone());
        wait_for(&mut host, peer_connected(2)).await;

        let mut second = Session::join(&dispatcher().await, addr);
        wait_for(&mut host, peer_connected(3)).await;

        // Each client learns about the other through the host.
        wait_for(&mut second, peer_connected(2)).await;
        wait_for(&mut first, peer_connected(3)).await;

        host.disconnect(2);

        wait_for(&mut host, |e| {
            matches!(e, Event::PeerDisconnected(2)).then_some(())
        })
        .await;
        wait_for(&mut second, |e| {
            matches!(e, Event::PeerDisconnected(2)).then_some(())
        })
        .await;
    }

    #[tokio::test]
    async fn losing_a_client_is_reported_to_the_host() {
        let (mut host, client) = connected_pair().await;
        client.close();

        wait_for(&mut host, |e| {
            matches!(e, Event::PeerDisconnected(p) if *p == FIRST_CLIENT_ID).then_some(())
        })
        .await;
    }

    /// The endpoint outlives any one session, so ending a session has to leave
    /// it — and the ALPN — usable by the next.
    #[tokio::test]
    async fn a_closed_session_leaves_the_endpoint_hostable() {
        let host_side = dispatcher().await;

        let mut first = Session::host(&host_side);
        first.close();
        wait_for(&mut first, |e| match e {
            Event::Closed(_) => Some(()),
            _ => None,
        })
        .await;

        let mut second = Session::host(&host_side);
        let addr = dialable(host_side.endpoint()).await;
        let mut client = Session::join(&dispatcher().await, addr);

        wait_for(&mut client, peer_connected(HOST_ID)).await;
        wait_for(&mut second, peer_connected(FIRST_CLIENT_ID)).await;
    }

    #[tokio::test]
    async fn one_session_hosts_per_endpoint() {
        let host_side = dispatcher().await;
        let _first = Session::host(&host_side);

        let mut second = Session::host(&host_side);
        let refused = wait_for(&mut second, |e| match e {
            Event::Closed(reason) => Some(reason.clone()),
            _ => None,
        })
        .await;
        assert!(refused.contains("already hosting"), "got: {refused}");
    }
}
