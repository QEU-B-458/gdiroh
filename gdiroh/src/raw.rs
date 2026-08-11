//! Raw connections, for protocols a game defines itself.
//!
//! Where [`crate::session`] builds Godot's multiplayer on top of iroh, this
//! offers a connection much as QUIC has it: bidirectional byte streams and
//! datagrams, with no peer ids, membership or framing imposed on top. A game
//! claims its own ALPN and decides what travels over it.
//!
//! Streams here are **bidirectional only**. `IrohStream` is a Godot
//! `StreamPeer`, which is read *and* write by definition, so exposing QUIC's
//! unidirectional streams through it would leave half of every stream raising
//! errors. Datagrams cover the fire-and-forget case.
//!
//! Godot-free, like [`crate::session`] and [`crate::dispatch`], so it can be
//! driven from a test.

use std::time::Duration;

use bytes::Bytes;
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::dispatch::Dispatcher;
use crate::runtime::detach;

/// Largest slice taken from a stream in one read. Chunks arrive as the network
/// delivers them, so this only caps how much one read can yield.
const READ_CHUNK: usize = 64 * 1024;

/// What the selected network path to a peer looks like right now.
pub(crate) struct PathInfo {
    /// True when traffic is going through a relay rather than peer-to-peer.
    pub relay: bool,
    pub rtt: Duration,
}

/// The path currently carrying traffic on `connection`, if there is one.
pub(crate) fn path_info(connection: &Connection) -> Option<PathInfo> {
    let paths = connection.paths();
    let selected = paths.iter().find(|path| path.is_selected())?;

    Some(PathInfo {
        relay: selected.is_relay(),
        rtt: selected.rtt(),
    })
}

/// Something that happened on a raw connection.
pub(crate) enum Event {
    /// The handshake finished. Only ever arrives for a dialled connection, and
    /// only once.
    Opened(Connection),
    /// The remote opened a stream to us.
    Stream(Stream),
    Datagram(Bytes),
    /// The connection ended, cleanly or otherwise. Nothing follows it.
    Closed(String),
}

/// The main thread's view of a raw connection: a queue of things that happened.
///
/// The [`Connection`] itself is handed over in [`Event::Opened`] and used
/// directly from there, since its methods are all non-blocking.
pub(crate) struct Events(UnboundedReceiver<Event>);

impl Events {
    /// Takes the next event, if one is waiting. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        self.0.try_recv().ok()
    }

    /// Waits for the next event. `None` means every sender is gone — the
    /// dial failed before starting the pump, or the runtime is not running.
    ///
    /// Same contract as [`Stream::recv_blocking`]: main thread only, and safe
    /// there because the pump tasks keep feeding this channel from the
    /// runtime while this thread waits.
    pub(crate) fn recv_blocking(&mut self) -> Option<Event> {
        self.0.blocking_recv()
    }
}

/// Dials `peer` and speaks `alpn` to it.
///
/// Returns immediately; the connection arrives as [`Event::Opened`], or the
/// attempt fails with [`Event::Closed`].
pub(crate) fn dial(dispatcher: &Dispatcher, peer: EndpointAddr, alpn: Vec<u8>) -> Events {
    let (events, queue) = mpsc::unbounded_channel();
    let endpoint = dispatcher.endpoint().clone();

    let failed = events.clone();
    if !detach(async move {
        match endpoint.connect(peer, &alpn).await {
            Ok(connection) => {
                if events.send(Event::Opened(connection.clone())).is_ok() {
                    pump(connection, events);
                }
            }
            Err(err) => {
                let _ = events.send(Event::Closed(err.to_string()));
            }
        }
    }) {
        let _ = failed.send(Event::Closed("the network runtime is not running".into()));
    }

    Events(queue)
}

/// Takes over a connection the dispatcher already accepted.
pub(crate) fn adopt(connection: Connection) -> Events {
    let (events, queue) = mpsc::unbounded_channel();
    pump(connection, events);
    Events(queue)
}

/// Starts the stream acceptor, the datagram reader and the close watcher.
fn pump(connection: Connection, events: UnboundedSender<Event>) {
    let streams = connection.clone();
    let stream_events = events.clone();
    detach(async move {
        while let Ok((send, recv)) = streams.accept_bi().await {
            let stream = Stream::adopt(send, recv);
            if stream_events.send(Event::Stream(stream)).is_err() {
                break;
            }
        }
    });

    let datagrams = connection.clone();
    let datagram_events = events.clone();
    detach(async move {
        while let Ok(datagram) = datagrams.read_datagram().await {
            if datagram_events.send(Event::Datagram(datagram)).is_err() {
                break;
            }
        }
    });

    detach(async move {
        let reason = connection.closed().await.to_string();
        let _ = events.send(Event::Closed(reason));
    });
}

/// Queued for the writer task.
enum Chunk {
    Data(Bytes),
    /// Close the write half. The read half stays open.
    Finish,
    /// Give up on the stream in both directions, telling the far side so.
    Abort(u32),
}

/// Read off a stream, in the order it arrived.
pub(crate) enum Incoming {
    Data(Bytes),
    /// The remote finished writing (`None`) or the stream failed (`Some`).
    Ended(Option<String>),
}

/// One bidirectional stream.
pub(crate) struct Stream {
    outgoing: UnboundedSender<Chunk>,
    incoming: UnboundedReceiver<Incoming>,
}

impl Stream {
    /// Opens a stream on `connection`.
    ///
    /// Writes queue until the stream actually exists, so this is usable straight
    /// away rather than after a round trip.
    pub(crate) fn open(connection: Connection) -> Self {
        let (outgoing, to_write) = mpsc::unbounded_channel();
        let (reports, incoming) = mpsc::unbounded_channel();

        let failed = reports.clone();
        if !detach(async move {
            match connection.open_bi().await {
                Ok((send, recv)) => carry(send, recv, to_write, reports).await,
                Err(err) => {
                    let _ = reports.send(Incoming::Ended(Some(err.to_string())));
                }
            }
        }) {
            let _ = failed.send(Incoming::Ended(Some(
                "the network runtime is not running".into(),
            )));
        }

        Self { outgoing, incoming }
    }

    /// Wraps a stream the remote opened.
    fn adopt(send: SendStream, recv: RecvStream) -> Self {
        let (outgoing, to_write) = mpsc::unbounded_channel();
        let (reports, incoming) = mpsc::unbounded_channel();

        detach(carry(send, recv, to_write, reports));

        Self { outgoing, incoming }
    }

    /// Queues bytes. Returns `false` once the stream is gone.
    ///
    /// The queue is unbounded: a Godot method cannot block waiting for the
    /// network, so a writer that outruns the link buffers in memory.
    pub(crate) fn write(&self, data: Bytes) -> bool {
        self.outgoing.send(Chunk::Data(data)).is_ok()
    }

    /// Closes the write half once everything queued has gone out. The remote
    /// sees a clean end of stream and can still send to us.
    pub(crate) fn finish(&self) -> bool {
        self.outgoing.send(Chunk::Finish).is_ok()
    }

    /// Gives up on the stream in both directions.
    ///
    /// Anything still queued is abandoned, and the far side is told with
    /// `code` rather than being left to notice — which is the difference
    /// between this and simply dropping the stream.
    pub(crate) fn abort(&self, code: u32) -> bool {
        self.outgoing.send(Chunk::Abort(code)).is_ok()
    }

    /// Takes the next thing read, if any. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Incoming> {
        self.incoming.try_recv().ok()
    }

    /// Waits for the next thing read. `None` means the carry task is gone and
    /// nothing more is coming.
    ///
    /// For the main thread only — it must never be called from a runtime
    /// thread, and the wait is safe from the main thread precisely because
    /// the reader keeps delivering from the runtime while this thread waits.
    pub(crate) fn recv_blocking(&mut self) -> Option<Incoming> {
        self.incoming.blocking_recv()
    }
}

/// Drives both halves of an open stream until either runs out.
async fn carry(
    mut send: SendStream,
    mut recv: RecvStream,
    mut to_write: UnboundedReceiver<Chunk>,
    reports: UnboundedSender<Incoming>,
) {
    // Both halves are driven here rather than in separate tasks, because an
    // abort has to reach into the read half — which means owning it.
    let (aborted, mut abort) = mpsc::unbounded_channel::<u32>();

    detach(async move {
        // Once the writer is done its sender drops, and nothing can abort any
        // more — but the read half must carry on, so that arm is switched off
        // rather than treated as an abort.
        let mut abortable = true;

        loop {
            tokio::select! {
                code = abort.recv(), if abortable => {
                    match code {
                        Some(code) => {
                            let _ = recv.stop(VarInt::from_u32(code));
                            return;
                        }
                        None => abortable = false,
                    }
                }
                chunk = recv.read_chunk(READ_CHUNK) => {
                    let ended = match chunk {
                        Ok(Some(chunk)) => {
                            if reports.send(Incoming::Data(chunk)).is_err() {
                                return;
                            }
                            continue;
                        }
                        Ok(None) => None,
                        Err(err) => Some(err.to_string()),
                    };

                    let _ = reports.send(Incoming::Ended(ended));
                    return;
                }
            }
        }
    });

    while let Some(chunk) = to_write.recv().await {
        match chunk {
            Chunk::Data(data) => {
                if send.write_all(&data).await.is_err() {
                    return;
                }
            }
            Chunk::Finish => break,
            Chunk::Abort(code) => {
                let _ = send.reset(VarInt::from_u32(code));
                let _ = aborted.send(code);
                return;
            }
        }
    }

    // Also covers the stream being dropped, so a remote reading to the end
    // always sees a clean finish rather than a hang.
    let _ = send.finish();
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{dialable, endpoint};

    const ALPN: &[u8] = b"gdiroh/raw-test";

    /// Long enough for a local exchange to have finished if it were going to.
    const SETTLE: Duration = Duration::from_secs(5);

    /// Waits for an event `pick` accepts, failing the test if none arrives.
    async fn wait_for<T>(events: &mut Events, mut pick: impl FnMut(Event) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + SETTLE;
        while tokio::time::Instant::now() < deadline {
            while let Some(event) = events.try_recv() {
                if let Some(found) = pick(event) {
                    return found;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for an event");
    }

    /// Reads from a stream until `Ended`, returning everything that arrived.
    async fn drain(stream: &mut Stream) -> (Vec<u8>, Option<String>) {
        let deadline = tokio::time::Instant::now() + SETTLE;
        let mut data = Vec::new();

        while tokio::time::Instant::now() < deadline {
            match stream.try_recv() {
                Some(Incoming::Data(chunk)) => data.extend_from_slice(&chunk),
                Some(Incoming::Ended(reason)) => return (data, reason),
                None => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        panic!("stream never ended");
    }

    /// A dialled connection and the accepted other side of it.
    async fn connected_pair() -> (Connection, Events, Connection, Events) {
        let listener = Dispatcher::start(endpoint().await);
        let mut claim = listener.register(ALPN).expect("alpn should be free");
        let addr = dialable(listener.endpoint()).await;

        let caller = Dispatcher::start(endpoint().await);
        let mut dialled = dial(&caller, addr, ALPN.to_vec());

        let accepted = claim.accept().await.expect("a connection should arrive");
        let opened = wait_for(&mut dialled, |event| match event {
            Event::Opened(connection) => Some(connection),
            _ => None,
        })
        .await;

        let inbound = adopt(accepted.clone());
        (opened, dialled, accepted, inbound)
    }

    #[tokio::test]
    async fn a_stream_carries_bytes_to_the_other_side() {
        let (outbound, _caller, _accepted, mut inbound) = connected_pair().await;

        let stream = Stream::open(outbound);
        assert!(stream.write(Bytes::from_static(b"hello over quic")));
        assert!(stream.finish());

        let mut received = wait_for(&mut inbound, |event| match event {
            Event::Stream(stream) => Some(stream),
            _ => None,
        })
        .await;

        let (data, ended) = drain(&mut received).await;
        assert_eq!(data, b"hello over quic");
        assert_eq!(ended, None, "a finished stream should end cleanly");
    }

    #[tokio::test]
    async fn a_stream_carries_bytes_both_ways() {
        let (outbound, _caller, _accepted, mut inbound) = connected_pair().await;

        let mut asking = Stream::open(outbound);
        assert!(asking.write(Bytes::from_static(b"ping")));

        let mut answering = wait_for(&mut inbound, |event| match event {
            Event::Stream(stream) => Some(stream),
            _ => None,
        })
        .await;

        // The reply travels back down the stream the caller opened.
        assert!(answering.write(Bytes::from_static(b"pong")));
        assert!(answering.finish());

        let (reply, _) = drain(&mut asking).await;
        assert_eq!(reply, b"pong");

        assert!(asking.finish());
        let (question, _) = drain(&mut answering).await;
        assert_eq!(question, b"ping");
    }

    /// Aborting tells the far side rather than leaving it to notice, which is
    /// the whole difference from dropping the stream.
    #[tokio::test]
    async fn aborting_a_stream_ends_it_on_the_other_side() {
        let (outbound, _caller, _accepted, mut inbound) = connected_pair().await;

        let stream = Stream::open(outbound);
        assert!(stream.write(Bytes::from_static(b"partial")));

        let mut received = wait_for(&mut inbound, |event| match event {
            Event::Stream(stream) => Some(stream),
            _ => None,
        })
        .await;

        assert!(stream.abort(7));

        let (_, ended) = drain(&mut received).await;
        assert!(
            ended.is_some(),
            "an aborted stream should end with a reason, not cleanly"
        );
    }

    /// `IrohStream.get_data` waits for bytes still on their way, so Godot's
    /// length-then-payload helpers (`get_string`, `get_var`) cannot mis-frame
    /// the stream. This is the primitive underneath that wait: a blocking
    /// receive on the main thread, fed by the reader on the runtime.
    #[test]
    fn a_blocking_read_gets_bytes_that_arrive_late() {
        let runtime = tokio::runtime::Runtime::new().expect("a runtime should build");

        let (outbound, _caller, _accepted, mut inbound) = runtime.block_on(connected_pair());

        // Opened inside the runtime's context, the way it happens in the
        // engine, where the shared runtime is up. The guard is dropped again
        // before the blocking read below, which must run outside one.
        let stream = {
            let _guard = runtime.enter();
            let stream = Stream::open(outbound);
            assert!(stream.write(Bytes::from_static(b"first, ")));
            stream
        };

        let mut received = runtime.block_on(wait_for(&mut inbound, |event| match event {
            Event::Stream(stream) => Some(stream),
            _ => None,
        }));

        // The rest goes out only after the reader below is already waiting.
        runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            stream.write(Bytes::from_static(b"then the rest"));
            stream.finish();
        });

        // This thread holds no runtime, so blocking here is legal — the same
        // situation Godot's main thread is in.
        let mut data = Vec::new();
        loop {
            match received.recv_blocking() {
                Some(Incoming::Data(chunk)) => data.extend_from_slice(&chunk),
                Some(Incoming::Ended(reason)) => {
                    assert_eq!(reason, None, "the stream should end cleanly");
                    break;
                }
                None => panic!("the stream's task disappeared mid-read"),
            }
        }
        assert_eq!(data, b"first, then the rest");
    }

    #[tokio::test]
    async fn datagrams_arrive_on_the_other_side() {
        let (outbound, _caller, _accepted, mut inbound) = connected_pair().await;

        outbound
            .send_datagram(Bytes::from_static(b"unreliable"))
            .expect("datagram should send");

        let received = wait_for(&mut inbound, |event| match event {
            Event::Datagram(data) => Some(data),
            _ => None,
        })
        .await;
        assert_eq!(received, Bytes::from_static(b"unreliable"));
    }

    #[tokio::test]
    async fn closing_is_reported_to_the_other_side() {
        let (outbound, _caller, _accepted, mut inbound) = connected_pair().await;

        outbound.close(0u32.into(), b"done here");

        let reason = wait_for(&mut inbound, |event| match event {
            Event::Closed(reason) => Some(reason),
            _ => None,
        })
        .await;
        assert!(reason.contains("done here"), "got: {reason}");
    }

    #[tokio::test]
    async fn dialling_an_alpn_nobody_claimed_fails() {
        let listener = Dispatcher::start(endpoint().await);
        let _claim = listener.register(ALPN).expect("alpn should be free");
        let addr = dialable(listener.endpoint()).await;

        let caller = Dispatcher::start(endpoint().await);
        let mut dialled = dial(&caller, addr, b"gdiroh/nobody-listens".to_vec());

        wait_for(&mut dialled, |event| match event {
            Event::Closed(reason) => Some(reason),
            _ => None,
        })
        .await;
    }
}
