//! Gossip: broadcasting to a swarm of peers with no server in the middle.
//!
//! Peers subscribe to a topic and every message reaches everyone else on it,
//! relayed peer to peer. Nobody holds the full membership list, so this scales
//! where a mesh of direct connections would not — lobby listings, presence,
//! chat, anything where "tell everyone" is the shape of the problem.
//!
//! Delivery is best effort. A message may not reach a peer that is joining, and
//! a slow subscriber is dropped rather than allowed to stall the swarm. Use a
//! [`crate::raw`] connection when something has to arrive.
//!
//! Godot-free, like the rest of the transport, so it can be driven from a test.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::EndpointId;
use iroh_gossip::api::Event as SwarmEvent;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::dispatch::{Claim, Dispatcher};
use crate::runtime::{self, detach, detach_handle};

/// Turns a topic name into the 32-byte id gossip works in.
///
/// Peers only have to agree on the name, which keeps any string usable and
/// spares script from handling raw bytes. Hashing also means an unlucky name
/// cannot collide with a neighbouring game's.
pub(crate) fn topic_id(name: &str) -> TopicId {
    TopicId::from(*blake3::hash(name.as_bytes()).as_bytes())
}

/// Something that happened on a topic.
pub(crate) enum Event {
    /// A peer became a direct neighbour of ours in this topic's swarm.
    NeighborUp(EndpointId),
    NeighborDown(EndpointId),
    Message {
        content: Bytes,
        /// Who passed it to us, which is not necessarily who wrote it.
        from: EndpointId,
        /// True when it came straight from its author rather than via hops.
        direct: bool,
    },
    /// We fell behind and messages were dropped. The topic is finished.
    Lagged,
    /// The topic ended and will produce nothing further.
    Closed(String),
}

enum Command {
    Broadcast(Bytes),
    BroadcastNeighbors(Bytes),
    Join(Vec<EndpointId>),
}

/// The gossip protocol running on one endpoint.
///
/// One per endpoint, started the first time a topic is subscribed to — a game
/// that never gossips pays nothing for it.
pub(crate) struct Swarm {
    gossip: Gossip,
    /// Held here rather than inside the task below, so dropping this releases
    /// the ALPN straight away instead of whenever the runtime reaps the task.
    _claim: Claim,
    /// Feeds accepted connections in.
    accepting: JoinHandle<()>,
}

impl Swarm {
    pub(crate) fn start(dispatcher: &Dispatcher) -> Option<Self> {
        let (claim, mut inbound) = dispatcher.register(iroh_gossip::ALPN)?.split();

        // `spawn` starts tasks as it builds, so it panics unless a reactor is in
        // scope — and this is normally reached from Godot's main thread, where
        // there is none. A test calls it from inside a runtime and would never
        // notice.
        let gossip = {
            let _reactor = runtime::handle()?.enter();
            Gossip::builder().spawn(dispatcher.endpoint().clone())
        };

        let handler = gossip.clone();
        let accepting = detach_handle(async move {
            while let Some(connection) = inbound.recv().await {
                let gossip = handler.clone();
                // One task each, so a slow peer cannot hold up the next.
                detach(async move {
                    let _ = gossip.handle_connection(connection).await;
                });
            }
        })?;

        Some(Self {
            gossip,
            _claim: claim,
            accepting,
        })
    }

    /// The gossip instance, for anything built on top of it. Documents sync
    /// their live updates over the same swarm rather than a second one.
    pub(crate) fn gossip(&self) -> Gossip {
        self.gossip.clone()
    }

    /// Joins `topic`, dialling `bootstrap` to find the swarm.
    ///
    /// Returns straight away. With an empty `bootstrap` this only hears from
    /// peers that find us, so at least one known peer is normally needed to get
    /// in — their addresses have to be resolvable, which for a closed network
    /// means teaching the endpoint about them first.
    pub(crate) fn subscribe(&self, topic: TopicId, bootstrap: Vec<EndpointId>) -> Topic {
        let (events, queue) = mpsc::unbounded_channel();
        let (commands, orders) = mpsc::unbounded_channel();
        let neighbors: Neighbors = Arc::default();

        let failed = events.clone();
        if !detach(run(
            self.gossip.clone(),
            topic,
            bootstrap,
            events,
            orders,
            neighbors.clone(),
        )) {
            let _ = failed.send(Event::Closed("the network runtime is not running".into()));
        }

        Topic {
            id: topic,
            events: queue,
            commands,
            neighbors,
        }
    }
}

impl Drop for Swarm {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// Current direct neighbours, readable from the main thread without disturbing
/// the task that owns the subscription.
type Neighbors = Arc<Mutex<HashSet<EndpointId>>>;

/// A joined topic. Dropping it leaves the topic.
pub(crate) struct Topic {
    id: TopicId,
    events: UnboundedReceiver<Event>,
    commands: UnboundedSender<Command>,
    neighbors: Neighbors,
}

impl Topic {
    /// Takes the next event, if one is waiting. Never blocks.
    pub(crate) fn try_recv(&mut self) -> Option<Event> {
        self.events.try_recv().ok()
    }

    /// Sends to everyone on the topic.
    pub(crate) fn broadcast(&self, message: Bytes) -> bool {
        self.commands.send(Command::Broadcast(message)).is_ok()
    }

    /// Sends only to our direct neighbours, with no onward relaying.
    pub(crate) fn broadcast_neighbors(&self, message: Bytes) -> bool {
        self.commands
            .send(Command::BroadcastNeighbors(message))
            .is_ok()
    }

    /// Dials more peers to widen or repair our view of the swarm.
    pub(crate) fn join(&self, peers: Vec<EndpointId>) -> bool {
        self.commands.send(Command::Join(peers)).is_ok()
    }

    /// The topic's hashed id, as text.
    pub(crate) fn id(&self) -> String {
        self.id.to_string()
    }

    pub(crate) fn neighbors(&self) -> Vec<EndpointId> {
        match self.neighbors.lock() {
            Ok(neighbors) => neighbors.iter().copied().collect(),
            Err(_) => Vec::new(),
        }
    }
}

async fn run(
    gossip: Gossip,
    topic: TopicId,
    bootstrap: Vec<EndpointId>,
    events: UnboundedSender<Event>,
    mut commands: UnboundedReceiver<Command>,
    neighbors: Neighbors,
) {
    let subscribed = match gossip.subscribe(topic, bootstrap).await {
        Ok(subscribed) => subscribed,
        Err(err) => {
            let _ = events.send(Event::Closed(err.to_string()));
            return;
        }
    };

    let (sender, mut incoming) = subscribed.split();

    loop {
        tokio::select! {
            command = commands.recv() => {
                let outcome = match command {
                    Some(Command::Broadcast(message)) => sender.broadcast(message).await,
                    Some(Command::BroadcastNeighbors(message)) => {
                        sender.broadcast_neighbors(message).await
                    }
                    Some(Command::Join(peers)) => sender.join_peers(peers).await,
                    // The handle was dropped, which means leave the topic.
                    None => break,
                };

                if let Err(err) = outcome {
                    let _ = events.send(Event::Closed(err.to_string()));
                    break;
                }
            }
            event = incoming.next() => match event {
                Some(Ok(event)) => {
                    if !report(event, &events, &neighbors) {
                        break;
                    }
                }
                Some(Err(err)) => {
                    let _ = events.send(Event::Closed(err.to_string()));
                    break;
                }
                None => {
                    let _ = events.send(Event::Closed("topic closed".into()));
                    break;
                }
            },
        }
    }
}

/// Translates one gossip event. Returns `false` when the topic is finished.
fn report(event: SwarmEvent, events: &UnboundedSender<Event>, neighbors: &Neighbors) -> bool {
    let translated = match event {
        SwarmEvent::NeighborUp(peer) => {
            if let Ok(mut neighbors) = neighbors.lock() {
                neighbors.insert(peer);
            }
            Event::NeighborUp(peer)
        }
        SwarmEvent::NeighborDown(peer) => {
            if let Ok(mut neighbors) = neighbors.lock() {
                neighbors.remove(&peer);
            }
            Event::NeighborDown(peer)
        }
        SwarmEvent::Received(message) => Event::Message {
            content: message.content,
            from: message.delivered_from,
            direct: message.scope.is_direct(),
        },
        // Gossip closes the subscription after this, so nothing more is coming.
        SwarmEvent::Lagged => {
            let _ = events.send(Event::Lagged);
            return false;
        }
    };

    events.send(translated).is_ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{endpoint_pair, wait_until};

    const TOPIC: &str = "gdiroh/test-topic";

    /// Drains events for up to five seconds, returning the first match.
    async fn wait_for<T>(topic: &mut Topic, mut pick: impl FnMut(&Event) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            while let Some(event) = topic.try_recv() {
                if let Some(found) = pick(&event) {
                    return found;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for a topic event");
    }

    fn message(event: &Event) -> Option<(Bytes, EndpointId)> {
        match event {
            Event::Message { content, from, .. } => Some((content.clone(), *from)),
            _ => None,
        }
    }

    #[tokio::test]
    async fn a_message_reaches_the_other_subscriber() {
        let (first, second) = endpoint_pair().await;
        let first_id = first.endpoint().id();
        let second_id = second.endpoint().id();

        let first_swarm = Swarm::start(&first).expect("gossip should start");
        let second_swarm = Swarm::start(&second).expect("gossip should start");

        let topic = topic_id(TOPIC);
        let mut here = first_swarm.subscribe(topic, vec![second_id]);
        let mut there = second_swarm.subscribe(topic, vec![first_id]);

        wait_for(&mut here, |event| {
            matches!(event, Event::NeighborUp(_)).then_some(())
        })
        .await;

        assert!(here.broadcast(Bytes::from_static(b"over the swarm")));

        let (content, from) = wait_for(&mut there, message).await;
        assert_eq!(content, Bytes::from_static(b"over the swarm"));
        assert_eq!(from, first_id);
    }

    #[tokio::test]
    async fn neighbours_are_reported_and_listed() {
        let (first, second) = endpoint_pair().await;
        let first_id = first.endpoint().id();
        let second_id = second.endpoint().id();

        let first_swarm = Swarm::start(&first).expect("gossip should start");
        let second_swarm = Swarm::start(&second).expect("gossip should start");

        let topic = topic_id(TOPIC);
        let mut here = first_swarm.subscribe(topic, vec![second_id]);
        let _there = second_swarm.subscribe(topic, vec![first_id]);

        let joined = wait_for(&mut here, |event| match event {
            Event::NeighborUp(peer) => Some(*peer),
            _ => None,
        })
        .await;

        assert_eq!(joined, second_id);
        wait_until(|| here.neighbors() == vec![second_id]).await;
    }

    #[tokio::test]
    async fn separate_topics_do_not_mix() {
        let (first, second) = endpoint_pair().await;
        let first_id = first.endpoint().id();
        let second_id = second.endpoint().id();

        let first_swarm = Swarm::start(&first).expect("gossip should start");
        let second_swarm = Swarm::start(&second).expect("gossip should start");

        let here = first_swarm.subscribe(topic_id("gdiroh/one"), vec![second_id]);
        let mut there = second_swarm.subscribe(topic_id("gdiroh/two"), vec![first_id]);

        assert!(here.broadcast(Bytes::from_static(b"only for topic one")));

        let leaked = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(found) = there.try_recv().as_ref().and_then(message) {
                    return found;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(leaked.is_err(), "a message crossed between topics");
    }

    #[test]
    fn a_topic_name_always_hashes_the_same_way() {
        assert_eq!(topic_id("gdiroh/lobby"), topic_id("gdiroh/lobby"));
        assert_ne!(topic_id("gdiroh/lobby"), topic_id("gdiroh/lobbz"));
    }
}
