//! One accept loop per endpoint, routing inbound connections by ALPN.
//!
//! An endpoint's `accept()` can only be consumed once, so the loop lives here
//! rather than inside whoever wants connections. Godot's multiplayer session is
//! then just one protocol among several: a game can claim an ALPN of its own and
//! get raw connections alongside it.
//!
//! Deliberately free of Godot types, like [`crate::session`], so routing can be
//! driven from a test.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use iroh::Endpoint;
use iroh::endpoint::{Connection, Incoming, VarInt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::runtime::detach;

/// Close code for a connection whose ALPN nobody claimed.
const NO_LISTENER: u32 = 1;

/// Close code for a protocol that is up but turning connections away.
const REFUSED: u32 = 2;

type Protocols = Arc<Mutex<HashMap<Vec<u8>, Protocol>>>;

/// One claimed ALPN.
struct Protocol {
    connections: UnboundedSender<Connection>,
    /// Read straight from the accept loop, so a refusal costs only a load.
    refuse: Arc<AtomicBool>,
}

/// Hands inbound connections to whichever protocol claimed their ALPN.
///
/// Cheap to clone — every clone drives the same endpoint and registry.
#[derive(Clone)]
pub(crate) struct Dispatcher {
    endpoint: Endpoint,
    protocols: Protocols,
}

impl Dispatcher {
    /// Takes over `endpoint`'s accept loop. Nothing is accepted until a protocol
    /// registers, since the endpoint's ALPN list is derived from the registry.
    pub(crate) fn start(endpoint: Endpoint) -> Self {
        let protocols: Protocols = Arc::new(Mutex::new(HashMap::new()));

        detach(accept_loop(endpoint.clone(), protocols.clone()));

        Self {
            endpoint,
            protocols,
        }
    }

    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Claims `alpn`. Returns `None` if something already holds it.
    ///
    /// The claim lasts as long as the returned [`Registration`].
    pub(crate) fn register(&self, alpn: &[u8]) -> Option<Registration> {
        let (connections, incoming) = mpsc::unbounded_channel();
        let refuse = Arc::new(AtomicBool::new(false));

        {
            let mut protocols = lock(&self.protocols);
            if protocols.contains_key(alpn) {
                return None;
            }
            protocols.insert(
                alpn.to_vec(),
                Protocol {
                    connections,
                    refuse: refuse.clone(),
                },
            );
        }

        self.publish_alpns();

        Some(Registration {
            claim: Claim {
                dispatcher: self.clone(),
                alpn: alpn.to_vec(),
            },
            incoming,
            refuse,
        })
    }

    /// Tells the endpoint which ALPNs to negotiate. Safe on a live endpoint, so
    /// registering after binding needs no rebind.
    fn publish_alpns(&self) {
        let alpns = lock(&self.protocols).keys().cloned().collect();
        self.endpoint.set_alpns(alpns);
    }
}

/// Holds one ALPN. Dropping it releases the claim, **synchronously** — which is
/// why it is kept apart from the receiver below.
pub(crate) struct Claim {
    dispatcher: Dispatcher,
    alpn: Vec<u8>,
}

/// Connections routed to a claimed ALPN.
pub(crate) type Inbound = UnboundedReceiver<Connection>;

/// A claim on one ALPN, plus the connections arriving for it.
pub(crate) struct Registration {
    claim: Claim,
    incoming: Inbound,
    refuse: Arc<AtomicBool>,
}

impl Registration {
    /// The next connection negotiated for this ALPN, or `None` once the
    /// dispatcher is gone.
    pub(crate) async fn accept(&mut self) -> Option<Connection> {
        self.incoming.recv().await
    }

    /// Takes the next connection, if one is waiting. Never blocks, so the main
    /// thread can poll this from a frame callback.
    pub(crate) fn try_accept(&mut self) -> Option<Connection> {
        self.incoming.try_recv().ok()
    }

    /// The flag the accept loop reads to decide whether to turn peers away.
    /// Shared, so the owner can flip it from another thread.
    pub(crate) fn refusals(&self) -> Arc<AtomicBool> {
        self.refuse.clone()
    }

    /// Separates the claim from the connections.
    ///
    /// Needed by anything that hands the receiving half to a task it later
    /// *aborts*: `JoinHandle::abort` only asks the runtime to drop the task, so
    /// a claim living inside that task outlives the owner that gave up on it,
    /// and the next attempt to claim the same ALPN is refused. Holding the
    /// `Claim` directly releases it the moment the owner is dropped.
    pub(crate) fn split(self) -> (Claim, Inbound) {
        (self.claim, self.incoming)
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        lock(&self.dispatcher.protocols).remove(&self.alpn);
        self.dispatcher.publish_alpns();
    }
}

async fn accept_loop(endpoint: Endpoint, protocols: Protocols) {
    while let Some(incoming) = endpoint.accept().await {
        // Refuse only on a definite "nobody wants this", so an offer we could
        // not read still gets its chance after the handshake.
        if offers_wanted_alpn(&incoming, &protocols) == Some(false) {
            incoming.refuse();
            continue;
        }

        detach(dispatch(incoming, protocols.clone()));
    }
}

/// Whether the ClientHello offers an ALPN some protocol is ready to take.
///
/// Reading it costs decrypting one packet and is what lets a refused peer be
/// turned away before the handshake rather than connected and then dropped.
/// Best effort: `None` when the offer could not be read, which is what a
/// ClientHello split across several packets looks like.
fn offers_wanted_alpn(incoming: &Incoming, protocols: &Protocols) -> Option<bool> {
    let mut offered = incoming.decrypt()?.alpns()?;
    Some(offered.any(|alpn| alpn.is_ok_and(|alpn| wanted(protocols, &alpn))))
}

/// Whether `alpn` is claimed by a protocol that is currently accepting.
fn wanted(protocols: &Protocols, alpn: &[u8]) -> bool {
    lock(protocols)
        .get(alpn)
        .is_some_and(|protocol| !protocol.refuse.load(Ordering::Relaxed))
}

async fn dispatch(incoming: Incoming, protocols: Protocols) {
    let Ok(mut accepting) = incoming.accept() else {
        // Any host on the network can send us datagrams that merely look like a
        // QUIC handshake, so this is noise rather than a fault.
        return;
    };

    // What the two sides actually settled on, as opposed to what the client
    // offered. This is the authority for routing.
    let Ok(alpn) = accepting.alpn().await else {
        return;
    };
    let Ok(connection) = accepting.await else {
        return;
    };

    // Scoped so the lock is not held across the send, and never across an await.
    let connections = {
        let protocols = lock(&protocols);
        match protocols.get(&alpn) {
            Some(protocol) if protocol.refuse.load(Ordering::Relaxed) => {
                connection.close(VarInt::from_u32(REFUSED), b"not accepting connections");
                return;
            }
            Some(protocol) => protocol.connections.clone(),
            None => {
                connection.close(
                    VarInt::from_u32(NO_LISTENER),
                    b"no listener for that protocol",
                );
                return;
            }
        }
    };

    // Failure means the protocol was released while the handshake ran, which
    // drops the connection here and is the right outcome.
    let _ = connections.send(connection);
}

fn lock(protocols: &Protocols) -> MutexGuard<'_, HashMap<Vec<u8>, Protocol>> {
    // Poisoning only tells us some other caller panicked while holding the
    // lock; the map itself is still sound.
    protocols
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{dialable, endpoint};

    const ALPN: &[u8] = b"gdiroh/test";
    const OTHER: &[u8] = b"gdiroh/other";

    /// Long enough for a local connection to have arrived if it were going to.
    const SETTLE: Duration = Duration::from_secs(2);

    async fn dispatcher() -> Dispatcher {
        Dispatcher::start(endpoint().await)
    }

    #[tokio::test]
    async fn a_connection_reaches_the_protocol_that_claimed_its_alpn() {
        let host = dispatcher().await;
        let mut claimed = host.register(ALPN).expect("alpn should be free");
        let mut other = host.register(OTHER).expect("alpn should be free");
        let addr = dialable(host.endpoint()).await;

        let client = endpoint().await;
        client.connect(addr, ALPN).await.expect("should connect");

        tokio::time::timeout(SETTLE, claimed.accept())
            .await
            .expect("connection should have arrived")
            .expect("dispatcher should still be running");

        assert!(
            tokio::time::timeout(Duration::from_millis(200), other.accept())
                .await
                .is_err(),
            "the other protocol should not have been given the connection"
        );
    }

    #[tokio::test]
    async fn an_unclaimed_alpn_is_turned_away() {
        let host = dispatcher().await;
        let _claimed = host.register(ALPN).expect("alpn should be free");
        let addr = dialable(host.endpoint()).await;

        let client = endpoint().await;
        assert!(
            client.connect(addr, OTHER).await.is_err(),
            "an alpn nobody claimed should not connect"
        );
    }

    #[tokio::test]
    async fn a_refusing_protocol_turns_connections_away() {
        let host = dispatcher().await;
        let claimed = host.register(ALPN).expect("alpn should be free");
        claimed.refusals().store(true, Ordering::Relaxed);
        let addr = dialable(host.endpoint()).await;

        let client = endpoint().await;
        assert!(
            client.connect(addr, ALPN).await.is_err(),
            "a refusing protocol should not accept a connection"
        );
    }

    #[tokio::test]
    async fn an_alpn_can_only_be_claimed_once() {
        let host = dispatcher().await;
        let _claimed = host.register(ALPN).expect("alpn should be free");
        assert!(
            host.register(ALPN).is_none(),
            "a second claim on the same alpn should be refused"
        );
    }

    #[tokio::test]
    async fn dropping_a_registration_frees_the_alpn() {
        let host = dispatcher().await;
        drop(host.register(ALPN).expect("alpn should be free"));
        assert!(
            host.register(ALPN).is_some(),
            "a released alpn should be claimable again"
        );
    }
}
