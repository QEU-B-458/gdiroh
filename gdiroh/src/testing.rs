//! Endpoint helpers shared by the transport tests.

use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, TransportAddr};

use crate::dispatch::Dispatcher;

/// Binds a test endpoint with relays disabled, so traffic stays local. ALPNs are
/// left unset — the dispatcher publishes them as protocols register.
pub(crate) async fn endpoint() -> Endpoint {
    Endpoint::builder(presets::N0DisableRelay)
        .bind()
        .await
        .expect("endpoint should bind")
}

/// Waits until the endpoint reports an address that can actually be dialled.
pub(crate) async fn dialable(endpoint: &Endpoint) -> EndpointAddr {
    for _ in 0..200 {
        let addr = endpoint.addr();
        if addr.addrs.iter().any(|a| matches!(a, TransportAddr::Ip(_))) {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("endpoint never reported a direct address");
}

/// Two dispatchers whose endpoints already know how to reach each other.
///
/// Anything that dials by bare [`iroh::EndpointId`] — gossip bootstrapping, for
/// one — needs the address resolvable, and these endpoints have no relays and no
/// DNS. Seeding a [`MemoryLookup`] on each side is the out-of-band equivalent of
/// handing over a ticket.
pub(crate) async fn endpoint_pair() -> (Dispatcher, Dispatcher) {
    let here_lookup = MemoryLookup::new();
    let there_lookup = MemoryLookup::new();

    let here = Endpoint::builder(presets::N0DisableRelay)
        .address_lookup(here_lookup.clone())
        .bind()
        .await
        .expect("endpoint should bind");
    let there = Endpoint::builder(presets::N0DisableRelay)
        .address_lookup(there_lookup.clone())
        .bind()
        .await
        .expect("endpoint should bind");

    here_lookup.add_endpoint_info(dialable(&there).await);
    there_lookup.add_endpoint_info(dialable(&here).await);

    (Dispatcher::start(here), Dispatcher::start(there))
}

/// Polls `ready` for up to ten seconds, failing the test if it never holds.
pub(crate) async fn wait_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition never became true");
}
