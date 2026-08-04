//! Readouts: connection statistics, and iroh's own metric counters.
//!
//! Everything here is a snapshot taken on the main thread. Nothing is polled or
//! cached, so a game asking every frame pays for it every frame — read these
//! when something wants showing, not in the hot path.
//!
//! Metrics are enumerated rather than listed out field by field: iroh describes
//! each counter through `MetricsGroup`, so whatever a version of iroh collects
//! is what appears, with no per-field code here to fall out of date.

use godot::prelude::*;
use iroh::endpoint::Connection;
use iroh_metrics::{MetricValue, MetricsGroupSet};

/// Turns a whole metrics set into `{ group: { name: value } }`, with each
/// counter's description alongside it under `<name>__help`.
///
/// Nothing here names an individual counter, so this reports whatever the
/// linked version of iroh happens to collect.
pub(crate) fn metrics(metrics: &impl MetricsGroupSet) -> VarDictionary {
    let mut out = VarDictionary::new();

    for (group, item) in metrics.iter() {
        let name = item.name();
        let value = match item.value() {
            MetricValue::Counter(count) => (count as i64).to_variant(),
            MetricValue::Gauge(level) => level.to_variant(),
            // A histogram is summarised rather than dropped: the buckets are
            // rarely what a game wants, but the total and the count are.
            MetricValue::Histogram { sum, count, .. } => {
                let mut summary = VarDictionary::new();
                summary.set("sum", &sum.to_variant());
                summary.set("count", &(count as i64).to_variant());
                summary.to_variant()
            }
            // `MetricValue` is non-exhaustive upstream, so an unfamiliar kind
            // is reported as absent rather than silently skipped.
            _ => Variant::nil(),
        };

        let mut counters: VarDictionary = out
            .get(group)
            .and_then(|existing| existing.try_to().ok())
            .unwrap_or_default();

        counters.set(name, &value);
        counters.set(format!("{name}__help"), &item.help().to_variant());
        out.set(group, &counters.to_variant());
    }

    out
}

/// A connection's traffic counters, as a dictionary.
///
/// Round-trip time comes from the selected path rather than the connection,
/// since a multipath connection has one per path and only the chosen one is
/// carrying traffic.
pub(crate) fn connection(connection: &Connection) -> VarDictionary {
    let stats = connection.stats();
    let mut out = VarDictionary::new();

    out.set(
        "sent_datagrams",
        &(stats.udp_tx.datagrams as i64).to_variant(),
    );
    out.set("sent_bytes", &(stats.udp_tx.bytes as i64).to_variant());
    out.set(
        "received_datagrams",
        &(stats.udp_rx.datagrams as i64).to_variant(),
    );
    out.set("received_bytes", &(stats.udp_rx.bytes as i64).to_variant());
    out.set("lost_packets", &(stats.lost_packets as i64).to_variant());
    out.set("lost_bytes", &(stats.lost_bytes as i64).to_variant());

    let path = crate::raw::path_info(connection);
    out.set(
        "rtt_ms",
        &path
            .as_ref()
            .map(|path| path.rtt.as_secs_f64() * 1000.0)
            .unwrap_or(-1.0)
            .to_variant(),
    );
    out.set(
        "relayed",
        &path.map(|path| path.relay).unwrap_or(false).to_variant(),
    );

    out
}
